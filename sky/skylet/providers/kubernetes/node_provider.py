import copy
import logging
import os
import re
import time
from typing import Dict
from uuid import uuid4

from ray.autoscaler._private.command_runner import SSHCommandRunner
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import NODE_KIND_HEAD
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME
from ray.autoscaler.tags import TAG_RAY_NODE_KIND

from sky import exceptions
from sky.adaptors import kubernetes
from sky.backends import backend_utils
from sky.provision.kubernetes import utils as kubernetes_utils
from sky.skylet import constants
from sky.skylet.providers.kubernetes import config
from sky.utils import cluster_yaml_utils
from sky.utils import common_utils

logger = logging.getLogger(__name__)

MAX_TAG_RETRIES = 3
DELAY_BEFORE_TAG_RETRY = 0.5
UPTIME_SSH_TIMEOUT = 10

RAY_COMPONENT_LABEL = 'cluster.ray.io/component'


# Monkey patch SSHCommandRunner to allow specifying SSH port
def set_port(self, port):
    self.ssh_options.arg_dict['Port'] = port


SSHCommandRunner.set_port = set_port

# Monkey patch SSHCommandRunner to use a larger timeout when running uptime to
# check cluster liveness. This is needed because the default timeout of 5s is
# too short when the cluster is accessed from different geographical
# locations over VPN.
#
# Ray autoscaler sets the timeout on a per-call basis (as an arg to
# SSHCommandRunner.run). The 5s timeout is hardcoded in
# NodeUpdater.wait_ready() in updater.py is hard to modify without
# duplicating a large chunk of ray autoscaler code. Instead, we
# monkey patch the run method to check if the command being run is 'uptime',
# and if so change the timeout to 10s.
#
# Fortunately, Ray uses a timeout of 120s for running commands after the
# cluster is ready, so we do not need to modify that.


def run_override_timeout(*args, **kwargs):
    # If command is `uptime`, change timeout to 10s
    command = args[1]
    if command == 'uptime':
        kwargs['timeout'] = UPTIME_SSH_TIMEOUT
    return SSHCommandRunner._run(*args, **kwargs)


SSHCommandRunner._run = SSHCommandRunner.run
SSHCommandRunner.run = run_override_timeout


def head_service_selector(cluster_name: str) -> Dict[str, str]:
    """Selector for Operator-configured head service."""
    return {RAY_COMPONENT_LABEL: f'{cluster_name}-head'}


def to_label_selector(tags):
    label_selector = ''
    for k, v in tags.items():
        if label_selector != '':
            label_selector += ','
        label_selector += '{}={}'.format(k, v)
    return label_selector


def run_command_on_pods(node_name, node_namespace, command):
    cmd_output = kubernetes.stream()(
        kubernetes.core_api().connect_get_namespaced_pod_exec,
        node_name,
        node_namespace,
        command=command,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _request_timeout=kubernetes.API_TIMEOUT)
    return cmd_output


class KubernetesNodeProvider(NodeProvider):

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.cluster_name = cluster_name

        # Kubernetes namespace to user
        self.namespace = provider_config.get(
            'namespace',
            kubernetes_utils.get_current_kube_config_context_namespace())

        # Timeout for resource provisioning. If it takes longer than this
        # timeout, the resource provisioning will be considered failed.
        # This is useful for failover. May need to be adjusted for different
        # kubernetes setups.
        self.timeout = provider_config['timeout']

    def non_terminated_nodes(self, tag_filters):
        # Match pods that are in the 'Pending' or 'Running' phase.
        # Unfortunately there is no OR operator in field selectors, so we
        # have to match on NOT any of the other phases.
        field_selector = ','.join([
            'status.phase!=Failed',
            'status.phase!=Unknown',
            'status.phase!=Succeeded',
            'status.phase!=Terminating',
        ])

        tag_filters[TAG_RAY_CLUSTER_NAME] = self.cluster_name
        label_selector = to_label_selector(tag_filters)
        pod_list = kubernetes.core_api().list_namespaced_pod(
            self.namespace,
            field_selector=field_selector,
            label_selector=label_selector)

        # Don't return pods marked for deletion,
        # i.e. pods with non-null metadata.DeletionTimestamp.
        return [
            pod.metadata.name
            for pod in pod_list.items
            if pod.metadata.deletion_timestamp is None
        ]

    def is_running(self, node_id):
        pod = kubernetes.core_api().read_namespaced_pod(node_id, self.namespace)
        return pod.status.phase == 'Running'

    def is_terminated(self, node_id):
        pod = kubernetes.core_api().read_namespaced_pod(node_id, self.namespace)
        return pod.status.phase not in ['Running', 'Pending']

    def node_tags(self, node_id):
        pod = kubernetes.core_api().read_namespaced_pod(node_id, self.namespace)
        return pod.metadata.labels

    def external_ip(self, node_id):
        return kubernetes_utils.get_external_ip()

    def external_port(self, node_id):
        # Extract the NodePort of the head node's SSH service
        # Node id is str e.g., example-cluster-head-v89lb

        # TODO(romilb): Implement caching here for performance.
        # TODO(romilb): Multi-node would need more handling here.
        cluster_name = node_id.split('-head')[0]
        return kubernetes_utils.get_head_ssh_port(cluster_name, self.namespace)

    def internal_ip(self, node_id):
        pod = kubernetes.core_api().read_namespaced_pod(node_id, self.namespace)
        return pod.status.pod_ip

    def get_node_id(self, ip_address, use_internal_ip=True) -> str:

        def find_node_id():
            if use_internal_ip:
                return self._internal_ip_cache.get(ip_address)
            else:
                return self._external_ip_cache.get(ip_address)

        if not find_node_id():
            all_nodes = self.non_terminated_nodes({})
            ip_func = self.internal_ip if use_internal_ip else self.external_ip
            ip_cache = (self._internal_ip_cache
                        if use_internal_ip else self._external_ip_cache)
            for node_id in all_nodes:
                ip_cache[ip_func(node_id)] = node_id

        if not find_node_id():
            if use_internal_ip:
                known_msg = f'Worker internal IPs: {list(self._internal_ip_cache)}'
            else:
                known_msg = f'Worker external IP: {list(self._external_ip_cache)}'
            raise ValueError(f'ip {ip_address} not found. ' + known_msg)

        return find_node_id()

    def set_node_tags(self, node_ids, tags):
        for _ in range(MAX_TAG_RETRIES - 1):
            try:
                self._set_node_tags(node_ids, tags)
                return
            except kubernetes.api_exception() as e:
                if e.status == 409:
                    logger.info(config.log_prefix +
                                'Caught a 409 error while setting'
                                ' node tags. Retrying...')
                    time.sleep(DELAY_BEFORE_TAG_RETRY)
                    continue
                else:
                    raise
        # One more try
        self._set_node_tags(node_ids, tags)

    def _recover_cluster_yaml_path(self, cluster_name_with_hash: str) -> str:
        # 'cluster_name_with_hash' combines the cluster name and hash value,
        # separated by a hyphen. By using 'slice_length', we remove the hash
        # (and its preceding hyphen) to retrieve the original cluster name.
        slice_length = -(common_utils.USER_HASH_LENGTH_IN_CLUSTER_NAME + 1)
        cluster_name = cluster_name_with_hash[:slice_length]
        cluster_yaml_path = (os.path.join(
            os.path.expanduser(backend_utils.SKY_USER_FILE_PATH),
            f'{cluster_name}.yml'))
        # Check if cluster_yaml_path exists. If not, we are running on
        # the master node in a multi-node setup, in which case we must use the
        # default ~/.sky/sky_ray.yml path.
        if not os.path.exists(cluster_yaml_path):
            cluster_yaml_path = os.path.expanduser(
                cluster_yaml_utils.SKY_CLUSTER_YAML_REMOTE_PATH)
        return cluster_yaml_path

    def _set_node_tags(self, node_id, tags):
        pod = kubernetes.core_api().read_namespaced_pod(node_id, self.namespace)
        pod.metadata.labels.update(tags)
        kubernetes.core_api().patch_namespaced_pod(node_id, self.namespace, pod)

    def _raise_pod_scheduling_errors(self, new_nodes):
        """Raise pod scheduling failure reason.

        When a pod fails to schedule in Kubernetes, the reasons for the failure
        are recorded as events. This function retrieves those events and raises
        descriptive errors for better debugging and user feedback.
        """
        for new_node in new_nodes:
            pod = kubernetes.core_api().read_namespaced_pod(
                new_node.metadata.name, self.namespace)
            pod_status = pod.status.phase
            # When there are multiple pods involved while launching instance,
            # there may be a single pod causing issue while others are
            # successfully scheduled. In this case, we make sure to not surface
            # the error message from the pod that is already scheduled.
            if pod_status != 'Pending':
                continue
            pod_name = pod._metadata._name
            events = kubernetes.core_api().list_namespaced_event(
                self.namespace,
                field_selector=(f'involvedObject.name={pod_name},'
                                'involvedObject.kind=Pod'))
            # Events created in the past hours are kept by
            # Kubernetes python client and we want to surface
            # the latest event message
            events_desc_by_time = sorted(
                events.items,
                key=lambda e: e.metadata.creation_timestamp,
                reverse=True)

            event_message = None
            for event in events_desc_by_time:
                if event.reason == 'FailedScheduling':
                    event_message = event.message
                    break
            timeout_err_msg = ('Timed out while waiting for nodes to start. '
                               'Cluster may be out of resources or '
                               'may be too slow to autoscale.')
            lack_resource_msg = (
                'Insufficient {resource} capacity on the cluster. '
                'Other SkyPilot tasks or pods may be using resources. '
                'Check resource usage by running `kubectl describe nodes`.')
            if event_message is not None:
                if pod_status == 'Pending':
                    if 'Insufficient cpu' in event_message:
                        raise config.KubernetesError(
                            lack_resource_msg.format(resource='CPU'))
                    if 'Insufficient memory' in event_message:
                        raise config.KubernetesError(
                            lack_resource_msg.format(resource='memory'))
                    gpu_lf_keys = [
                        lf.get_label_key()
                        for lf in kubernetes_utils.LABEL_FORMATTER_REGISTRY
                    ]
                    if pod.spec.node_selector:
                        for label_key in pod.spec.node_selector.keys():
                            if label_key in gpu_lf_keys:
                                # TODO(romilb): We may have additional node
                                #  affinity selectors in the future - in that
                                #  case we will need to update this logic.
                                if ('Insufficient nvidia.com/gpu'
                                        in event_message or
                                        'didn\'t match Pod\'s node affinity/selector'
                                        in event_message):
                                    raise config.KubernetesError(
                                        f'{lack_resource_msg.format(resource="GPU")} '
                                        f'Verify if {pod.spec.node_selector[label_key]}'
                                        ' is available in the cluster.')
                raise config.KubernetesError(f'{timeout_err_msg} '
                                             f'Pod status: {pod_status}'
                                             f'Details: \'{event_message}\' ')
        raise config.KubernetesError(f'{timeout_err_msg}')

    def _wait_for_pods_to_schedule(self, new_nodes_with_jump_pod):
        """Wait for all pods to be scheduled.

        Wait for all pods including jump pod to be scheduled, and if it
        exceeds the timeout, raise an exception. If pod's container
        is ContainerCreating, then we can assume that resources have been
        allocated and we can exit.
        """
        start_time = time.time()
        while time.time() - start_time < self.timeout:
            all_pods_scheduled = True
            for node in new_nodes_with_jump_pod:
                # Iterate over each pod to check their status
                pod = kubernetes.core_api().read_namespaced_pod(
                    node.metadata.name, self.namespace)
                if pod.status.phase == 'Pending':
                    # If container_statuses is None, then the pod hasn't
                    # been scheduled yet.
                    if pod.status.container_statuses is None:
                        all_pods_scheduled = False
                        break

            if all_pods_scheduled:
                return
            time.sleep(1)

        # Handle pod scheduling errors
        try:
            self._raise_pod_scheduling_errors(new_nodes_with_jump_pod)
        except config.KubernetesError:
            raise
        except Exception as e:
            raise config.KubernetesError(
                'An error occurred while trying to fetch the reason '
                'for pod scheduling failure. '
                f'Error: {common_utils.format_exception(e)}') from None

    def _wait_for_pods_to_run(self, new_nodes_with_jump_pod):
        """Wait for pods and their containers to be ready.

        Pods may be pulling images or may be in the process of container
        creation.
        """
        while True:
            all_pods_running = True
            # Iterate over each pod to check their status
            for node in new_nodes_with_jump_pod:
                pod = kubernetes.core_api().read_namespaced_pod(
                    node.metadata.name, self.namespace)

                # Continue if pod and all the containers within the
                # pod are succesfully created and running.
                if pod.status.phase == 'Running' and all([
                        container.state.running
                        for container in pod.status.container_statuses
                ]):
                    continue

                all_pods_running = False
                if pod.status.phase == 'Pending':
                    # Iterate over each container in pod to check their status
                    for container_status in pod.status.container_statuses:
                        # If the container wasn't in 'ContainerCreating'
                        # state, then we know pod wasn't scheduled or
                        # had some other error, such as image pull error.
                        # See list of possible reasons for waiting here:
                        # https://stackoverflow.com/a/57886025
                        waiting = container_status.state.waiting
                        if waiting is not None and waiting.reason != 'ContainerCreating':
                            raise config.KubernetesError(
                                'Failed to create container while launching '
                                'the node. Error details: '
                                f'{container_status.state.waiting.message}.')
                # Reaching this point means that one of the pods had an issue,
                # so break out of the loop
                break

            if all_pods_running:
                break
            time.sleep(1)

    def _check_user_privilege(self, new_nodes):
        # Checks if the default user has sufficient privilege to set up
        # the kubernetes instance pod.
        check_k8s_user_sudo_cmd = [
            '/bin/sh',
            '-c',
            (
                'if [ $(id -u) -eq 0 ]; then'
                # If user is root, create an alias for sudo used in skypilot setup
                '  echo \'alias sudo=""\' >> ~/.bashrc; '
                'else '
                '  if command -v sudo >/dev/null 2>&1; then '
                '    timeout 2 sudo -l >/dev/null 2>&1 || '
                f'    ( echo {exceptions.INSUFFICIENT_PRIVILEGES_CODE!r}; ); '
                '  else '
                f'    ( echo {exceptions.INSUFFICIENT_PRIVILEGES_CODE!r}; ); '
                '  fi; '
                'fi')
        ]

        for new_node in new_nodes:
            privilege_check = run_command_on_pods(new_node.metadata.name,
                                                  self.namespace,
                                                  check_k8s_user_sudo_cmd)
            if privilege_check == str(exceptions.INSUFFICIENT_PRIVILEGES_CODE):
                raise config.KubernetesError(
                    'Insufficient system privileges detected. '
                    'Ensure the default user has root access or '
                    '"sudo" is installed and the user is added to the sudoers '
                    'from the image.')

    def _setup_ssh_in_pods(self, new_nodes):
        # Setting up ssh for the pod instance. This is already setup for
        # the jump pod so it does not need to be run for it.
        set_k8s_ssh_cmd = [
            '/bin/sh', '-c',
            ('prefix_cmd() { if [ $(id -u) -ne 0 ]; then echo "sudo"; else echo ""; fi; }; '
             'export DEBIAN_FRONTEND=noninteractive;'
             '$(prefix_cmd) apt-get update;'
             '$(prefix_cmd) apt install openssh-server rsync -y; '
             '$(prefix_cmd) mkdir -p /var/run/sshd; '
             '$(prefix_cmd) sed -i "s/PermitRootLogin prohibit-password/PermitRootLogin yes/" /etc/ssh/sshd_config; '
             '$(prefix_cmd) sed "s@session\\s*required\\s*pam_loginuid.so@session optional pam_loginuid.so@g" -i /etc/pam.d/sshd; '
             'cd /etc/ssh/ && $(prefix_cmd) ssh-keygen -A; '
             '$(prefix_cmd) mkdir -p ~/.ssh; '
             '$(prefix_cmd) cat /etc/secret-volume/ssh-publickey* > ~/.ssh/authorized_keys; '
             '$(prefix_cmd) service ssh restart')
        ]

        # TODO(romilb): We need logging and surface errors here.
        for new_node in new_nodes:
            run_command_on_pods(new_node.metadata.name, self.namespace,
                                set_k8s_ssh_cmd)

    def _set_env_vars_in_pods(self, new_nodes):
        """Setting environment variables in pods.

        Once all containers are ready, we can exec into them and set env vars.
        Kubernetes automatically populates containers with critical
        environment variables, such as those for discovering services running
        in the cluster and CUDA/nvidia environment variables. We need to
        make sure these env vars are available in every task and ssh session.
        This is needed for GPU support and service discovery.
        See https://github.com/skypilot-org/skypilot/issues/2287 for
        more details.

        To do so, we capture env vars from the pod's runtime and write them to
        /etc/profile.d/, making them available for all users in future
        shell sessions.
        """
        set_k8s_env_var_cmd = [
            '/bin/sh', '-c',
            ('prefix_cmd() { if [ $(id -u) -ne 0 ]; then echo "sudo"; else echo ""; fi; } && '
             'printenv | awk -F "=" \'{print "export " $1 "=\\047" $2 "\\047"}\' > ~/k8s_env_var.sh && '
             'mv ~/k8s_env_var.sh /etc/profile.d/k8s_env_var.sh || '
             '$(prefix_cmd) mv ~/k8s_env_var.sh /etc/profile.d/k8s_env_var.sh')
        ]

        for new_node in new_nodes:
            run_command_on_pods(new_node.metadata.name, self.namespace,
                                set_k8s_env_var_cmd)

    def _update_ssh_user_config(self, new_nodes, cluster_name_with_hash):
        get_k8s_ssh_user_cmd = ['/bin/sh', '-c', ('echo $(whoami)')]
        for new_node in new_nodes:
            ssh_user = run_command_on_pods(new_node.metadata.name,
                                           self.namespace, get_k8s_ssh_user_cmd)

        cluster_yaml_path = self._recover_cluster_yaml_path(
            cluster_name_with_hash)
        with open(cluster_yaml_path, 'r') as f:
            content = f.read()

        # Replacing the default ssh user name with the actual user name.
        # This updates user name specified in user's custom image if it's used.
        content = re.sub(r'ssh_user: \w+', f'ssh_user: {ssh_user}', content)

        with open(cluster_yaml_path, 'w') as f:
            f.write(content)

    def create_node(self, node_config, tags, count):
        conf = copy.deepcopy(node_config)
        pod_spec = conf.get('pod', conf)
        service_spec = conf.get('service')
        node_uuid = str(uuid4())
        tags[TAG_RAY_CLUSTER_NAME] = self.cluster_name
        tags['ray-node-uuid'] = node_uuid
        pod_spec['metadata']['namespace'] = self.namespace
        if 'labels' in pod_spec['metadata']:
            pod_spec['metadata']['labels'].update(tags)
        else:
            pod_spec['metadata']['labels'] = tags

        # Allow Operator-configured service to access the head node.
        if tags[TAG_RAY_NODE_KIND] == NODE_KIND_HEAD:
            head_selector = head_service_selector(self.cluster_name)
            pod_spec['metadata']['labels'].update(head_selector)

        logger.info(config.log_prefix +
                    'calling create_namespaced_pod (count={}).'.format(count))
        new_nodes = []
        for _ in range(count):
            pod = kubernetes.core_api().create_namespaced_pod(
                self.namespace, pod_spec)
            new_nodes.append(pod)

        new_svcs = []
        if service_spec is not None:
            logger.info(config.log_prefix + 'calling create_namespaced_service '
                        '(count={}).'.format(count))

            for new_node in new_nodes:
                metadata = service_spec.get('metadata', {})
                metadata['name'] = new_node.metadata.name
                service_spec['metadata'] = metadata
                service_spec['spec']['selector'] = {'ray-node-uuid': node_uuid}
                svc = kubernetes.core_api().create_namespaced_service(
                    self.namespace, service_spec)
                new_svcs.append(svc)

        # Adding the jump pod to the new_nodes list as well so it can be
        # checked if it's scheduled and running along with other pod instances.
        ssh_jump_pod_name = conf['metadata']['labels']['skypilot-ssh-jump']
        new_nodes_with_jump_pod = new_nodes[:]
        jump_pod = kubernetes.core_api().read_namespaced_pod(
            ssh_jump_pod_name, self.namespace)
        new_nodes_with_jump_pod.append(jump_pod)
        node_names = [node.metadata.name for node in new_nodes_with_jump_pod]

        # Wait until the pods are scheduled and surface cause for error
        # if there is one
        logger.info(config.log_prefix +
                    f'Waiting for pods to schedule. Pods: {node_names}')
        self._wait_for_pods_to_schedule(new_nodes_with_jump_pod)
        # Wait until the pods and their containers are up and running, and
        # fail early if there is an error
        logger.info(config.log_prefix +
                    f'Waiting for pods to run. Pods: {node_names}')
        self._wait_for_pods_to_run(new_nodes_with_jump_pod)
        logger.info(config.log_prefix +
                    f'Checking if user in image has sufficient privileges.')
        self._check_user_privilege(new_nodes)
        logger.info(config.log_prefix + f'Setting up SSH in pod.')
        self._setup_ssh_in_pods(new_nodes)
        logger.info(config.log_prefix +
                    f'Setting up environment variables in pod.')
        self._set_env_vars_in_pods(new_nodes)
        cluster_name_with_hash = conf['metadata']['labels']['skypilot-cluster']
        logger.info(config.log_prefix + f'Fetching and updating ssh username.')
        self._update_ssh_user_config(new_nodes, cluster_name_with_hash)

    def terminate_node(self, node_id):
        logger.info(config.log_prefix + 'calling delete_namespaced_pod')
        try:
            kubernetes_utils.clean_zombie_ssh_jump_pod(self.namespace, node_id)
        except Exception as e:
            logger.warning(config.log_prefix +
                           f'Error occurred when analyzing SSH Jump pod: {e}')
        try:
            kubernetes.core_api().delete_namespaced_service(
                node_id,
                self.namespace,
                _request_timeout=config.DELETION_TIMEOUT)
            kubernetes.core_api().delete_namespaced_service(
                f'{node_id}-ssh',
                self.namespace,
                _request_timeout=config.DELETION_TIMEOUT)
        except kubernetes.api_exception():
            pass
        # Note - delete pod after all other resources are deleted.
        # This is to ensure there are no leftover resources if this down is run
        # from within the pod, e.g., for autodown.
        try:
            kubernetes.core_api().delete_namespaced_pod(
                node_id,
                self.namespace,
                _request_timeout=config.DELETION_TIMEOUT)
        except kubernetes.api_exception() as e:
            if e.status == 404:
                logger.warning(config.log_prefix +
                               f'Tried to delete pod {node_id},'
                               ' but the pod was not found (404).')
            else:
                raise

    def terminate_nodes(self, node_ids):
        # TODO(romilb): terminate_nodes should be include optimizations for
        #  deletion of multiple nodes. Currently, it deletes one node at a time.
        #  We should look in to using deletecollection here for batch deletion.
        for node_id in node_ids:
            self.terminate_node(node_id)

    def get_command_runner(self,
                           log_prefix,
                           node_id,
                           auth_config,
                           cluster_name_with_hash,
                           process_runner,
                           use_internal_ip,
                           docker_config=None):
        """Returns the CommandRunner class used to perform SSH commands.

        Args:
        log_prefix(str): stores "NodeUpdater: {}: ".format(<node_id>). Used
            to print progress in the CommandRunner.
        node_id(str): the node ID.
        auth_config(dict): the authentication configs from the autoscaler
            yaml file.
        cluster_name_with_hash(str): the name of the cluster and hash value,
            separated by a hyphen.
        process_runner(module): the module to use to run the commands
            in the CommandRunner. E.g., subprocess.
        use_internal_ip(bool): whether the node_id belongs to an internal ip
            or external ip.
        docker_config(dict): If set, the docker information of the docker
            container that commands should be run on.
        """
        # For custom images, the username might differ across images.
        # The 'ssh_user' is updated inplace in the YAML at the end of the
        # 'create_node()' process in _update_ssh_user_config.
        # Since the node provider is initialized with stale auth information,
        # we need to reload the updated user from YAML.
        cluster_yaml_path = self._recover_cluster_yaml_path(
            cluster_name_with_hash)
        ssh_credentials = backend_utils.ssh_credential_from_yaml(
            cluster_yaml_path)
        auth_config['ssh_user'] = ssh_credentials['ssh_user']

        common_args = {
            'log_prefix': log_prefix,
            'node_id': node_id,
            'provider': self,
            'auth_config': auth_config,
            'cluster_name': cluster_name_with_hash,
            'process_runner': process_runner,
            'use_internal_ip': use_internal_ip,
        }
        command_runner = SSHCommandRunner(**common_args)
        if use_internal_ip:
            port = 22
        else:
            port = self.external_port(node_id)
        command_runner.set_port(port)
        return command_runner

    @staticmethod
    def bootstrap_config(cluster_config):
        return config.bootstrap_kubernetes(cluster_config)

    @staticmethod
    def fillout_available_node_types_resources(cluster_config):
        """Fills out missing "resources" field for available_node_types."""
        return config.fillout_resources_kubernetes(cluster_config)
