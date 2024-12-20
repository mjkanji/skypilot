from dataclasses import dataclass
from datetime import timedelta
from textwrap import dedent

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities import SkyTaskCommand, run_sky_task


@dataclass
class SkyPilotWorkflowInput:
    cloud: str
    cluster_prefix: str
    repo_url: str
    data_bucket_url: str | None = None


@workflow.defn
class SkyPilotWorkflow:
    @workflow.run
    async def run(self, input: SkyPilotWorkflowInput) -> str:
        retry_policy = RetryPolicy(
            maximum_attempts=3,
            maximum_interval=timedelta(seconds=2),
        )

        clone_path = "/tmp/skypilot_repo"
        data_bucket_flag = (
            "--env DATA_BUCKET_URL=" + input.data_bucket_url
            if input.data_bucket_url
            else ""
        )

        workflow.logger.info(
            f"Running SkyPilot workflow with cluster prefix: {input.cluster_prefix} "
        )

        # 1. Launch data preprocessing
        cluster_name = f"{input.cluster_prefix}-preprocess"
        preprocess_result = await workflow.execute_activity(
            run_sky_task,
            SkyTaskCommand(
                cluster_name=cluster_name,
                entrypoint=f"{clone_path}/data_preprocessing.yaml",
                flags=f"--cloud {input.cloud} {data_bucket_flag}",
                repo_url=input.repo_url,
                clone_path=clone_path,
            ),
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=retry_policy,
        )
        workflow.logger.info(f"Preprocessing result: {preprocess_result}")

        # 2. Launch training
        cluster_name = f"{input.cluster_prefix}-train"
        train_result = await workflow.execute_activity(
            run_sky_task,
            SkyTaskCommand(
                cluster_name=cluster_name,
                entrypoint=f"{clone_path}/train.yaml",
                flags=f"--cloud {input.cloud} {data_bucket_flag}",
                repo_url=input.repo_url,
                clone_path=clone_path,
            ),
            start_to_close_timeout=timedelta(minutes=60),
            retry_policy=retry_policy,
        )
        workflow.logger.info(f"Training result: {train_result}")

        # 3. Launch evaluation
        cluster_name = f"{input.cluster_prefix}-eval"
        eval_result = await workflow.execute_activity(
            run_sky_task,
            SkyTaskCommand(
                cluster_name=cluster_name,
                entrypoint=f"{clone_path}/eval.yaml",
                flags=f"--cloud {input.cloud} {data_bucket_flag}",
                repo_url=input.repo_url,
                clone_path=clone_path,
            ),
            start_to_close_timeout=timedelta(minutes=60),
            retry_policy=retry_policy,
        )
        workflow.logger.info(f"Evaluation result: {train_result}")

        # Return the combined result
        return dedent(
            f"""Preprocessing: {preprocess_result}
            Training: {train_result}
            Evaluation: {eval_result}"""
        )
