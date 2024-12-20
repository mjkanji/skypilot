import asyncio
import os
from dataclasses import dataclass

from temporalio import activity


async def run_subprocess_with_streams(command) -> tuple[int, str, str]:
    """Runs a command using asyncio's subprocess module, with streaming prints for STDOUT and STDERR.

    Returns a tuple with the returncode, and the accumulated STDOUT and STDERR from the process."""
    proc = await asyncio.create_subprocess_shell(
        command,
        stdin=asyncio.subprocess.DEVNULL,  # Close stdin to avoid deadlocks
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    print("Streaming stdout and stderr in real-time...\n")

    # Buffers to retain output
    stdout_lines = []
    stderr_lines = []

    # Stream stdout and save output
    async def stream_output(stream, stream_name, buffer):
        while True:
            line = await stream.readline()
            if not line:  # EOF
                break
            decoded_line = line.decode().strip()
            print(f"{stream_name}: {decoded_line}")
            buffer.append(decoded_line)

    # Run the streams concurrently
    await asyncio.gather(
        stream_output(proc.stdout, "STDOUT", stdout_lines),
        stream_output(proc.stderr, "STDERR", stderr_lines),
    )

    # Wait for the process to finish
    returncode = await proc.wait()
    stdout = "\n".join(stdout_lines)
    stderr = "\n".join(stderr_lines)

    if returncode == 0:
        activity.logger.info(f"{command} output: {stdout}")
    else:
        activity.logger.error(f"{command} failed with error: {stderr}")
        raise Exception(f"{command} failed.\nStdout: {stdout}\nStderr:{stderr}")

    return returncode, stdout, stderr


async def run_git_clone(repo_url: str, clone_path: str) -> None:
    activity.logger.info(f"Cloning git repository: {repo_url} to {clone_path}")

    # Create clone path if it doesn't exist
    os.makedirs(clone_path, exist_ok=True)

    # Check if the repository already exists
    if os.path.exists(os.path.join(clone_path, ".git")):
        # If it exists, pull the latest changes
        command = f"git -C {clone_path} pull"
    else:
        # If it doesn't exist, clone the repository
        command = f"git clone {repo_url} {clone_path}"

    returncode, stdout, stderr = await run_subprocess_with_streams(command)


@dataclass
class SkyTaskCommand:
    cluster_name: str
    entrypoint: str
    flags: str
    repo_url: str
    clone_path: str


@activity.defn
async def run_sky_task(input: SkyTaskCommand) -> str:
    # Clone the tasks repo to the worker
    await run_git_clone(input.repo_url, input.clone_path)

    # Provision the cluster and run the task
    # Always run with --down flag to ensure that any failures on the worker do not leave us with
    # "orphaned" resources.
    activity.logger.info(
        f"Running Sky Launch on cluster: {input.cluster_name} "
        f"with entrypoint: {input.entrypoint} and flags: {input.flags}"
    )
    command = (
        f"sky launch -y -c {input.cluster_name} --down {input.flags} {input.entrypoint}"
    )
    _ = await run_subprocess_with_streams(command)

    # Run the sky down command using subprocess
    activity.logger.info(f"Running Sky Down on cluster: {input.cluster_name}")
    command = f"sky down -y {input.cluster_name}"
    _ = await run_subprocess_with_streams(command)

    return "Success!"
