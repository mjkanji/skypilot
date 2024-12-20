import asyncio

from activities import run_sky_task
from temporalio.client import Client
from temporalio.worker import Worker
from workflows import SkyPilotWorkflow


async def main() -> None:
    client: Client = await Client.connect("localhost:7233", namespace="default")

    # Start client
    client = await Client.connect("localhost:7233")

    worker: Worker = Worker(
        client,
        task_queue="skypilot-task-queue",
        workflows=[SkyPilotWorkflow],
        activities=[run_sky_task],  # Register all Sky activities to the same worker
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
