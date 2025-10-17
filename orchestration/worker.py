import asyncio
import yaml

from temporalio.client import Client
from temporalio.worker import Worker
from activities import summarize_text_activity

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)


async def main():
    client = await Client.connect(config['temporal_server'])
    worker = Worker(
        client,
        task_queue="pykm-task-queue",
        workflows=[],
        activities=[
            summarize_text_activity,
        ],
    )
    print(f"Beginning worker for task queue {worker.task_queue.title()}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
