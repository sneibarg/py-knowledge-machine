import asyncio
import yaml

from temporalio.client import Client
from orchestration.workflows import DatasetDownloadWorkflow, DatasetProcessingWorkflow, OwlOntologyProcessingWorkflow


async def main():
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    client = await Client.connect(config['temporal_server'])
    if 'datasets' in config and config['datasets'].get('download', False):
        await client.execute(DatasetDownloadWorkflow, config['datasets'])

    if 'datasets' in config and config['datasets'].get('process', False):
        await client.execute(DatasetProcessingWorkflow, config['datasets'])

    if 'ontology' in config and config['ontology'].get('process', False):
        await client.execute(OwlOntologyProcessingWorkflow, config['ontology'])


if __name__ == "__main__":
    asyncio.run(main())
