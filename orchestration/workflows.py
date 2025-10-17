from temporalio import workflow
from temporalio.common import RetryPolicy
from orchestration.activities import download_dataset_activity, process_dataset_activity, load_ontology_activity, \
    process_assertions_activity, summarize_text_activity


@workflow.defn
class DatasetDownloadWorkflow:
    @workflow.run
    async def run(self, config: dict) -> dict:
        downloaded = await workflow.execute_activity(
            download_dataset_activity,
            config,
            start_to_close_timeout=3600,  # 1 hour timeout
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        return {"status": "downloaded", "details": downloaded}


@workflow.defn
class DatasetProcessingWorkflow:
    @workflow.run
    async def run(self, config: dict) -> dict:
        if config.get('download_first', True):
            await workflow.execute(DatasetDownloadWorkflow, config)

        processed = await workflow.execute_activity(
            process_dataset_activity,
            config,
            start_to_close_timeout=7200,  # 2 hours
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        summaries = []
        for record in processed.get('records', []):  # Assuming processed returns records
            summary = await workflow.execute_activity(
                summarize_text_activity,
                {"text": record, "rank": config.get('rank', False)},
                start_to_close_timeout=300,  # 5 min per summary
            )
            summaries.append(summary)

        return {"status": "processed", "summaries": summaries}


@workflow.defn
class OwlOntologyProcessingWorkflow:
    @workflow.run
    async def run(self, config: dict) -> dict:
        graph = await workflow.execute_activity(
            load_ontology_activity,
            config['file'],
            start_to_close_timeout=1800,
        )

        results = await workflow.execute_activity(
            process_assertions_activity,
            {"graph": graph, "assertions": config.get('assertions', [])},
            start_to_close_timeout=3600,
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        return {"status": "processed", "results": results}