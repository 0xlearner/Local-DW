import asyncio
import logging
from typing import List, Optional
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the project root directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from src.config import Config
from src.pipeline.data_pipeline import Pipeline
from src.logger import setup_logger


class PipelineRunner:
    def __init__(self):
        self.config = Config()
        self.pipeline = Pipeline(self.config)
        self.logger = setup_logger("pipeline_runner")

    async def initialize(self):
        """Initialize the pipeline and its dependencies"""
        try:
            await self.pipeline.initialize()
            self.logger.info("Pipeline initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize pipeline: {str(e)}")
            raise

    async def process_files(
        self, file_patterns: List[dict], monitor: bool = True, polling_interval: int = 5
    ) -> dict:
        """
        Process multiple files through the pipeline

        Args:
            file_patterns: List of dicts containing file patterns and table info
                         [{"s3_path": "listings/listings.csv.gz", "table": "listings"}]
            monitor: Whether to monitor processing
            polling_interval: Interval for monitoring status

        Returns:
            Dictionary containing batch IDs and processing results
        """
        results = {}

        for file_info in file_patterns:
            try:
                self.logger.info(f"Processing file: {file_info['s3_path']}")

                batch_id = await self.pipeline.run(
                    file_prefix=file_info["s3_path"],
                    primary_key="id",
                    target_table=file_info["table"],
                )

                results[file_info["s3_path"]] = {
                    "batch_id": batch_id,
                    "status": "COMPLETED",
                }

                self.logger.info(
                    f"Successfully processed {file_info['s3_path']} with batch_id {batch_id}"
                )

            except Exception as e:
                self.logger.error(
                    f"Error processing file {file_info['s3_path']}: {str(e)}"
                )
                results[file_info["s3_path"]] = {"status": "FAILED", "error": str(e)}

        return results

    async def generate_report(
        self,
        batch_ids: Optional[List[str]] = None,
        table_name: Optional[str] = None,
        save_path: Optional[str] = None,
    ):
        """Generate and optionally save processing report"""
        try:
            if save_path:
                await self.pipeline.save_load_report(
                    report_path=save_path, batch_ids=batch_ids, table_name=table_name
                )
                self.logger.info(f"Report saved to {save_path}")
            else:
                report = await self.pipeline.generate_load_report(
                    batch_ids=batch_ids, table_name=table_name
                )
                return report
        except Exception as e:
            self.logger.error(f"Error generating report: {str(e)}")
            raise

    async def shutdown(self):
        """Gracefully shutdown the pipeline"""
        try:
            await self.pipeline.shutdown()
            self.logger.info("Pipeline shut down successfully")
        except Exception as e:
            self.logger.error(f"Error during pipeline shutdown: {str(e)}")
            raise


async def main():
    """Main entry point for the pipeline"""
    runner = PipelineRunner()

    # Define files to process
    files_to_process = [
        {"s3_path": "listings/listings.csv.gz", "table": "listings"},
        {"s3_path": "reviews/reviews.csv.gz", "table": "reviews"},
        {"s3_path": "calendar/calendar.csv.gz", "table": "calendar"},
    ]

    try:
        # Initialize pipeline
        await runner.initialize()

        # Process files
        results = await runner.process_files(
            file_patterns=files_to_process, monitor=True, polling_interval=5
        )

        # Generate and save report
        await runner.generate_report(save_path="reports/pipeline_execution_report.json")

        # Log results
        for file_path, result in results.items():
            if result.get("status") == "FAILED":
                logging.error(f"Failed processing {file_path}: {result.get('error')}")
            else:
                logging.info(
                    f"Successfully processed {file_path} - Batch ID: {result.get('batch_id')}"
                )

    except Exception as e:
        logging.error(f"Pipeline execution failed: {str(e)}")
        raise
    finally:
        await runner.shutdown()


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Run the pipeline
    asyncio.run(main())
