import dlt
import logging
import os
import time
from datetime import datetime
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# ── Logging setup ─────────────────────────────────────────────────────────────
log_dir = os.path.dirname(os.path.abspath(__file__))
log_path = os.path.join(log_dir, "pipeline.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)
# ──────────────────────────────────────────────────────────────────────────────


def nyc_taxi():
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/",
        },
        "resources": [
            {
                "name": "rides",
                "endpoint": {
                    "path": "data_engineering_zoomcamp_api",
                    "paginator": PageNumberPaginator(
                        base_page=1,
                        page_param="page",
                        stop_after_empty_page=True,
                        total_path=None,
                    ),
                },
            }
        ],
    }
    return rest_api_source(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="postgres",
    dataset_name="taxi_data",
)

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("Pipeline run started at %s", datetime.now().isoformat())
    logger.info("Pipeline name : %s", pipeline.pipeline_name)
    logger.info("Destination   : postgres")
    logger.info("Dataset       : taxi_data")
    logger.info("Log file      : %s", log_path)

    start = time.time()
    try:
        logger.info("Extracting data from NYC Taxi API …")
        load_info = pipeline.run(nyc_taxi())
        elapsed = time.time() - start

        logger.info("Pipeline finished in %.1f s", elapsed)
        logger.info("Load info:\n%s", load_info)

        # Row-count verification via the destination
        try:
            with pipeline.sql_client() as client:
                rows = client.execute_sql(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
                    "taxi_data",
                )
                logger.info("Tables in database:")
                for (tbl,) in rows:
                    count_rows = client.execute_sql(f'SELECT COUNT(*) FROM taxi_data."{tbl}"')
                    count = count_rows[0][0]
                    logger.info("  taxi_data.%s — %d rows", tbl, count)
        except Exception as verify_err:
            logger.warning("Could not verify row counts: %s", verify_err)

        logger.info("Data load SUCCESSFUL")

    except Exception as exc:
        elapsed = time.time() - start
        logger.error("Pipeline FAILED after %.1f s: %s", elapsed, exc, exc_info=True)
        raise

    print(load_info)
