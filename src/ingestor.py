import dlt
import os
import json
import logging
# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("loop_project")

# Path to mock data
MOCK_DATA_FILE_PATH = os.path.join(os.path.dirname(__file__), "data", "shopify_mock_data.json")
MOCK_ORDERS_FILE_PATH = os.path.join(os.path.dirname(__file__), "data", "order_data.json")
MOCK_CUSTOMERS_FILE_PATH = os.path.join(os.path.dirname(__file__), "data", "customer_data.json")
MOCK_PRODUCTS_FILE_PATH = os.path.join(os.path.dirname(__file__), "data", "product_data.json")


# Define a simple resource to load mock Shopify data
@dlt.source
def mock_shopify_data():
    @dlt.resource
    def raw_shopify_mock_data():
        """Reads data from the mock Shopify JSON file."""
        try:
            with open(MOCK_DATA_FILE_PATH, 'r') as f:
                data = json.load(f)  # Load the JSON data (list)
                logger.info("Successfully loaded mock data.")
                # Yield each order one by one
                yield from data
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Error loading mock data: {e}")
            yield []

    @dlt.resource
    def raw_orders():
        """Reads orders data from the mock Shopify JSON file."""
        try:
            with open(MOCK_ORDERS_FILE_PATH, 'r') as f:
                data = json.load(f)  # Load the orders data (as a list)
                logger.info("Successfully loaded orders data.")

                for order in data:
                    # Flatten nested items into a separate structure
                    for item in order.pop('items', []):
                        item['order_id'] = order['order_id']  # Link item to order
                        yield {'order': order, 'item': item}  # Yield both order and items separately
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Error loading orders data: {e}")
            yield []

    @dlt.resource
    def raw_products():
        """Reads products data from JSON file."""
        try:
            with open(MOCK_PRODUCTS_FILE_PATH, 'r') as f:
                data = json.load(f)
                logger.info("Successfully loaded products data.")
                yield from data  # Yield each product
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Error loading products data: {e}")
            yield []

    @dlt.resource
    def raw_customers():
        """Reads customers data from JSON file."""
        try:
            with open(MOCK_CUSTOMERS_FILE_PATH, 'r') as f:
                data = json.load(f)
                logger.info("Successfully loaded customers data.")
                yield from data  # Yield each customer
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Error loading customers data: {e}")
            yield []

    return raw_shopify_mock_data(), raw_orders(), raw_products(), raw_customers()

# Define the pipeline to load data into BigQuery
def run_pipeline():
    pipeline_name = "shopify_mock_pipeline"

    # Create dlt pipeline for BigQuery
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="bigquery",  # Destination is BigQuery
        dataset_name="shopify_sales"  # Dataset name in BigQuery
    )

    # Run the pipeline using the mock data source
    load_info = pipeline.run(mock_shopify_data())
    logger.info(f"Pipeline completed. Load info: {load_info}")

# Run the pipeline
if __name__ == "__main__":
    run_pipeline()
