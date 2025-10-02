import asyncio
import httpx
import aiofiles
import time
from tqdm import tqdm

# --- Configuration ---
KAFKA_REST_PROXY_URL = "https://<REST_PROXY_URL>/kafka/v3"
CLUSTER_ID = "<LKC_ID>"
TARGET_TOPIC = "test-topic1"
OUTPUT_LOG = "produce_records_python.log"
REQUESTS_PER_SECOND = 700
TOTAL_DURATION_SECONDS = 600
TOTAL_RECORDS = REQUESTS_PER_SECOND * TOTAL_DURATION_SECONDS

# This is the key tuning parameter: how many requests can be "in flight" at once.
CONCURRENT_REQUESTS = 700

URL = f"{KAFKA_REST_PROXY_URL}/clusters/{CLUSTER_ID}/topics/{TARGET_TOPIC}/records"
HEADERS = {
    "Authorization": "Basic <base64 username:password>",
    "Content-Type": "application/json",
    "Connection": "close"
}

async def produce_record(client, session_log, record_id, semaphore):
    """Sends a single record, respecting the concurrency semaphore."""
    async with semaphore: # This will wait if too many requests are already active
        data_payload = {
            "value": {"type": "JSON", "data": {"record_id": record_id, "message": f"This is test record #{record_id} from python script."}}
        }
        try:
            response = await client.post(URL, json=data_payload, headers=HEADERS)
            await session_log.write(f"Record {record_id}: {response.status_code}\n")
            return response.status_code
        except Exception as e:
            await session_log.write(f"Record {record_id}: FAILED ({e})\n")
            return None

async def main():
    """Main function to run the load test."""
    print(f"Starting production of {TOTAL_RECORDS} records over {TOTAL_DURATION_SECONDS} seconds (~{REQUESTS_PER_SECOND} req/s)...")

    # httpx.AsyncClient manages an efficient connection pool
    limits = httpx.Limits(max_connections=CONCURRENT_REQUESTS, max_keepalive_connections=100)
    # Using a semaphore to precisely control concurrency
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)

    async with httpx.AsyncClient(timeout=10.0, limits=limits) as client, \
            aiofiles.open(OUTPUT_LOG, mode='w') as log_file:

        tasks = []
        start_time = time.time()

        # Create all tasks upfront
        for i in range(TOTAL_RECORDS):
            task = asyncio.create_task(produce_record(client, log_file, i, semaphore))
            tasks.append(task)

        # Await their completion with a progress bar
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing Records"):
            await f

    total_time = time.time() - start_time
    print(f"\nFinished producing {TOTAL_RECORDS} records.")
    print(f"Total time: {total_time:.2f} seconds.")
    print(f"Actual rate: {TOTAL_RECORDS / total_time:.2f} req/s.")
    print(f"Check {OUTPUT_LOG} for details.")

if __name__ == "__main__":
    # Ensure you have the required libraries: pip install httpx aiofiles tqdm
    asyncio.run(main())
