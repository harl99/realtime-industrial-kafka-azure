from datetime import datetime, timezone
from pathlib import Path

from azure.storage.blob import BlobServiceClient

from settings import AZURE_STORAGE_CONNECTION_STRING, AZURE_BLOB_CONTAINER


LOCAL_FILE = Path("data/processed/sensor_events_processed.jsonl")


def main():
    if not AZURE_STORAGE_CONNECTION_STRING:
        raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING in .env")

    if not LOCAL_FILE.exists():
        raise FileNotFoundError(f"File not found: {LOCAL_FILE}")

    blob_service_client = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )

    container_client = blob_service_client.get_container_client(
        AZURE_BLOB_CONTAINER
    )

    try:
        container_client.create_container()
        print(f"Created container: {AZURE_BLOB_CONTAINER}")
    except Exception:
        print(f"Container already exists or could not be created: {AZURE_BLOB_CONTAINER}")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    blob_name = f"processed/sensor_events_{timestamp}.jsonl"

    blob_client = container_client.get_blob_client(blob_name)

    with LOCAL_FILE.open("rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    print("Upload successful")
    print(f"Local file: {LOCAL_FILE}")
    print(f"Azure container: {AZURE_BLOB_CONTAINER}")
    print(f"Blob path: {blob_name}")


if __name__ == "__main__":
    main()
