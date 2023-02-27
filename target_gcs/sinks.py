"""GCS target sink class, which handles writing streams.
Opens an I/O stream pointed to a blob in the GCS bucket
As messages are received from the source, they are streamed to the bucket. As the max batch size is reached, the queue is emptied and written to the I/O streams

"""
import time
from collections import defaultdict
from datetime import datetime
from io import FileIO
from typing import Optional

import orjson
import smart_open
from google.cloud.storage import Client
from singer_sdk.sinks import BatchSink
from typing import Any, Dict, List, Optional


import sys
import csv

import singer

logger = singer.get_logger()

class GCSSink(BatchSink):
    """GCS target sink class."""

    max_size = sys.maxsize  # A singe batch for now
    records: List[Dict[str, Any]]

    def __init__(self, target, stream_name, schema, key_properties):
        super().__init__(
            target=target,
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
        )
        self._gcs_write_handle: Optional[FileIO] = None
        self._key_name: str = ""
        self.records = []

    @property
    def key_name(self) -> str:
        """Return the key name."""
        if not self._key_name:
            extraction_timestamp = round(time.time())
            base_key_name = self.config.get(
                "key_naming_convention",
                f"{self.stream_name}_{extraction_timestamp}.{self.output_format}",
            )
            prefixed_key_name = (
                f'{self.config.get("key_prefix", "")}/{base_key_name}'.replace(
                    "//", "/"
                )
            ).lstrip("/")
            date = datetime.today().strftime(self.config.get("date_format", "%Y-%m-%d"))
            self._key_name = prefixed_key_name.format_map(
                defaultdict(
                    str,
                    stream=self.stream_name,
                    date=date,
                    timestamp=extraction_timestamp,
                )
            )
        return self._key_name

    @property
    def gcs_write_handle(self) -> FileIO:
        """Opens a stream for writing to the target cloud object"""
        if not self._gcs_write_handle:
            credentials_path = self.config.get("credentials_file")
            self._gcs_write_handle = smart_open.open(
                f'gs://{self.config.get("bucket_name")}/{self.key_name}',
                "w",
                transport_params=dict(
                    client=Client.from_service_account_json(credentials_path)
                ),
            )
        return self._gcs_write_handle

    @property
    def output_format(self) -> str:
        """In the future maybe we will support more formats"""
        return self.config.get("output_format", "json")
    
    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Developers may optionally read or write additional markers within the
        passed `context` dict from the current batch.
        """
        if self.output_format == "json":
            self.gcs_write_handle.write(
                orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE)
            )
        else: #CSV
            self.records.append(record)
    
    def process_batch(self, context: dict) -> None:
        if self.output_format == "json":
            return

        if "properties" not in self.schema:
            raise ValueError("Stream's schema has no properties defined.")

        keys: List[str] = list(self.schema["properties"].keys())
        logger.info(f"Schema {self.stream_name}: {keys}")
        writer = csv.DictWriter(self.gcs_write_handle, fieldnames=keys, dialect="excel")
        writer.writeheader()
        records_processed = 0
        records_failed = 0
        for record in enumerate(self.records, start=1):
            logger.info(f"Processing record {self.stream_name}: {record}")
            try:
                writer.writerow(record)
                records_processed += 1
            except Exception as e:
                records_failed += 1
                logger.info(f"Failed processing record for {self.stream_name}: {e}")

        logger.info(f"Finished processing batch for {self.stream_name}. Records processed: {records_processed}. Records failed: {records_failed}.")

        self.records = []

