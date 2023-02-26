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

import pandas as pd
import json
import io


class GCSSink(BatchSink):
    """GCS target sink class."""

    max_size = 1000  # Max records to write in one batch

    def __init__(self, target, stream_name, schema, key_properties):
        super().__init__(
            target=target,
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
        )
        self._gcs_write_handle: Optional[FileIO] = None
        self._key_name: str = ""

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
                "wb",
                transport_params=dict(
                    client=Client.from_service_account_json(credentials_path)
                ),
            )
        return self._gcs_write_handle

    @property
    def output_format(self) -> str:
        """In the future maybe we will support more formats"""
        return self.config.get("output_format")

    def setup(self) -> None:
        if self.output_format == "csv":
            self.df = pd.DataFrame()
    
    def process_record(self, record: dict, context: dict) -> None:
        """Process the record.

        Developers may optionally read or write additional markers within the
        passed `context` dict from the current batch.
        """
        if self.output_format == "csv":
            self.df.append(pd.DataFrame(record), ignore_index=True)
        else: #json case
            self.gcs_write_handle.write(
                orjson.dumps(record, option=orjson.OPT_APPEND_NEWLINE)
            )
    
    def process_batch(self, context: dict) -> None:
        if self.output_format == "csv":
            output_buffer = io.BytesIO()
            self.df.to_csv(output_buffer, index=False)
            self.gcs_write_handle.write(output_buffer.getvalue())

