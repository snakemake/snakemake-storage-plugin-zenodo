from dataclasses import dataclass, field
import hashlib
from pathlib import Path
import re
from typing import Any, List, Optional
from urllib.parse import urlparse
from requests import HTTPError
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_provider import (
    StorageProviderBase,
    StorageQueryValidationResult,
    ExampleQuery,
    Operation,
    QueryType,
)
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectRead,
    StorageObjectWrite,
    retry_decorator,
)
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface, Mtime

from snakemake_storage_plugin_zenodo.helper import ZENHelper


number_re = re.compile(r"/\d+/")


# Optional:
# Define settings for your storage plugin (e.g. host url, credentials).
# They will occur in the Snakemake CLI as --storage-<storage-plugin-name>-<param-name>
# Make sure that all defined fields are 'Optional' and specify a default value
# of None or anything else that makes sense in your case.
# Note that we allow storage plugin settings to be tagged by the user. That means,
# that each of them can be specified multiple times (an implicit nargs=+), and
# the user can add a tag in front of each value (e.g. tagname1:value1 tagname2:value2).
# This way, a storage plugin can be used multiple times within a workflow with different
# settings.
@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    access_token: Optional[str] = field(
        default=None,
        metadata={
            "help": "Zenodo personal access token. Separate registration and access "
            "token is needed for Zenodo sandbox environment at "
            "https://sandbox.zenodo.org.",
            "env_var": True,
            # Optionally specify that setting is required when the executor is in use.
            "required": True,
        },
    )
    restricted_access_token: Optional[str] = field(
        default=None,
        metadata={
            "help": "Zenodo restricted access token. This is needed for access to "
            "restricted records.",
            "env_var": True,
            # Optionally specify that setting is required when the executor is in use.
            "required": False,
        },
    )
    sandbox: bool = field(
        default=False,
        metadata={
            "help": "Whether to use sandbox.zenodo.org instead of the production "
            "instance.",
            "env_var": False,
            # Optionally specify that setting is required when the executor is in use.
            "required": False,
        },
    )


# Required:
# Implementation of your storage provider
# This class can be empty as the one below.
# You can however use it to store global information or maintain e.g. a connection
# pool.
class StorageProvider(StorageProviderBase):
    def __post_init__(self):
        self.endpoint = (
            "https://sandbox.zenodo.org"
            if self.settings.sandbox
            else "https://zenodo.org"
        )

    @classmethod
    def example_queries(cls) -> List[ExampleQuery]:
        """Return an example query with description for this storage provider."""
        return [
            ExampleQuery(
                query="zenodo://record/123456/path/to/file_or_dir",
                type=QueryType.INPUT,
                description="A published zenodo record, starting with the ID, followed "
                "by the path to a file or directory in the record.",
            ),
            ExampleQuery(
                query="zenodo://deposition/123456/path/to/file_or_dir",
                type=QueryType.ANY,
                description="A unpublished (still writable) zenodo record, starting "
                "with the ID, followed by the path to a file or directory in the "
                "record.",
            ),
        ]

    def rate_limiter_key(self, query: str, operation: Operation) -> Any:
        """Return a key for identifying a rate limiter given a query and an operation.

        This is used to identify a rate limiter for the query.
        E.g. for a storage provider like http that would be the host name.
        For s3 it might be just the endpoint URL.
        """
        return self.endpoint

    def default_max_requests_per_second(self) -> float:
        """Return the default maximum number of requests per second for this storage
        provider."""
        return 5

    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        return True

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Return whether the given query is valid for this storage provider."""
        # Ensure that also queries containing wildcards (e.g. {sample}) are accepted
        # and considered valid. The wildcards will be resolved before the storage
        # object is actually used.
        parsed = urlparse(query)
        if parsed.scheme != "zenodo":
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason="Invalid scheme. Expected 'zenodo'.",
            )
        if parsed.netloc not in ("record", "deposition"):
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason="Invalid item type. Expected 'record' or 'deposition'.",
            )
        if not parsed.path:
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason="No file path given.",
            )
        if not number_re.match(parsed.path):
            return StorageQueryValidationResult(
                query=query,
                valid=False,
                reason="Invalid record ID. Expected a number, to occur directly after "
                "record/ or deposition/.",
            )
        return StorageQueryValidationResult(valid=True, query=query)


# Required:
# Implementation of storage object. If certain methods cannot be supported by your
# storage (e.g. because it is read-only see
# snakemake-storage-http for comparison), remove the corresponding base classes
# from the list of inherited items.
class StorageObject(StorageObjectRead, StorageObjectWrite):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.
        self.parsed = urlparse(self.query)
        self.is_record = self.parsed.netloc == "record"
        self.bucket_id = self.parsed.path.lstrip("/").split("/")[0]
        self.path = str(
            Path(self.parsed.path.lstrip("/")).relative_to(self.bucket_id).as_posix()
        )
        self.helper = ZENHelper(
            self.provider.settings, is_record=self.is_record, deposition=self.bucket_id
        )

    async def inventory(self, cache: IOCacheStorageInterface):
        """From this file, try to find as much existence and modification date
        information as possible. Only retrieve that information that comes for free
        given the current object.
        """
        # This is optional and can be left as is

        # If this is implemented in a storage object, results have to be stored in
        # the given IOCache object, using self.cache_key() as key.
        # Optionally, this can take a custom local suffix, needed e.g. when you want
        # to cache more items than the current query: self.cache_key(local_suffix=...)

        if self.get_inventory_parent() in cache.exists_in_storage:
            # record has been inventorized before
            return

        try:
            files = self.helper.get_files_record()
        except HTTPError as e:
            if e.response.status_code == 404:
                # record does not exist
                cache.exists_in_storage[self.get_inventory_parent()] = False
                return
            else:
                raise e
        cache.exists_in_storage[self.get_inventory_parent()] = True
        for f, meta in files.items():
            key = self.cache_key(self._local_suffix_from_file(f))
            cache.mtime[key] = Mtime(storage=0)
            cache.size[key] = meta.filesize
            cache.exists_in_storage[key] = True

    def _local_suffix_from_file(self, f):
        return f"{self.parsed.netloc}/{self.bucket_id}/{f}"

    def get_inventory_parent(self) -> Optional[str]:
        """Return the parent directory of this object."""
        # this is optional and can be left as is
        return self.cache_key(f"{self.parsed.netloc}/{self.bucket_id}")

    def local_suffix(self) -> str:
        """Return a unique suffix for the local path, determined from self.query."""
        return self.parsed.netloc + self.parsed.path

    def cleanup(self):
        """Perform local cleanup of any remainders of the storage object."""
        # self.local_path() should not be removed, as this is taken care of by
        # Snakemake.
        pass

    # Fallible methods should implement some retry logic.
    # The easiest way to do this (but not the only one) is to use the retry_decorator
    # provided by snakemake-interface-storage-plugins.
    @retry_decorator
    def exists(self) -> bool:
        # return True if the object exists
        try:
            return self.path in self.helper.get_files()
        except HTTPError as e:
            if e.response.status_code == 404:
                return False
            else:
                raise e

    @retry_decorator
    def mtime(self) -> float:
        # return the modification time
        # no mtime available from Zenodo
        return 0

    @retry_decorator
    def size(self) -> int:
        # return the size in bytes
        return self._stats().filesize

    @retry_decorator
    def retrieve_object(self):
        # Ensure that the object is accessible locally under self.local_path()
        stats = self._stats()
        download_url = stats.download
        r = self.helper._api_request(download_url)

        local_md5 = hashlib.md5()

        # Download file.
        with open(self.local_path(), "wb") as rf:
            for chunk in r.iter_content(chunk_size=1024 * 1024 * 10):
                local_md5.update(chunk)
                rf.write(chunk)
        local_md5 = local_md5.hexdigest()

        if local_md5 != stats.checksum:
            raise ValueError(
                f"File checksums do not match for remote file: {stats.filename}"
            )

    # The following to methods are only required if the class inherits from
    # StorageObjectReadWrite.

    @retry_decorator
    def store_object(self):
        # Ensure that the object is stored at the location specified by
        # self.local_path().
        with open(self.local_path(), "rb") as lf:
            self.helper._api_request(
                self.helper.bucket + f"/{self.path}",
                method="PUT",
                data=lf,
            )

    @retry_decorator
    def remove(self):
        # Remove the object from the storage.
        raise NotImplementedError(
            "Zenodo does not support removing files from a record."
        )

    # helper

    def _stats(self):
        return self.helper.get_files()[self.path]
