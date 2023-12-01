import os
from typing import Optional, Type
from snakemake_interface_storage_plugins.tests import TestStorageBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase

from snakemake_storage_plugin_zenodo import StorageProvider, StorageProviderSettings


class TestStorageZenodoBase(TestStorageBase):
    __test__ = False

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        # Return the StorageProvider class of this plugin
        return StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        # instantiate StorageProviderSettings of this plugin as appropriate
        return StorageProviderSettings(
            access_token=os.environ("SNAKEMAKE_STORAGE_ZENODO_ACCESS_TOKEN"),
            sandbox=True,
        )


class TestStorageRead(TestStorageZenodoBase):
    __test__ = True
    retrieve_only = True  # set to True if the storage is read-only
    store_only = False  # set to True if the storage is write-only
    delete = False  # set to False if the storage does not support deletion

    def get_query(self, tmp_path) -> str:
        # Return a query. If retrieve_only is True, this should be a query that
        # is present in the storage, as it will not be created.
        return "zenodo://3269"

    def get_query_not_existing(self, tmp_path) -> str:
        return "zenodo://0"


class TestStorageWrite(TestStorageZenodoBase):
    __test__ = True
    retrieve_only = False  # set to True if the storage is read-only
    store_only = True  # set to True if the storage is write-only
    # TODO enable deletion in not yet published depositions
    delete = False  # set to False if the storage does not support deletion

    def get_query(self, tmp_path) -> str:
        # Return a query. If retrieve_only is True, this should be a query that
        # is present in the storage, as it will not be created.
        return "zenodo://5157"

    def get_query_not_existing(self, tmp_path) -> str:
        return "zenodo://0"