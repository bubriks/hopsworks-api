#   Copyright 2020 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
from __future__ import annotations

import json
import warnings
from typing import Any, Dict, List, Optional, Set, TypeVar, Union

import humps
import pandas as pd
from hopsworks_common import client
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.core.constants import HAS_NUMPY
from hsfs import engine, training_dataset_feature, util
from hsfs.constructor import filter, query
from hsfs.core import (
    statistics_engine,
    training_dataset_api,
    training_dataset_engine,
    vector_server,
)
from hsfs.statistics_config import StatisticsConfig
from hsfs.storage_connector import HopsFSConnector, StorageConnector
from hsfs.training_dataset_split import TrainingDatasetSplit


if HAS_NUMPY:
    import numpy as np


class TrainingDatasetBase:
    NOT_FOUND_ERROR_CODE = 270012
    # NOTE: This class is exposed to users with the only purpose of providing information about a Training Dataset
    # and, therefore, it should not implement any functionality and remain with as minimal as possible

    HOPSFS = "HOPSFS_TRAINING_DATASET"
    EXTERNAL = "EXTERNAL_TRAINING_DATASET"
    IN_MEMORY = "IN_MEMORY_TRAINING_DATASET"
    ENTITY_TYPE = "trainingdatasets"

    def __init__(
        self,
        name,
        version,
        data_format,
        location="",
        event_start_time=None,
        event_end_time=None,
        coalesce=False,
        description=None,
        storage_connector=None,
        splits=None,
        validation_size=None,
        test_size=None,
        train_start=None,
        train_end=None,
        validation_start=None,
        validation_end=None,
        test_start=None,
        test_end=None,
        seed=None,
        created=None,
        creator=None,
        features=None,
        statistics_config=None,
        training_dataset_type=None,
        label=None,
        train_split=None,
        time_split_size=None,
        extra_filter=None,
        **kwargs,
    ):
        self._name = name
        self._version = version
        self._description = description
        self._data_format = data_format
        self._validation_size = validation_size
        self._test_size = test_size
        self._train_start = train_start
        self._train_end = train_end
        self._validation_start = validation_start
        self._validation_end = validation_end
        self._test_start = test_start
        self._test_end = test_end
        self._coalesce = coalesce
        self._seed = seed
        self._location = location
        self._train_split = train_split

        if training_dataset_type:
            self.training_dataset_type = training_dataset_type
        else:
            self._training_dataset_type = None
        # set up depending on user initialized or coming from backend response
        if created is None:
            self._start_time = util.convert_event_time_to_timestamp(event_start_time)
            self._end_time = util.convert_event_time_to_timestamp(event_end_time)
            # no type -> user init
            self._features = features
            self.storage_connector = storage_connector
            self.splits = splits
            self.statistics_config = statistics_config
            self._label = label
            if validation_size or test_size:
                self._train_split = TrainingDatasetSplit.TRAIN
                self.splits = {
                    TrainingDatasetSplit.TRAIN: 1
                    - (validation_size or 0)
                    - (test_size or 0),
                    TrainingDatasetSplit.VALIDATION: validation_size,
                    TrainingDatasetSplit.TEST: test_size,
                }
            self._set_time_splits(
                time_split_size,
                train_start,
                train_end,
                validation_start,
                validation_end,
                test_start,
                test_end,
            )
            self._extra_filter = (
                filter.Logic(filter.Logic.SINGLE, left_f=extra_filter)
                if isinstance(extra_filter, filter.Filter)
                else extra_filter
            )
        else:
            self._start_time = event_start_time
            self._end_time = event_end_time
            # type available -> init from backend response
            # make rest call to get all connector information, description etc.
            self._storage_connector = StorageConnector.from_response_json(
                storage_connector
            )

            if features is None:
                features = []
            self._features = [
                training_dataset_feature.TrainingDatasetFeature.from_response_json(feat)
                for feat in features
            ]
            self._splits = [
                TrainingDatasetSplit.from_response_json(split) for split in splits
            ]
            self._statistics_config = StatisticsConfig.from_response_json(
                statistics_config
            )
            self._label = [
                util.autofix_feature_name(feat.name)
                for feat in self._features
                if feat.label
            ]
            self._extra_filter = filter.Logic.from_response_json(extra_filter)

    def _set_time_splits(
        self,
        time_split_size,
        train_start=None,
        train_end=None,
        validation_start=None,
        validation_end=None,
        test_start=None,
        test_end=None,
    ):
        train_start = util.convert_event_time_to_timestamp(train_start)
        train_end = util.convert_event_time_to_timestamp(train_end)
        validation_start = util.convert_event_time_to_timestamp(validation_start)
        validation_end = util.convert_event_time_to_timestamp(validation_end)
        test_start = util.convert_event_time_to_timestamp(test_start)
        test_end = util.convert_event_time_to_timestamp(test_end)

        time_splits = list()
        self._append_time_split(
            time_splits,
            split_name=TrainingDatasetSplit.TRAIN,
            start_time=train_start,
            end_time=train_end or validation_start or test_start,
        )
        if time_split_size == 3:
            self._append_time_split(
                time_splits,
                split_name=TrainingDatasetSplit.VALIDATION,
                start_time=validation_start or train_end,
                end_time=validation_end or test_start,
            )
        self._append_time_split(
            time_splits,
            split_name=TrainingDatasetSplit.TEST,
            start_time=test_start or validation_end or train_end,
            end_time=test_end,
        )
        if time_splits:
            self._train_split = TrainingDatasetSplit.TRAIN
            # prioritise time split
            self._splits = time_splits

    def _append_time_split(
        self,
        time_splits,
        split_name,
        start_time=None,
        end_time=None,
    ):
        if start_time or end_time:
            time_splits.append(
                TrainingDatasetSplit(
                    name=split_name,
                    split_type=TrainingDatasetSplit.TIME_SERIES_SPLIT,
                    start_time=start_time,
                    end_time=end_time,
                )
            )

    def _infer_training_dataset_type(self, connector_type):
        if connector_type == StorageConnector.HOPSFS or connector_type is None:
            return self.HOPSFS
        elif (
            connector_type == StorageConnector.S3
            or connector_type == StorageConnector.ADLS
            or connector_type == StorageConnector.GCS
        ):
            return self.EXTERNAL
        else:
            raise TypeError(
                "Storage connectors of type {} are currently not supported for training datasets.".format(
                    connector_type
                )
            )

    def to_dict(self):
        return {
            "name": self._name,
            "version": self._version,
            "description": self._description,
            "dataFormat": self._data_format,
            "coalesce": self._coalesce,
            "storageConnector": self._storage_connector,
            "location": self._location,
            "trainingDatasetType": self._training_dataset_type,
            "splits": self._splits,
            "seed": self._seed,
            "statisticsConfig": self._statistics_config,
            "trainSplit": self._train_split,
            "eventStartTime": self._start_time,
            "eventEndTime": self._end_time,
            "extraFilter": self._extra_filter,
        }

    @property
    def name(self) -> str:
        """Name of the training dataset."""
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = name

    @property
    def version(self) -> int:
        """Version number of the training dataset."""
        return self._version

    @version.setter
    def version(self, version: int) -> None:
        self._version = version

    @property
    def description(self) -> Optional[str]:
        return self._description

    @description.setter
    def description(self, description: Optional[str]) -> None:
        """Description of the training dataset contents."""
        self._description = description

    @property
    def data_format(self):
        """File format of the training dataset."""
        return self._data_format

    @data_format.setter
    def data_format(self, data_format):
        self._data_format = data_format

    @property
    def coalesce(self) -> bool:
        """If true the training dataset data will be coalesced into
        a single partition before writing. The resulting training dataset
        will be a single file per split"""
        return self._coalesce

    @coalesce.setter
    def coalesce(self, coalesce: bool):
        self._coalesce = coalesce

    @property
    def storage_connector(self):
        """Storage connector."""
        return self._storage_connector

    @storage_connector.setter
    def storage_connector(self, storage_connector):
        if isinstance(storage_connector, StorageConnector):
            self._storage_connector = storage_connector
        elif storage_connector is None:
            # init empty connector, otherwise will have to handle it at serialization time
            self._storage_connector = HopsFSConnector(
                None, None, None, None, None, None
            )
        else:
            raise TypeError(
                "The argument `storage_connector` has to be `None` or of type `StorageConnector`, is of type: {}".format(
                    type(storage_connector)
                )
            )
        if self.training_dataset_type != self.IN_MEMORY:
            self._training_dataset_type = self._infer_training_dataset_type(
                self._storage_connector.type
            )

    @property
    def splits(self) -> List[TrainingDatasetSplit]:
        """Training dataset splits. `train`, `test` or `eval` and corresponding percentages."""
        return self._splits

    @splits.setter
    def splits(self, splits: Optional[Dict[str, float]]):
        # user api differs from how the backend expects the splits to be represented
        if splits is None:
            self._splits = []
        elif isinstance(splits, dict):
            self._splits = [
                TrainingDatasetSplit(
                    name=k, split_type=TrainingDatasetSplit.RANDOM_SPLIT, percentage=v
                )
                for k, v in splits.items()
                if v is not None
            ]
        else:
            raise TypeError(
                "The argument `splits` has to be `None` or a dictionary of key, relative size e.g "
                + "{'train': 0.7, 'test': 0.1, 'validation': 0.2}.\n"
                + "Got {} with type {}".format(splits, type(splits))
            )

    @property
    def location(self) -> str:
        """Path to the training dataset location. Can be an empty string if e.g. the training dataset is in-memory."""
        return self._location

    @location.setter
    def location(self, location: str):
        self._location = location

    @property
    def seed(self) -> Optional[int]:
        """Seed used to perform random split, ensure reproducibility of the random split at a later date."""
        return self._seed

    @seed.setter
    def seed(self, seed: Optional[int]):
        self._seed = seed

    @property
    def statistics_config(self):
        """Statistics configuration object defining the settings for statistics
        computation of the training dataset."""
        return self._statistics_config

    @statistics_config.setter
    def statistics_config(self, statistics_config):
        if isinstance(statistics_config, StatisticsConfig):
            self._statistics_config = statistics_config
        elif isinstance(statistics_config, dict):
            self._statistics_config = StatisticsConfig(**statistics_config)
        elif isinstance(statistics_config, bool):
            self._statistics_config = StatisticsConfig(statistics_config)
        elif statistics_config is None:
            self._statistics_config = StatisticsConfig()
        else:
            raise TypeError(
                "The argument `statistics_config` has to be `None` of type `StatisticsConfig, `bool` or `dict`, but is of type: `{}`".format(
                    type(statistics_config)
                )
            )

    @property
    def train_split(self):
        """Set name of training dataset split that is used for training."""
        return self._train_split

    @train_split.setter
    def train_split(self, train_split):
        self._train_split = train_split

    @property
    def event_start_time(self):
        return self._start_time

    @event_start_time.setter
    def event_start_time(self, start_time):
        self._start_time = start_time

    @property
    def event_end_time(self):
        return self._end_time

    @event_end_time.setter
    def event_end_time(self, end_time):
        self._end_time = end_time

    @property
    def training_dataset_type(self):
        return self._training_dataset_type

    @training_dataset_type.setter
    def training_dataset_type(self, training_dataset_type):
        valid_type = [self.IN_MEMORY, self.HOPSFS, self.EXTERNAL]
        if training_dataset_type not in valid_type:
            raise ValueError(
                "Training dataset type should be one of " ", ".join(valid_type)
            )
        else:
            self._training_dataset_type = training_dataset_type

    @property
    def validation_size(self):
        return self._validation_size

    @validation_size.setter
    def validation_size(self, validation_size):
        self._validation_size = validation_size

    @property
    def test_size(self):
        return self._test_size

    @test_size.setter
    def test_size(self, test_size):
        self._test_size = test_size

    @property
    def train_start(self):
        return self._train_start

    @train_start.setter
    def train_start(self, train_start):
        self._train_start = train_start

    @property
    def train_end(self):
        return self._train_end

    @train_end.setter
    def train_end(self, train_end):
        self._train_end = train_end

    @property
    def validation_start(self):
        return self._validation_start

    @validation_start.setter
    def validation_start(self, validation_start):
        self._validation_start = validation_start

    @property
    def validation_end(self):
        return self._validation_end

    @validation_end.setter
    def validation_end(self, validation_end):
        self._validation_end = validation_end

    @property
    def test_start(self):
        return self._test_start

    @test_start.setter
    def test_start(self, test_start):
        self._test_start = test_start

    @property
    def test_end(self):
        return self._test_end

    @test_end.setter
    def test_end(self, test_end):
        self._test_end = test_end

    @property
    def extra_filter(self):
        return self._extra_filter

    @extra_filter.setter
    def extra_filter(self, extra_filter):
        self._extra_filter = extra_filter


class TrainingDataset(TrainingDatasetBase):
    def __init__(
        self,
        name,
        version,
        data_format,
        featurestore_id,
        location="",
        event_start_time=None,
        event_end_time=None,
        coalesce=False,
        description=None,
        storage_connector=None,
        splits=None,
        validation_size=None,
        test_size=None,
        train_start=None,
        train_end=None,
        validation_start=None,
        validation_end=None,
        test_start=None,
        test_end=None,
        seed=None,
        created=None,
        creator=None,
        features=None,
        statistics_config=None,
        featurestore_name=None,
        id=None,
        inode_id=None,
        training_dataset_type=None,
        from_query=None,
        querydto=None,
        label=None,
        train_split=None,
        time_split_size=None,
        extra_filter=None,
        **kwargs,
    ):
        super().__init__(
            name,
            version,
            data_format,
            location=location,
            event_start_time=event_start_time,
            event_end_time=event_end_time,
            coalesce=coalesce,
            description=description,
            storage_connector=storage_connector,
            splits=splits,
            validation_size=validation_size,
            test_size=test_size,
            train_start=train_start,
            train_end=train_end,
            validation_start=validation_start,
            validation_end=validation_end,
            test_start=test_start,
            test_end=test_end,
            seed=seed,
            created=created,
            creator=creator,
            features=features,
            statistics_config=statistics_config,
            training_dataset_type=training_dataset_type,
            label=label,
            train_split=train_split,
            time_split_size=time_split_size,
            extra_filter=extra_filter,
        )

        self._id = id
        self._from_query = from_query
        self._querydto = querydto
        self._feature_store_id = featurestore_id
        self._feature_store_name = featurestore_name

        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(
            featurestore_id
        )
        self._training_dataset_engine = training_dataset_engine.TrainingDatasetEngine(
            featurestore_id
        )
        self._statistics_engine = statistics_engine.StatisticsEngine(
            featurestore_id, self.ENTITY_TYPE
        )
        self._vector_server = vector_server.VectorServer(
            featurestore_id, features=self._features
        )

    def save(
        self,
        features: Union[
            query.Query,
            pd.DataFrame,
            TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
            TypeVar("pyspark.RDD"),  # noqa: F821
            np.ndarray,
            List[list],
        ],
        write_options: Optional[Dict[Any, Any]] = None,
    ):
        """Materialize the training dataset to storage.

        This method materializes the training dataset either from a Feature Store
        `Query`, a Spark or Pandas `DataFrame`, a Spark RDD, two-dimensional Python
        lists or Numpy ndarrays.
        From v2.5 onward, filters are saved along with the `Query`.

        !!! warning "Engine Support"
            Creating Training Datasets from Dataframes is only supported using Spark as Engine.

        # Arguments
            features: Feature data to be materialized.
            write_options: Additional write options as key-value pairs, defaults to `{}`.
                When using the `python` engine, write_options can contain the
                following entries:
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../jobs/#jobconfiguration)
                  to configure the Hopsworks Job used to compute the training dataset.
                * key `wait_for_job` and value `True` or `False` to configure
                  whether or not to the save call should return only
                  after the Hopsworks Job has finished. By default it waits.

        # Returns
            `Job`: When using the `python` engine, it returns the Hopsworks Job
                that was launched to create the training dataset.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: Unable to create training dataset metadata.
        """
        user_version = self._version
        user_stats_config = self._statistics_config
        # td_job is used only if the python engine is used
        training_dataset, td_job = self._training_dataset_engine.save(
            self, features, write_options or {}
        )
        self.storage_connector = training_dataset.storage_connector
        # currently we do not save the training dataset statistics config for training datasets
        self.statistics_config = user_stats_config
        if self.statistics_config.enabled and engine.get_type().startswith("spark"):
            self.compute_statistics()
        if user_version is None:
            warnings.warn(
                "No version provided for creating training dataset `{}`, incremented version to `{}`.".format(
                    self._name, self._version
                ),
                util.VersionWarning,
                stacklevel=1,
            )

        return td_job

    def insert(
        self,
        features: Union[
            query.Query,
            pd.DataFrame,
            TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
            TypeVar("pyspark.RDD"),  # noqa: F821
            np.ndarray,
            List[list],
        ],
        overwrite: bool,
        write_options: Optional[Dict[Any, Any]] = None,
    ):
        """Insert additional feature data into the training dataset.

        !!! warning "Deprecated"
            `insert` method is deprecated.

        This method appends data to the training dataset either from a Feature Store
        `Query`, a Spark or Pandas `DataFrame`, a Spark RDD, two-dimensional Python
        lists or Numpy ndarrays. The schemas must match for this operation.

        This can also be used to overwrite all data in an existing training dataset.

        # Arguments
            features: Feature data to be materialized.
            overwrite: Whether to overwrite the entire data in the training dataset.
            write_options: Additional write options as key-value pairs, defaults to `{}`.
                When using the `python` engine, write_options can contain the
                following entries:
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../jobs/#jobconfiguration)
                  to configure the Hopsworks Job used to compute the training dataset.
                * key `wait_for_job` and value `True` or `False` to configure
                  whether or not to the insert call should return only
                  after the Hopsworks Job has finished. By default it waits.

        # Returns
            `Job`: When using the `python` engine, it returns the Hopsworks Job
                that was launched to create the training dataset.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: Unable to create training dataset metadata.
        """
        # td_job is used only if the python engine is used
        td_job = self._training_dataset_engine.insert(
            self, features, write_options or {}, overwrite
        )

        self.compute_statistics()

        return td_job

    def read(self, split=None, read_options=None):
        """Read the training dataset into a dataframe.

        It is also possible to read only a specific split.

        # Arguments
            split: Name of the split to read, defaults to `None`, reading the entire
                training dataset. If the training dataset has split, the `split` parameter
                is mandatory.
            read_options: Additional read options as key/value pairs, defaults to `{}`.
        # Returns
            `DataFrame`: The spark dataframe containing the feature data of the
                training dataset.
        """
        if self.splits and split is None:
            raise ValueError(
                "The training dataset has splits, please specify the split you want to read"
            )

        return self._training_dataset_engine.read(self, split, read_options or {})

    def compute_statistics(self):
        """Compute the statistics for the training dataset and save them to the
        feature store.
        """
        if self.statistics_config.enabled and engine.get_type().startswith("spark"):
            try:
                registered_stats = self._statistics_engine.get(
                    self,
                    before_transformation=False,
                )
            except RestAPIError as e:
                if (
                    e.response.json().get("errorCode", "")
                    == RestAPIError.FeatureStoreErrorCode.STATISTICS_NOT_FOUND
                    and e.response.status_code == 404
                ):
                    registered_stats = None
                raise e
            if registered_stats is not None:
                return registered_stats
            if self.splits:
                return self._statistics_engine.compute_and_save_split_statistics(self)
            else:
                return self._statistics_engine.compute_and_save_statistics(
                    self, self.read()
                )

    def show(self, n: int, split: str = None):
        """Show the first `n` rows of the training dataset.

        You can specify a split from which to retrieve the rows.

        # Arguments
            n: Number of rows to show.
            split: Name of the split to show, defaults to `None`, showing the first rows
                when taking all splits together.
        """
        self.read(split).show(n)

    def add_tag(self, name: str, value):
        """Attach a tag to a training dataset.

        A tag consists of a <name,value> pair. Tag names are unique identifiers across the whole cluster.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        # Arguments
            name: Name of the tag to be added.
            value: Value of the tag to be added.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: in case the backend fails to add the tag.
        """
        self._training_dataset_engine.add_tag(self, name, value)

    def delete_tag(self, name: str):
        """Delete a tag attached to a training dataset.

        # Arguments
            name: Name of the tag to be removed.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: in case the backend fails to delete the tag.
        """
        self._training_dataset_engine.delete_tag(self, name)

    def get_tag(self, name):
        """Get the tags of a training dataset.

        # Arguments
            name: Name of the tag to get.

        # Returns
            tag value

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: in case the backend fails to retrieve the tag.
        """
        return self._training_dataset_engine.get_tag(self, name)

    def get_tags(self):
        """Returns all tags attached to a training dataset.

        # Returns
            `Dict[str, obj]` of tags.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: in case the backend fails to retrieve the tags.
        """
        return self._training_dataset_engine.get_tags(self)

    def update_statistics_config(self):
        """Update the statistics configuration of the training dataset.

        Change the `statistics_config` object and persist the changes by calling
        this method.

        # Returns
            `TrainingDataset`. The updated metadata object of the training dataset.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: in case the backend encounters an issue
        """
        self._training_dataset_engine.update_statistics_config(self)
        return self

    def delete(self):
        """Delete training dataset and all associated metadata.

        !!! note "Drops only HopsFS data"
            Note that this operation drops only files which were materialized in
            HopsFS. If you used a Storage Connector for a cloud storage such as S3,
            the data will not be deleted, but you will not be able to track it anymore
            from the Feature Store.

        !!! danger "Potentially dangerous operation"
            This operation drops all metadata associated with **this version** of the
            training dataset **and** and the materialized data in HopsFS.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`.
        """
        warnings.warn(
            "All jobs associated to training dataset `{}`, version `{}` will be removed.".format(
                self._name, self._version
            ),
            util.JobWarning,
            stacklevel=1,
        )
        self._training_dataset_api.delete(self)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            tds = []
            for td in json_decamelized["items"]:
                td.pop("type")
                td.pop("href")
                cls._rewrite_location(td)
                tds.append(cls(**td))
            return tds
        else:  # backwards compatibility
            for td in json_decamelized:
                _ = td.pop("type")
                cls._rewrite_location(td)
            return [cls(**td) for td in json_decamelized]

    @classmethod
    def from_response_json_single(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        json_decamelized.pop("type", None)
        json_decamelized.pop("href", None)
        cls._rewrite_location(json_decamelized)
        return cls(**json_decamelized)

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        # here we lose the information that the user set, e.g. write_options
        self._rewrite_location(json_decamelized)
        self.__init__(**json_decamelized)
        return self

    # A bug is introduced https://github.com/logicalclocks/hopsworks/blob/7adcad3cf5303ef19c996d75e6f4042cf565c8d5/hopsworks-common/src/main/java/io/hops/hopsworks/common/featurestore/trainingdatasets/hopsfs/HopsfsTrainingDatasetController.java#L85
    # Rewrite the td location if it is TD root directory
    @classmethod
    def _rewrite_location(cls, td_json):
        _client = client.get_instance()
        if "location" in td_json:
            if td_json["location"].endswith(
                f"/Projects/{_client._project_name}/{_client._project_name}_Training_Datasets"
            ):
                td_json["location"] = (
                    f"{td_json['location']}/{td_json['name']}_{td_json['version']}"
                )

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        return {
            "name": self._name,
            "version": self._version,
            "description": self._description,
            "dataFormat": self._data_format,
            "coalesce": self._coalesce,
            "storageConnector": self._storage_connector,
            "location": self._location,
            "trainingDatasetType": self._training_dataset_type,
            "features": self._features,
            "splits": self._splits,
            "seed": self._seed,
            "queryDTO": self._querydto.to_dict() if self._querydto else None,
            "statisticsConfig": self._statistics_config,
            "trainSplit": self._train_split,
            "eventStartTime": self._start_time,
            "eventEndTime": self._end_time,
            "extraFilter": self._extra_filter,
            "type": "trainingDatasetDTO",
        }

    @property
    def id(self):
        """Training dataset id."""
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @property
    def write_options(self):
        """User provided options to write training dataset."""
        return self._write_options

    @write_options.setter
    def write_options(self, write_options):
        self._write_options = write_options

    @property
    def schema(self):
        """Training dataset schema."""
        return self._features

    @schema.setter
    def schema(self, features):
        """Training dataset schema."""
        self._features = features

    @property
    def statistics(self):
        """Get computed statistics for the training dataset.

        # Returns
            `Statistics`. Object with statistics information.
        """
        return self._statistics_engine.get(self, before_transformation=False)

    @property
    def query(self):
        """Query to generate this training dataset from online feature store."""
        return self._training_dataset_engine.query(self, True, True, False)

    def get_query(self, online: bool = True, with_label: bool = False):
        """Returns the query used to generate this training dataset

        # Arguments
            online: boolean, optional. Return the query for the online storage, else
                for offline storage, defaults to `True` - for online storage.
            with_label: Indicator whether the query should contain features which were
                marked as prediction label/feature when the training dataset was
                created, defaults to `False`.

        # Returns
            `str`. Query string for the chosen storage used to generate this training
                dataset.
        """
        return self._training_dataset_engine.query(
            self, online, with_label, engine.get_type() == "python"
        )

    def init_prepared_statement(
        self, batch: Optional[bool] = None, external: Optional[bool] = None
    ):
        """Initialise and cache parametrized prepared statement to
           retrieve feature vector from online feature store.

        # Arguments
            batch: boolean, optional. If set to True, prepared statements will be
                initialised for retrieving serving vectors as a batch.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hopsworks.login()`](login.md#login) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP. Defaults to True if connection to Hopsworks is established from
                external environment (e.g AWS Sagemaker or Google Colab), otherwise to False.
        """
        self._vector_server.init_serving(self, batch, external)

    def get_serving_vector(
        self, entry: Dict[str, Any], external: Optional[bool] = None
    ):
        """Returns assembled serving vector from online feature store.

        # Arguments
            entry: dictionary of training dataset feature group primary key names as keys and values provided by
                serving application.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hopsworks.login()`](login.md#login) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP. Defaults to True if connection to Hopsworks is established from
                external environment (e.g AWS Sagemaker or Google Colab), otherwise to False.
        # Returns
            `list` List of feature values related to provided primary keys, ordered according to positions of this
            features in training dataset query.
        """
        if self._vector_server.prepared_statements is None:
            self.init_prepared_statement(None, external)
        return self._vector_server.get_feature_vector(entry)

    def get_serving_vectors(
        self, entry: Dict[str, List[Any]], external: Optional[bool] = None
    ):
        """Returns assembled serving vectors in batches from online feature store.

        # Arguments
            entry: dict of feature group primary key names as keys and value as list of primary keys provided by
                serving application.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hopsworks.login()`](login.md#login) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP. Defaults to True if connection to Hopsworks is established from
                external environment (e.g AWS Sagemaker or Google Colab), otherwise to False.
        # Returns
            `List[list]` List of lists of feature values related to provided primary keys, ordered according to
            positions of this features in training dataset query.
        """
        if self._vector_server.prepared_statements is None:
            self.init_prepared_statement(None, external)
        return self._vector_server.get_feature_vectors(entry)

    @property
    def label(self) -> Union[str, List[str]]:
        """The label/prediction feature of the training dataset.

        Can be a composite of multiple features.
        """
        return self._label

    @label.setter
    def label(self, label: str) -> None:
        self._label = [util.autofix_feature_name(lb) for lb in label]

    @property
    def feature_store_id(self) -> int:
        return self._feature_store_id

    @property
    def feature_store_name(self) -> str:
        """Name of the feature store in which the feature group is located."""
        return self._feature_store_name

    @property
    def serving_keys(self) -> Set[str]:
        """Set of primary key names that is used as keys in input dict object for `get_serving_vector` method."""
        if self._serving_keys is None or len(self._serving_keys) == 0:
            self._serving_keys = util.build_serving_keys_from_prepared_statements(
                self._training_dataset_api.get_serving_prepared_statement(
                    entity=self, batch=False
                ),
                feature_store_id=self._feature_store_id,
                ignore_prefix=True,  # if serving_keys have to be built it is because fv created prior to 3.3, this ensure compatibility
            )
        return self._serving_keys
