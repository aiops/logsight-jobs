from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import dateutil.parser
import pytest
from elastic_transport import ApiResponseMeta, HttpHeaders, NodeConfig

from logsight.connectors.connectors.elasticsearch import ElasticsearchConfigProperties
from logsight.connectors.connectors.sql_db import DatabaseConfigProperties
from logsight_jobs.common.index_job import IndexJob, IndexJobResult
from logsight_jobs.persistence.dto import IndexInterval
from logsight_jobs.persistence.timestamp_storage import PostgresTimestampStorage, TimestampStorageProvider
from logsight.services.elasticsearch_service.elasticsearch_service import ElasticsearchService
from logsight.services.service_provider import ServiceProvider
from tests.inputs import processed_logs
from elasticsearch import NotFoundError


@pytest.fixture
@patch.multiple(IndexJob, __abstractmethods__=set())
def index_job():
    return IndexJob(IndexInterval("index", datetime.min), index_ext="ext",
                    table_name="test")


@pytest.fixture
def es_config():
    yield ElasticsearchConfigProperties(scheme="scheme", host="host", port=9201, username="user", password="password")


@pytest.fixture
def db():
    cfg = DatabaseConfigProperties(host="host", port=9000, username="username", password="password", db_name="db_name")
    db = PostgresTimestampStorage("table", cfg)
    db.connect = MagicMock()
    db.update_timestamps = MagicMock()
    db.close = MagicMock()
    return db




def test__execute(index_job, db):
    TimestampStorageProvider.provide_timestamp_storage = MagicMock(return_value=db)
    index_job._perform_aggregation = MagicMock(side_effect=[True, False])

    result = index_job._execute()

    assert isinstance(result, IndexJobResult)
    assert result.table == index_job.table_name
    assert result.index_interval == index_job.index_interval


def test__perform_aggregation(index_job):
    index_job._load_data = MagicMock()
    index_job._load_data.side_effect = (processed_logs, [],)
    index_job._calculate = MagicMock()
    index_job._store_results = MagicMock()
    index_job._update_index_interval = MagicMock()

    result = index_job._perform_aggregation()

    assert result is True
    assert index_job._load_data.call_count == 1
    assert index_job._calculate.call_count == 1
    assert index_job._store_results.call_count == 1

    result2 = index_job._perform_aggregation()

    assert result2 is False
    assert index_job._load_data.call_count == 2
    assert index_job._calculate.call_count == 1
    assert index_job._store_results.call_count == 1


def test__update_index_interval(index_job):
    latest_ingest_time = index_job.index_interval.latest_ingest_time
    index_job._update_index_interval(latest_ingest_time)
    assert index_job.index_interval.latest_ingest_time == latest_ingest_time + timedelta(milliseconds=1)


def test__load_data_new_entries(index_job, es_config):
    es = ElasticsearchService(es_config)
    es._connect = MagicMock()
    es.get_all_logs_for_index = MagicMock(return_value=processed_logs[:5])
    es.get_all_logs_after_ingest = MagicMock(return_value=processed_logs[:5])
    ServiceProvider.provide_elasticsearch = MagicMock(return_value=es)
    result = index_job._load_data('index', index_job.index_interval.latest_ingest_time)
    assert result == processed_logs[:5]


def test__load_data_no_data(index_job, es_config):
    es = ElasticsearchService(es_config)
    es._connect = MagicMock()
    es.get_all_logs_after_ingest = MagicMock(
        side_effect=NotFoundError(message="index not found", body=None,
                                  meta=ApiResponseMeta(status=200, headers=HttpHeaders(), duration=0,
                                                       http_version="http2",
                                                       node=NodeConfig("host", "port", 2000))))
    ServiceProvider.provide_elasticsearch = MagicMock(return_value=es)
    result = index_job._load_data('index', index_job.index_interval.latest_ingest_time)
    assert len(result) == 0


def test__store_results(index_job, es_config):
    es = ElasticsearchService(es_config)
    es.connect = MagicMock()
    es.save = MagicMock()
    es.delete_logs_for_index = MagicMock()
    ServiceProvider.provide_elasticsearch = MagicMock(return_value=es)
    results = processed_logs[:4]
    index_job._store_results(results, "index")
    es.save.assert_called_once()
