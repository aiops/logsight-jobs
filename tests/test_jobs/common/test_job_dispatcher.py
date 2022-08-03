from datetime import datetime
import random
from unittest.mock import MagicMock

import pytest
from logsight.connectors.connectors.elasticsearch import ElasticsearchConfigProperties

from logsight.connectors.connectors.sql_db import DatabaseConfigProperties
from logsight.services import ElasticsearchService
from logsight.services.service_provider import ServiceProvider

from logsight_jobs.common.job_dispatcher import PeriodicJobDispatcher, TimedJobDispatcher
from logsight_jobs.common.factory import JobDispatcherFactory
from logsight_jobs.persistence.dto import IndexInterval
from logsight_jobs.persistence.timestamp_storage import PostgresTimestampStorage


@pytest.fixture
def db():
    cfg = DatabaseConfigProperties(host="host", port=9000, username="username", password="password", db_name="db_name")
    db = PostgresTimestampStorage("table", cfg)
    db.connect = MagicMock()
    db.update_timestamps = MagicMock()
    db.close = MagicMock()
    return db


@pytest.fixture
def es_config():
    yield ElasticsearchConfigProperties(scheme="scheme", host="host", port=9201, username="user", password="password")


@pytest.fixture
def job_dispatcher(db):
    job_dispatcher = JobDispatcherFactory.get_log_agg_dispatcher(2, 10)
    job_dispatcher.storage = db
    return job_dispatcher


def get_index_intervals(n_intervals):
    return [IndexInterval("index", *random_times("2020-01-01 00:00:00", "2022-01-01 00:00:00", 1)) for _ in
            range(n_intervals)]


def random_times(start, end, n):
    frmt = '%Y-%m-%d %H:%M:%S'
    stime = datetime.strptime(start, frmt)
    etime = datetime.strptime(end, frmt)
    td = etime - stime
    return [random.random() * td + stime for _ in range(n)]


def test_submit_job(job_dispatcher):
    n_intervals = 5
    index_intervals = get_index_intervals(n_intervals)
    job_dispatcher.storage = MagicMock()
    job_dispatcher.timer = MagicMock()
    job_dispatcher.sync_index = MagicMock()
    job_dispatcher.manager.submit_job = MagicMock(side_effect=None)
    job_dispatcher.manager.pool = MagicMock(side_effect=[])
    job_dispatcher.storage.__table__ = MagicMock()
    job_dispatcher.storage.get_all = MagicMock(side_effect=[index_intervals])
    job_dispatcher.submit_job()
    assert job_dispatcher.manager.submit_job.call_count == n_intervals


def test_sync_index(job_dispatcher):
    index_intervals = [f"index_{i}" for i in range(5)]
    current = index_intervals[2:]
    idx = set(index_intervals).difference(set(current))

    job_dispatcher.storage.update_timestamps = MagicMock()
    job_dispatcher.select_all_es_index = MagicMock(side_effect=[index_intervals])
    job_dispatcher.storage.select_all_index = MagicMock(side_effect=[current])

    job_dispatcher.sync_index()

    assert job_dispatcher.storage.update_timestamps.call_count == len(idx)


def test_select_es_index(es_config):
    es = ElasticsearchService(es_config)
    es._connect = MagicMock()
    es.get_all_indices = MagicMock(return_value=["index_1", "index_2", "index_3"])
    ServiceProvider.provide_elasticsearch = MagicMock(return_value=es)

    result = PeriodicJobDispatcher.select_all_es_index()
    assert result == {"index"}


def test_start(job_dispatcher):
    job_dispatcher.timer.start = MagicMock()
    job_dispatcher.run()
    job_dispatcher.timer.start.assert_called()


def test_timed_job_dispatcher():
    job = MagicMock()
    job.execute = MagicMock()
    dispatcher = TimedJobDispatcher(job, 10, 'timer')
    dispatcher.timer = MagicMock()
    dispatcher.timer.reset_timer = MagicMock()

    dispatcher.submit_job()

    job.execute.assert_called_once()
    dispatcher.timer.reset_timer.assert_called_once()


def test_timed_job_dispatcher_start():
    dispatcher = TimedJobDispatcher(MagicMock(), 10, 'timer')
    dispatcher.timer = MagicMock()
    dispatcher.timer.start = MagicMock()

    dispatcher.run()
    assert dispatcher.timer.daemon is True
    dispatcher.timer.start.assert_called_once()
