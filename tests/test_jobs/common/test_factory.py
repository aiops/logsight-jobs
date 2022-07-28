from logsight_jobs.common.factory import JobDispatcherFactory
from logsight_jobs.common.job_dispatcher import PeriodicJobDispatcher, TimedJobDispatcher
from logsight_jobs.jobs.delete_es_data_job import DeleteESDataJob
from logsight_jobs.persistence.timestamp_storage import PostgresTimestampStorage


def test_get_incident_dispatcher():
    max_workers = 2
    dispatcher = JobDispatcherFactory.get_incident_dispatcher(n_jobs=max_workers, timeout_period=10)
    assert dispatcher.manager.pool._max_workers == max_workers
    assert isinstance(dispatcher, PeriodicJobDispatcher)
    assert isinstance(dispatcher.storage, PostgresTimestampStorage)
    assert dispatcher.timer.name == "CalculateIncidentJob_timer"
    assert dispatcher.storage.__table__ == "incidents"


def test_get_log_agg_dispatcher():
    max_workers = 2
    dispatcher = JobDispatcherFactory.get_log_agg_dispatcher(n_jobs=max_workers, timeout_period=10)
    assert dispatcher.manager.pool._max_workers == max_workers
    assert isinstance(dispatcher, PeriodicJobDispatcher)
    assert isinstance(dispatcher.storage, PostgresTimestampStorage)
    assert dispatcher.timer.name == "CalculateLogAgg_timer"
    assert dispatcher.storage.__table__ == "log_agg"


def test_get_es_delete_idx_dispatcher():
    cleanup_age = "now-1y"
    dispatcher = JobDispatcherFactory.get_es_delete_idx_dispatcher(timeout_period=10, cleanup_age=cleanup_age)
    assert isinstance(dispatcher.job, DeleteESDataJob)
    assert isinstance(dispatcher, TimedJobDispatcher)
    assert dispatcher.job.cleanup_age == cleanup_age
