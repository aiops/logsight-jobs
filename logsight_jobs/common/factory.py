from logsight.common.patterns.concurrent_job_manager import QueueableJobManager
from logsight_jobs.common.job_dispatcher import PeriodicJobDispatcher, TimedJobDispatcher
from logsight_jobs.jobs.delete_es_data_job import DeleteESDataJob
from logsight_jobs.jobs.incidents import CalculateIncidentJob
from logsight_jobs.jobs.log_aggregation import CalculateLogAggregationJob
from logsight_jobs.persistence.timestamp_storage import TimestampStorageProvider


class JobDispatcherFactory:
    @staticmethod
    def get_incident_dispatcher(n_jobs: int, timeout_period: int) -> PeriodicJobDispatcher:
        manager = QueueableJobManager(n_jobs)
        storage = TimestampStorageProvider.provide_timestamp_storage('incidents')
        return PeriodicJobDispatcher(job=CalculateIncidentJob,
                                     job_manager=manager,
                                     storage=storage,
                                     timeout_period=timeout_period,
                                     timer_name="CalculateIncidentJob")

    @staticmethod
    def get_log_agg_dispatcher(n_jobs: int, timeout_period: int) -> PeriodicJobDispatcher:
        manager = QueueableJobManager(n_jobs)
        storage = TimestampStorageProvider.provide_timestamp_storage('log_agg')
        return PeriodicJobDispatcher(job=CalculateLogAggregationJob,
                                     job_manager=manager,
                                     storage=storage,
                                     timeout_period=timeout_period,
                                     timer_name="CalculateLogAgg")

    @staticmethod
    def get_es_delete_idx_dispatcher(timeout_period: int, cleanup_age: str) -> TimedJobDispatcher:
        job = DeleteESDataJob(cleanup_age=cleanup_age)
        return TimedJobDispatcher(job=job,
                                  timeout_period=timeout_period,
                                  timer_name="CalculateLogAgg")
