import logging.config
import platform
from datetime import datetime
from multiprocessing import set_start_method

# hello world

from logsight_jobs.jobs.incidents import CalculateIncidentJob
from logsight_jobs.common.factory import JobDispatcherFactory
from logsight_jobs.persistence.timestamp_storage import TimestampStorageProvider
from logsight.logger.configuration import LogConfig
from logsight.services.service_provider import ServiceProvider
from logsight_jobs.configs.properties import JobConfigProperties
from logsight_jobs.persistence.dto import IndexInterval

logging.config.dictConfig(LogConfig().config)
logger = logging.getLogger('logsight')

# needed for running on Windows or macOS
if platform.system() != 'Linux':
    logger.debug(f"Start method fork for system {platform.system()}.")
    set_start_method("fork", force=True)

config = JobConfigProperties()


def verify_services():
    # Verify elasticsearch connection
    es = ServiceProvider.provide_elasticsearch()
    es.connect()

    # Verify db connection
    db = ServiceProvider.provide_postgres()
    db.connect()

    # Verify db connection for incidents
    if config.incident_job:
        ts = TimestampStorageProvider.provide_timestamp_storage("incidents")
        ts.connect()


def run_scheduled_jobs():
    # Run incidents
    if config.incident_job:
        incidents = JobDispatcherFactory.get_incident_dispatcher(config.incident_job.n_jobs,
                                                                 config.incident_job.interval)
        logger.info("Starting Incident Job Dispatcher.")
        incidents.run()

    if config.es_cleanup_job:
        logger.info("Starting Delete ES Index Job Dispatcher.")
        delete_es = JobDispatcherFactory.get_es_delete_idx_dispatcher(config.es_cleanup_job.interval,
                                                                      config.es_cleanup_job.cleanup_age)
        delete_es.run()


def run():
    incident_job = CalculateIncidentJob(IndexInterval("dwwd5tuvy6aqerrjkmr4pe635c", datetime.min))
    incident_job.execute()

    # verify_services()
    # run_scheduled_jobs()
    # while True:
    #     sleep(100)


if __name__ == '__main__':
    run()
