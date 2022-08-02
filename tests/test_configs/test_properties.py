from configs.properties import ESCleanupConfigProperties, IncidentJobConfigProperties, JobConfigProperties


def test_default_incident_properties():
    properties = IncidentJobConfigProperties()
    assert properties.n_jobs == 2
    assert properties.interval == 60


def test_default_es_cleanup_properties():
    properties = ESCleanupConfigProperties()
    assert properties.cleanup_age == "now-1y"
    assert properties.interval == 60


def test_default_job_config_properties():
    properties = JobConfigProperties()
    assert hasattr(properties, "incident_job")
    assert hasattr(properties, "es_cleanup_job")
    assert isinstance(properties.incident_job, IncidentJobConfigProperties)
    assert isinstance(properties.es_cleanup_job, ESCleanupConfigProperties)
