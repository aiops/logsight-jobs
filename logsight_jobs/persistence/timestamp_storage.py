import logging
from abc import ABC, abstractmethod
from typing import List

from dacite import from_dict

from logsight.connectors.builders.config_provider import ConnectorConfigProvider
from logsight.connectors.connectors.sql_db import DatabaseConfigProperties
from logsight.services import PostgresDBService
import logsight_jobs.persistence.sql_statements as statements
from logsight_jobs.persistence.dto import IndexInterval

logger = logging.getLogger("logsight")


class TimestampStorageProvider:
    @staticmethod
    def provide_timestamp_storage(table):
        return PostgresTimestampStorage(table, ConnectorConfigProvider().get_config("database")())


class TimestampStorage(ABC):
    def __init__(self, table=None):
        self.__table__ = table or "timestamps"

    @abstractmethod
    def get_timestamps_for_index(self, index: str) -> IndexInterval:
        raise NotImplementedError

    def get_all(self) -> List[IndexInterval]:
        raise NotImplementedError

    @abstractmethod
    def update_timestamps(self, timestamps: IndexInterval) -> IndexInterval:
        raise NotImplementedError

    @abstractmethod
    def select_all_index(self) -> List[str]:
        raise NotImplementedError


class PostgresTimestampStorage(TimestampStorage, PostgresDBService):
    def __init__(self, table, config: DatabaseConfigProperties):
        PostgresDBService.__init__(self, config)
        TimestampStorage.__init__(self, table)

    def select_all_index(self) -> List[str]:
        sql = statements.SELECT_ALL_INDEX
        rows = [row['index'] for row in self._read_many(sql % self.__table__)]
        return rows

    def get_timestamps_for_index(self, index: str) -> IndexInterval:
        sql = statements.SELECT_FOR_INDEX
        row = self._read_one(sql % (self.__table__, index))
        return from_dict(data_class=IndexInterval, data=row)

    def get_all(self) -> List[IndexInterval]:
        sql = statements.SELECT_ALL
        rows = self._read_many(sql % self.__table__)
        return [from_dict(data_class=IndexInterval, data=row) for row in rows]

    def update_timestamps(self, timestamps: IndexInterval):
        sql = statements.UPDATE_TIMESTAMPS
        row = self._execute_sql(
            sql % (self.__table__, timestamps.index, timestamps.latest_ingest_time))
        return from_dict(data_class=IndexInterval, data=row)

    def _verify_database_exists(self, conn):
        super()._verify_database_exists(conn)
        self._auto_create_table(conn)

    def _auto_create_table(self, conn):
        table = conn.execute(statements.SELECT_TABLE, self.__table__).fetchall()
        if not table:
            logger.info(f"Table {self.__table__} not found. Auto-creating table {self.__table__}.")
            conn.execute(statements.CREATE_TABLE % self.__table__)
