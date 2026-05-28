import logging
from pyflink.datastream import MapFunction
import clickhouse_driver

logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_DB = "default"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = ""

class ClickHouseSink(MapFunction):
    """
    Base ClickHouse sink implemented as MapFunction.
    Subclasses must implement:
      - table_name: str
      - to_record(value) -> tuple
      - columns: list[str]
    """

    def __init__(self, host: str, port: int, database: str,
                 user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.client = None

    def open(self, runtime_context):
        """Create ClickHouse connection once per TaskManager slot."""
        self.client = clickhouse_driver.Client(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )
        logger.info(f"ClickHouse connection opened: {self.host}:{self.port}")

    def map(self, value):
        """
        Called for each record.
        Converts value to record tuple and inserts into ClickHouse.
        Must return value, required by MapFunction contract.
        """
        record = self.to_record(value)
        columns = ", ".join(self.columns)

        try:
            self.client.execute(
                f"INSERT INTO {self.table_name} ({columns}) VALUES",
                [record],
            )
            logger.info(f"Inserted into {self.table_name}: {record[0]}")
        except Exception as e:
            logger.error(f"ClickHouse insert failed: {e}")

        return value

    def close(self):
        if self.client:
            self.client.disconnect()
            logger.info("ClickHouse connection closed")

    @property
    def table_name(self) -> str:
        raise NotImplementedError

    @property
    def columns(self) -> list:
        raise NotImplementedError

    def to_record(self, value) -> tuple:
        raise NotImplementedError