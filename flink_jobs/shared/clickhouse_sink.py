import logging
import time
from pyflink.datastream.functions import MapFunction
import clickhouse_driver

logger = logging.getLogger(__name__)

class ClickHouseSink(MapFunction):
    """    
    PyFlink's MapFunction natively supports the open() and close() lifecycle 
    methods inside the Python Worker process, allowing efficient micro-batching.
    
    Subclasses must implement:
      - table_name: str
      - columns: list[str]
      - to_record(value) -> tuple
    """

    def __init__(self, host: str, port: int, database: str,
                 user: str, password: str, batch_size: int = 1000, 
                 flush_interval_sec: float = 2.0):
        super().__init__()
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

        # Batching configuration thresholds
        self.batch_size = batch_size
        self.flush_interval_sec = flush_interval_sec

        # Internal state managed per parallel task slot
        self.client = None
        self.buffer = []
        self.last_flush_time = 0.0

    def open(self, runtime_context):
        """
        Invoked once by the Flink Python worker during task initialization.
        Establishes the connection to ClickHouse and initializes the buffers.
        """
        self.client = clickhouse_driver.Client(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )
        self.buffer = []
        self.last_flush_time = time.time()
        logger.info(
            f"ClickHouse Side-Effect Sink opened: {self.host}:{self.port} "
            f"(batch_size={self.batch_size}, flush_interval={self.flush_interval_sec}s)"
        )

    def map(self, value):
        """
        Processes each incoming record. Acts as the functional equivalent of invoke().
        Accumulates records into the internal buffer and triggers a flush when thresholds are met.
        """
        # Transform the internal Flink Row/Tuple into a ClickHouse-compatible tuple
        record = self.to_record(value)
        self.buffer.append(record)
        
        current_time = time.time()
        time_since_last_flush = current_time - self.last_flush_time

        # Trigger flush if either the batch size or the time interval threshold is reached
        if len(self.buffer) >= self.batch_size or time_since_last_flush >= self.flush_interval_sec:
            self.flush()

        return value

    def flush(self):
        """
        Executes a native batch INSERT into ClickHouse using the accumulated buffer.
        Resets the internal buffer state and the interval timer.
        """
        if not self.buffer:
            self.last_flush_time = time.time()
            return

        columns_str = ", ".join(self.columns)
        query = f"INSERT INTO {self.table_name} ({columns_str}) VALUES"

        try:
            self.client.execute(query, self.buffer)
            logger.info(f"Successfully flushed {len(self.buffer)} records to ClickHouse table: {self.table_name}")
        except Exception as e:
            # In production, we might want to raise an exception here to trigger Flink failover,
            # or route corrupted records to a Dead Letter Queue (DLQ).
            logger.error(f"Failed to flush batch to ClickHouse table {self.table_name}: {e}")
            raise e
        finally:
            # Always clear the buffer and reset the timer, even if the insert failed
            self.buffer.clear()
            self.last_flush_time = time.time()

    def close(self):
        """
        Invoked once by the Flink Python worker during a graceful shutdown or failover.
        Guarantees that any remaining records in the buffer are pushed before connection teardown.
        """
        logger.info(f"Closing ClickHouse Sink for {self.table_name}. Executing final flush...")
        self.flush()
        
        if self.client:
            self.client.disconnect()
            logger.info(f"ClickHouse connection for {self.table_name} closed cleanly.")

    @property
    def table_name(self) -> str:
        raise NotImplementedError("Subclasses must specify the target ClickHouse table name.")

    @property
    def columns(self) -> list:
        raise NotImplementedError("Subclasses must define the list of column names.")

    def to_record(self, value) -> tuple:
        raise NotImplementedError("Subclasses must implement custom row-to-tuple mapping logic.")