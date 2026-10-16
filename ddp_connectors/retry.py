"""Bounded retries for explicitly safe operations, independent of driver APIs."""
import time
import math
from loguru import logger


class RetryPolicy:
    max_connection_retries = 3
    retry_backoff_seconds = 2

    @staticmethod
    def _safe_close(resource):
        if resource is not None:
            try:
                resource.close()
            except Exception:
                logger.debug("Could not close connector resource")

    def _read_attempts(self):
        if not isinstance(self.max_connection_retries, int) or self.max_connection_retries < 0:
            raise ValueError("max_connection_retries must be a non-negative integer")
        if (not isinstance(self.retry_backoff_seconds, (int, float))
                or not math.isfinite(self.retry_backoff_seconds) or self.retry_backoff_seconds < 0):
            raise ValueError("retry_backoff_seconds must be finite and non-negative")
        return range(self.max_connection_retries + 1)

    def _wait_for_retry(self, operation_name, retry_number):
        logger.warning(
            f"{type(self).__name__} retry {retry_number}/{self.max_connection_retries} "
            f"for {operation_name} after a connection failure"
        )
        time.sleep(retry_number * self.retry_backoff_seconds)

    def _is_retryable_connection_error(self, error, *, connecting=False):
        return False

    def _run_with_retry(self, operation_name, operation, *, connecting=False):
        for attempt in self._read_attempts():
            try:
                return operation()
            except Exception as error:
                if attempt >= self.max_connection_retries or not self._is_retryable_connection_error(error, connecting=connecting):
                    raise
            self._wait_for_retry(operation_name, attempt + 1)

    def _iterate_with_retry(self, operation_name, operation):
        emitted = False
        for attempt in self._read_attempts():
            iterator = None
            try:
                iterator = operation()
                for item in iterator:
                    emitted = True
                    yield item
                return
            except Exception as error:
                if emitted or attempt >= self.max_connection_retries or not self._is_retryable_connection_error(error):
                    raise
            finally:
                self._safe_close(iterator)
            self._wait_for_retry(operation_name, attempt + 1)

    def _connect_with_retry(self):
        return self._run_with_retry("connect", self._open_connection, connecting=True)
