import logging
import logging.handlers
import threading
import time

try:
    # Python 3.7+
    from queue import SimpleQueue as _Queue
except ImportError:
    from queue import Queue as _Queue  # type: ignore[assignment]


class QueueHandler(logging.Handler):
    """Minimal, speed-focused handler that just enqueues records."""

    def __init__(self, q: _Queue):
        super().__init__()
        self._queue = q

    def emit(self, record: logging.LogRecord) -> None:
        # Fast path: no formatting, minimal work.
        # Let the background handler format and write.
        self._queue.put(record)


class QueueListener(logging.handlers.TimedRotatingFileHandler):
    """Background file logger consuming from a queue."""

    _SENTINEL = object()

    def __init__(self, filename: str, rotate_log_at_restart: bool) -> None:
        when = "S" if rotate_log_at_restart else "midnight"
        interval = 60 * 60 * 24 if rotate_log_at_restart else 1

        super().__init__(filename, when=when, interval=interval, backupCount=5)

        self.bg_queue: _Queue = _Queue()
        self.rollover_info = {}

        t = threading.Thread(target=self._bg_thread, daemon=True)
        self._bg_thread_obj = t
        t.start()

    def _bg_thread(self) -> None:
        q_get = self.bg_queue.get
        handle = self.handle
        sentinel = self._SENTINEL

        while True:
            record = q_get()
            if record is sentinel:
                break
            handle(record)

    def stop(self) -> None:
        self.bg_queue.put(self._SENTINEL)
        self._bg_thread_obj.join()

    def set_rollover_info(self, name: str, info: str | None) -> None:
        if info is None:
            self.rollover_info.pop(name, None)
        else:
            self.rollover_info[name] = info

    def clear_rollover_info(self) -> None:
        self.rollover_info.clear()

    def doRollover(self) -> None:
        super().doRollover()

        lines = [self.rollover_info[n] for n in sorted(self.rollover_info)]
        lines.append(
            "=============== Log rollover at %s ==============="
            % time.asctime()
        )
        msg = "\n".join(lines)

        # Rollover is rare, so simple path here is fine.
        self.handle(logging.makeLogRecord({"msg": msg, "level": logging.INFO}))


_MainQueueHandler: QueueHandler | None = None


def setup_bg_logging(
    filename: str, debuglevel: int, rotate_log_at_restart: bool
) -> QueueListener:
    global _MainQueueHandler

    ql = QueueListener(filename=filename,
                       rotate_log_at_restart=rotate_log_at_restart)
    qh = QueueHandler(ql.bg_queue)
    _MainQueueHandler = qh

    root = logging.getLogger()
    root.addHandler(qh)
    root.setLevel(debuglevel)
    return ql


def clear_bg_logging() -> None:
    global _MainQueueHandler
    if _MainQueueHandler is not None:
        root = logging.getLogger()
        root.removeHandler(_MainQueueHandler)
        root.setLevel(logging.WARNING)
        _MainQueueHandler = None
