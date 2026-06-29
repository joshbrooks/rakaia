import threading

from rakaia import StreamStore

_store_instance: StreamStore | None = None
_store_lock = threading.Lock()


def get_store() -> StreamStore:
    """
    Get the global StreamStore instance for django_rakaia.
    Thread-safe lazy initialization.
    """
    global _store_instance
    if _store_instance is None:
        with _store_lock:
            if _store_instance is None:
                _store_instance = StreamStore()
    return _store_instance
