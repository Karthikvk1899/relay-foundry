from contextlib import contextmanager

from psycopg_pool import ConnectionPool

from relay.config import settings

pool: ConnectionPool | None = None


def open_pool() -> None:
    global pool
    pool = ConnectionPool(settings.database_url, min_size=1, max_size=10, open=True)
    pool.wait(timeout=30)


def close_pool() -> None:
    global pool
    if pool:
        pool.close()
        pool = None


@contextmanager
def connection():
    if pool is None:
        raise RuntimeError("database is unavailable")
    with pool.connection() as conn:
        yield conn
