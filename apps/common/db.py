"""Connexion PostgreSQL avec attente au démarrage."""
import logging
import time

import psycopg

from common import config

log = logging.getLogger(__name__)


def conninfo():
    return (
        f"host={config.POSTGRES_HOST} port={config.POSTGRES_PORT} "
        f"dbname={config.POSTGRES_DB} user={config.POSTGRES_USER} "
        f"password={config.POSTGRES_PASSWORD}"
    )


def connect(timeout_s=120):
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            return psycopg.connect(conninfo(), connect_timeout=5)
        except psycopg.OperationalError as exc:
            if time.monotonic() > deadline:
                raise
            log.info("PostgreSQL indisponible (%s), nouvelle tentative…", exc)
            time.sleep(3)
