from typing import Any, Iterable, List

import psycopg2

connection = {
    "database": "dbt_eg_1",
    "user": "postgres",
    "password": "pass",
    "host": "localhost",
    "port": "54321",
}


def run_query(sql: str, values: Iterable[Any] = None) -> List[dict]:
    with psycopg2.connect(**connection) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, values)
            conn.commit()