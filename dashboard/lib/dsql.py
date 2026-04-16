"""DSQL connection with automatic IAM token refresh."""

import secrets
import time

import boto3
import psycopg2


FIRST_NAMES = [
    "Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank",
    "Ivy", "Jack", "Karen", "Leo", "Mona", "Nate", "Olivia", "Paul",
    "Quinn", "Rosa", "Sam", "Tina", "Uma", "Vic", "Wendy", "Xander",
    "Yara", "Zane",
]

LAST_NAMES = [
    "Adams", "Baker", "Clark", "Diaz", "Evans", "Foster", "Garcia",
    "Harris", "Ito", "Jones", "Kim", "Lee", "Martinez", "Nguyen",
    "Ortiz", "Patel", "Quinn", "Rivera", "Smith", "Taylor", "Ueda",
    "Vargas", "Wang", "Xu", "Young", "Zhang",
]


class DsqlConnection:
    """Wraps psycopg2 with automatic DSQL IAM token refresh."""

    TOKEN_REFRESH = 14 * 60  # refresh before 15-min expiry

    def __init__(self, host: str, region: str, dsql_endpoint: str = ""):
        self.host = host
        self.region = region
        self.dsql_endpoint = dsql_endpoint
        self._conn = None
        self._token_time = 0.0

    def _generate_token(self) -> str:
        kwargs = {"Hostname": self.host, "Region": self.region}
        if self.dsql_endpoint:
            kwargs["endpoint_url"] = self.dsql_endpoint
        client = boto3.client("dsql", region_name=self.region,
                              **({"endpoint_url": self.dsql_endpoint} if self.dsql_endpoint else {}))
        return client.generate_db_connect_admin_auth_token(
            Hostname=self.host, Region=self.region
        )

    def get(self):
        """Return a live psycopg2 connection, refreshing the token if needed."""
        now = time.time()
        if self._conn and (now - self._token_time) < self.TOKEN_REFRESH:
            try:
                with self._conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return self._conn
            except Exception:
                pass
        # Reconnect
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
        token = self._generate_token()
        self._conn = psycopg2.connect(
            host=self.host, port=5432, dbname="postgres",
            user="admin", password=token, sslmode="require",
        )
        self._conn.autocommit = True
        self._token_time = time.time()
        return self._conn

    def execute(self, sql: str, params=None) -> list[dict]:
        """Run SQL and return rows as list of dicts."""
        conn = self.get()
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description:
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
            return []

    def close(self):
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def generate_events(self, count: int = 10) -> list[dict]:
        """Insert random events into the DSQL events table."""
        conn = self.get()
        rows = []
        with conn.cursor() as cur:
            for _ in range(count):
                first = secrets.choice(FIRST_NAMES)
                last = secrets.choice(LAST_NAMES)
                name = f"{first} {last}"
                email = f"{first.lower()}.{last.lower()}@example.com"
                cur.execute(
                    "INSERT INTO public.events (name, email) VALUES (%s, %s) RETURNING id, name, email, created_at",
                    (name, email),
                )
                row = cur.fetchone()
                if row:
                    rows.append({"id": str(row[0]), "name": row[1], "email": row[2], "created_at": str(row[3])})
        return rows

    def mutate_events(self, count: int = 5) -> list[dict]:
        """Randomly UPDATE or DELETE existing events."""
        conn = self.get()
        ops = []
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, email FROM public.events ORDER BY random() LIMIT %s", (count,))
            targets = cur.fetchall()
            for row_id, name, email in targets:
                if secrets.randbelow(2) == 0:
                    new_email = f"{name.split()[0].lower()}.updated@example.com"
                    cur.execute("UPDATE public.events SET email = %s WHERE id = %s", (new_email, row_id))
                    ops.append({"op": "UPDATE", "id": str(row_id), "name": name, "email": new_email})
                else:
                    cur.execute("DELETE FROM public.events WHERE id = %s", (row_id,))
                    ops.append({"op": "DELETE", "id": str(row_id), "name": name, "email": email})
        return ops

    def clear_events(self) -> int:
        """Delete all events. Returns count deleted."""
        conn = self.get()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM public.events")
            return cur.rowcount

    def count_events(self) -> int:
        """Count rows in the events table."""
        rows = self.execute("SELECT count(*) AS cnt FROM public.events")
        return int(rows[0]["cnt"]) if rows else 0
