"""
app/db.py
SQLite database for storing monitored routes.
Phase 2 — simple, no ORM, just sqlite3.
"""
from __future__ import annotations
import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

DB_PATH = Path("data/app.db")


def get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS monitored_routes (
            id           TEXT PRIMARY KEY,
            label        TEXT NOT NULL,
            origin       TEXT NOT NULL,
            destination  TEXT NOT NULL,
            date_start   TEXT NOT NULL,
            date_end     TEXT NOT NULL,
            passengers   INTEGER NOT NULL DEFAULT 1,
            cabin        TEXT NOT NULL DEFAULT 'economy',
            hubs         TEXT NOT NULL DEFAULT '[]',
            target_price REAL,
            active       INTEGER NOT NULL DEFAULT 1,
            created_at   TEXT NOT NULL,
            updated_at   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS price_history (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            route_id        TEXT NOT NULL,
            checked_at      TEXT NOT NULL,
            direct_price    REAL,
            best_split_hub  TEXT,
            best_split_price REAL,
            winner          TEXT,
            savings         REAL,
            raw_result      TEXT
        );
        """)
    log.info("Database initialized at %s", DB_PATH)


def upsert_route(
    id: str,
    label: str,
    origin: str,
    destination: str,
    date_start: str,
    date_end: str,
    passengers: int,
    cabin: str,
    hubs: list[str],
    target_price: Optional[float] = None,
) -> None:
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute("""
        INSERT INTO monitored_routes
            (id, label, origin, destination, date_start, date_end,
             passengers, cabin, hubs, target_price, active, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?)
        ON CONFLICT(id) DO UPDATE SET
            label=excluded.label,
            origin=excluded.origin,
            destination=excluded.destination,
            date_start=excluded.date_start,
            date_end=excluded.date_end,
            passengers=excluded.passengers,
            cabin=excluded.cabin,
            hubs=excluded.hubs,
            target_price=excluded.target_price,
            updated_at=excluded.updated_at
        """, (id, label, origin, destination, date_start, date_end,
              passengers, cabin, json.dumps(hubs), target_price, now, now))
    log.info("Upserted route: %s", id)


def get_active_routes() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM monitored_routes WHERE active=1 ORDER BY created_at"
        ).fetchall()
    routes = []
    for row in rows:
        r = dict(row)
        r["hubs"] = json.loads(r["hubs"])
        routes.append(r)
    return routes


def save_price_history(
    route_id: str,
    direct_price: Optional[float],
    best_split_hub: Optional[str],
    best_split_price: Optional[float],
    winner: str,
    savings: float,
    raw_result: dict,
) -> None:
    with get_conn() as conn:
        conn.execute("""
        INSERT INTO price_history
            (route_id, checked_at, direct_price, best_split_hub,
             best_split_price, winner, savings, raw_result)
        VALUES (?,?,?,?,?,?,?,?)
        """, (
            route_id,
            datetime.utcnow().isoformat(),
            direct_price,
            best_split_hub,
            best_split_price,
            winner,
            savings,
            json.dumps(raw_result),
        ))


def get_price_history(route_id: str, limit: int = 90) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute("""
        SELECT * FROM price_history
        WHERE route_id=?
        ORDER BY checked_at DESC
        LIMIT ?
        """, (route_id, limit)).fetchall()
    return [dict(row) for row in rows]


def get_last_price(route_id: str) -> Optional[float]:
    with get_conn() as conn:
        row = conn.execute("""
        SELECT direct_price FROM price_history
        WHERE route_id=?
        ORDER BY checked_at DESC
        LIMIT 1
        """, (route_id,)).fetchone()
    return row["direct_price"] if row else None
