"""
services/sheets.py
Google Sheets writer for Phase 2.
Writes daily price results to two tabs:
  - "Flights"        : standard route results
  - "Repositioning"  : split-ticket comparisons
"""
from __future__ import annotations
import json
import logging
import os
from typing import Optional

log = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

FLIGHT_HEADERS = [
    "Date Checked", "Route ID", "Label", "Origin", "Destination",
    "Travel Date", "Cabin", "Passengers", "Direct Price",
    "Best Split Hub", "Best Split Price", "Savings ($)", "Savings (%)",
    "Winner", "Airline", "Depart", "Arrive", "Stops",
    "Google Flights URL", "Kayak URL", "Skyscanner URL", "Notes",
]

REPO_HEADERS = [
    "Date Checked", "Route ID", "Label", "Origin", "Destination",
    "Travel Date", "Cabin", "Passengers",
    "Leg 1 From", "Leg 1 To", "Leg 1 Date", "Leg 1 Price", "Leg 1 Airline",
    "Leg 2 From", "Leg 2 To", "Leg 2 Date", "Leg 2 Price", "Leg 2 Airline",
    "Combined Price", "Direct Price", "Savings ($)", "Savings (%)",
    "Winner", "Leg 1 GF URL", "Leg 1 Kayak URL",
    "Leg 2 GF URL", "Leg 2 Kayak URL",
    "Direct GF URL", "Direct Kayak URL", "Notes",
]


def _get_service():
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
    except ImportError:
        log.error("Google libraries not installed")
        return None

    creds_json = os.getenv("GOOGLE_CREDS_JSON", "")
    if not creds_json:
        log.warning("GOOGLE_CREDS_JSON not set — skipping Sheets")
        return None

    try:
        creds_dict = json.loads(creds_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
        return build("sheets", "v4", credentials=creds)
    except Exception as e:
        log.error("Sheets auth error: %s", e)
        return None


def _ensure_tab(service, spreadsheet_id: str, tab_name: str, headers: list[str]) -> None:
    try:
        meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        existing = [s["properties"]["title"] for s in meta["sheets"]]
        if tab_name not in existing:
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
            ).execute()
            log.info("Created tab: %s", tab_name)
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1:A1",
        ).execute()
        if not result.get("values"):
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{tab_name}!A1",
                valueInputOption="RAW",
                body={"values": [headers]},
            ).execute()
            log.info("Added headers to tab: %s", tab_name)
    except Exception as e:
        log.error("ensure_tab error for %s: %s", tab_name, e)


def _append_row(service, spreadsheet_id: str, tab_name: str, row: list) -> None:
    try:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [row]},
        ).execute()
    except Exception as e:
        log.error("append_row error for %s: %s", tab_name, e)


def write_compare_result(result, route_id: str, label: str, checked_at: str) -> bool:
    """
    Write a CompareResult to Google Sheets.
    Returns True if successful.
    """
    spreadsheet_id = os.getenv("SPREADSHEET_ID", "")
    if not spreadsheet_id:
        log.warning("SPREADSHEET_ID not set — skipping Sheets write")
        return False

    service = _get_service()
    if not service:
        return False

    try:
        _ensure_tab(service, spreadsheet_id, "Flights", FLIGHT_HEADERS)
        _ensure_tab(service, spreadsheet_id, "Repositioning", REPO_HEADERS)

        date_str    = str(result.date)
        cabin       = result.cabin.value
        passengers  = result.passengers
        direct      = result.direct
        best_split  = result.best_split

        direct_price = direct.total_amount if direct else None
        airline      = ""
        depart       = ""
        arrive       = ""
        stops        = ""
        gf_url       = ""
        kayak_url    = ""
        ss_url       = ""

        if direct and direct.slices:
            sl       = direct.slices[0]
            airline  = sl.carrier_label
            stops    = str(sl.stops)
            if sl.segments:
                depart = sl.segments[0].departing_at[:16].replace("T", " ")
                arrive = sl.segments[-1].arriving_at[:16].replace("T", " ")
            gf_url    = direct.google_flights_url or ""
            kayak_url = direct.kayak_url or ""
            ss_url    = direct.skyscanner_url or ""

        # Write to Flights tab
        flight_row = [
            checked_at, route_id, label,
            result.origin, result.destination, date_str, cabin, passengers,
            direct_price,
            best_split.hub if best_split else "",
            best_split.combined_price if best_split else "",
            result.max_savings or "",
            f"{result.max_savings_pct:.1f}%" if result.max_savings_pct else "",
            result.winner.value,
            airline, depart, arrive, stops,
            gf_url, kayak_url, ss_url,
            result.winner_label,
        ]
        _append_row(service, spreadsheet_id, "Flights", flight_row)

        # Write each split option to Repositioning tab
        for opt in result.split_options:
            if not opt.fully_available:
                continue
            l1    = opt.leg1
            l2    = opt.leg2
            l1_gf = l1.offer.google_flights_url if l1.offer else ""
            l1_ky = l1.offer.kayak_url if l1.offer else ""
            l2_gf = l2.offer.google_flights_url if l2.offer else ""
            l2_ky = l2.offer.kayak_url if l2.offer else ""
            l1_airline = l1.offer.slices[0].carrier_label if l1.offer and l1.offer.slices else ""
            l2_airline = l2.offer.slices[0].carrier_label if l2.offer and l2.offer.slices else ""
            savings_pct = f"{opt.savings_pct:.1f}%" if opt.savings_pct else ""

            repo_row = [
                checked_at, route_id, label,
                result.origin, result.destination, date_str, cabin, passengers,
                l1.origin, l1.destination, str(l1.date), l1.price or "", l1_airline,
                l2.origin, l2.destination, str(l2.date), l2.price or "", l2_airline,
                opt.combined_price or "", direct_price, opt.savings_vs_direct or "", savings_pct,
                "WINNER" if opt.is_winner else result.winner.value,
                l1_gf, l1_ky, l2_gf, l2_ky, gf_url, kayak_url,
                f"Via {opt.hub} — {opt.hub_carrier_hint or ''}",
            ]
            _append_row(service, spreadsheet_id, "Repositioning", repo_row)

        log.info("Written to Sheets: %s", route_id)
        return True

    except Exception as e:
        log.error("write_compare_result error: %s", e)
        return False
