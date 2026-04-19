"""
services/scheduler.py
Daily price monitoring scheduler.
Runs every morning, checks all active routes,
writes to Sheets, sends email alerts.
"""
from __future__ import annotations
import json
import logging
import os
import smtplib
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

log = logging.getLogger(__name__)


# ── Route definitions ─────────────────────────────────────────────────────────

MONITORED_ROUTES = [
    # PHX -> BCN, Jun 7-12, 2 passengers (parents)
    {
        "id":          "PHX-BCN-JUN-2PAX",
        "label":       "PHX → BCN (Parents, 2 pax)",
        "origin":      "PHX",
        "destination": "BCN",
        "date_start":  "2026-06-07",
        "date_end":    "2026-06-12",
        "passengers":  2,
        "cabin":       "business",
        "hubs":        ["ORD", "ATL", "JFK", "MIA", "EWR"],
        "target_price": None,
    },
    # PHX -> BCN, Jun 7-12, 4 passengers (Doug's family)
    {
        "id":          "PHX-BCN-JUN-4PAX",
        "label":       "PHX → BCN (Family, 4 pax)",
        "origin":      "PHX",
        "destination": "BCN",
        "date_start":  "2026-06-07",
        "date_end":    "2026-06-12",
        "passengers":  4,
        "cabin":       "business",
        "hubs":        ["ORD", "ATL", "JFK", "MIA", "EWR"],
        "target_price": None,
    },
    # ATH -> PHX, Jun 24-28, 2 passengers (parents)
    {
        "id":          "ATH-PHX-JUN-2PAX",
        "label":       "ATH → PHX (Parents, 2 pax)",
        "origin":      "ATH",
        "destination": "PHX",
        "date_start":  "2026-06-24",
        "date_end":    "2026-06-28",
        "passengers":  2,
        "cabin":       "business",
        "hubs":        ["JFK", "ORD", "ATL", "EWR", "IAD"],
        "target_price": None,
    },
    # ATH -> PHX, Jun 24-28, 4 passengers (Doug's family)
    {
        "id":          "ATH-PHX-JUN-4PAX",
        "label":       "ATH → PHX (Family, 4 pax)",
        "origin":      "ATH",
        "destination": "PHX",
        "date_start":  "2026-06-24",
        "date_end":    "2026-06-28",
        "passengers":  4,
        "cabin":       "business",
        "hubs":        ["JFK", "ORD", "ATL", "EWR", "IAD"],
        "target_price": None,
    },
]


# ── Date helpers ──────────────────────────────────────────────────────────────

def dates_in_range(date_start: str, date_end: str) -> list[str]:
    """Return all dates between date_start and date_end inclusive."""
    start = datetime.strptime(date_start, "%Y-%m-%d").date()
    end   = datetime.strptime(date_end,   "%Y-%m-%d").date()
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


# ── Email ─────────────────────────────────────────────────────────────────────

def send_daily_email(results: list[dict]) -> None:
    email_from = os.getenv("EMAIL_FROM", "")
    email_pass = os.getenv("EMAIL_PASSWORD", "")
    email_to   = os.getenv("EMAIL_TO", "")

    if not all([email_from, email_pass, email_to]):
        log.warning("Email not configured — skipping alert")
        return

    recipients = [e.strip() for e in email_to.split(",") if e.strip()]
    today_str  = date.today().strftime("%B %d, %Y")

    rows_html = ""
    for r in results:
        trend    = "🟢 New low!" if r.get("is_new_low") else ("📉 Down" if r.get("dropped") else "📈 Up")
        price    = f"${r['direct_price']:.0f}" if r.get("direct_price") else "N/A"
        split    = f"${r['best_split_price']:.0f} via {r['best_split_hub']}" if r.get("best_split_price") else "—"
        savings  = f"Saves ${r['savings']:.0f}" if r.get("savings", 0) > 0 else "Direct is cheaper"
        winner   = "✦ SPLIT" if r.get("winner") == "split" else "Direct"

        rows_html += f"""
        <tr style="border-bottom:1px solid #eee;">
          <td style="padding:12px 8px;">
            <div style="font-weight:600;font-size:14px;">{r['label']}</div>
            <div style="font-size:11px;color:#666;margin-top:2px;">{r['date']} · {r['cabin'].title()} · {r['passengers']} pax</div>
          </td>
          <td style="padding:12px 8px;text-align:center;">
            <div style="font-size:18px;font-weight:700;">{price}</div>
            <div style="font-size:11px;color:#999;">direct</div>
          </td>
          <td style="padding:12px 8px;text-align:center;">
            <div style="font-size:13px;font-weight:600;">{split}</div>
            <div style="font-size:11px;color:#085041;">{savings}</div>
          </td>
          <td style="padding:12px 8px;text-align:center;font-weight:700;">{winner}</td>
          <td style="padding:12px 8px;text-align:center;">{trend}</td>
          <td style="padding:12px 8px;text-align:center;">
            <a href="{r.get('gf_url','#')}" style="display:inline-block;background:#1a1a18;color:#fff;padding:6px 12px;border-radius:6px;text-decoration:none;font-size:11px;font-weight:600;margin-bottom:3px;">Google Flights</a><br>
            <a href="{r.get('kayak_url','#')}" style="display:inline-block;background:#ff6900;color:#fff;padding:6px 12px;border-radius:6px;text-decoration:none;font-size:11px;font-weight:600;">Kayak</a>
          </td>
        </tr>"""

    spreadsheet_id = os.getenv("SPREADSHEET_ID", "")
    sheets_link = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}" if spreadsheet_id else "#"

    html = f"""<!DOCTYPE html><html><body style="font-family:-apple-system,sans-serif;background:#f5f5f3;margin:0;padding:20px;">
<div style="max-width:800px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.08);">
  <div style="background:#1a1a18;padding:24px 28px;">
    <div style="font-size:22px;font-weight:700;color:#fff;">✈ Daily Flight Prices</div>
    <div style="font-size:13px;color:#a8a89f;margin-top:4px;">{today_str} — {len(results)} routes checked</div>
  </div>
  <div style="padding:20px 28px 0;">
    <p style="font-size:14px;margin:0 0 16px;">Good morning! Here are today's prices for your tracked routes. Click a button to see live fares and book.</p>
  </div>
  <div style="padding:0 28px;">
    <table style="width:100%;border-collapse:collapse;">
      <thead><tr style="border-bottom:2px solid #eee;">
        <th style="padding:8px;text-align:left;font-size:11px;color:#999;text-transform:uppercase;">Route</th>
        <th style="padding:8px;text-align:center;font-size:11px;color:#999;text-transform:uppercase;">Direct</th>
        <th style="padding:8px;text-align:center;font-size:11px;color:#999;text-transform:uppercase;">Best Split</th>
        <th style="padding:8px;text-align:center;font-size:11px;color:#999;text-transform:uppercase;">Winner</th>
        <th style="padding:8px;text-align:center;font-size:11px;color:#999;text-transform:uppercase;">Trend</th>
        <th style="padding:8px;text-align:center;font-size:11px;color:#999;text-transform:uppercase;">Book</th>
      </tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>
  <div style="padding:20px 28px;background:#f5f5f3;margin-top:20px;">
    <p style="font-size:12px;color:#666;margin:0;">
      <a href="{sheets_link}" style="color:#185FA5;font-weight:600;">View full price history in Google Sheets →</a><br><br>
      Prices via Duffel GDS. Book directly on airline websites for best service.
      Split ticket = two separate one-way bookings — allow 3+ hrs at connecting hub.
    </p>
  </div>
</div></body></html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"✈ Flight Prices — {today_str}"
    msg["From"]    = email_from
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(email_from, email_pass)
            smtp.sendmail(email_from, recipients, msg.as_string())
        log.info("Daily email sent to %s", recipients)
    except Exception as e:
        log.error("Email error: %s", e)


# ── Main run ──────────────────────────────────────────────────────────────────

async def run_daily_check(duffel_client) -> None:
    """
    Main entry point for the daily price check.
    Called by the scheduler or manually via POST /run.
    """
    from services.compare import CompareEngine
    from services.sheets import write_compare_result
    from app.models import CabinClass, CompareRequest, Passenger
    from app.db import save_price_history, get_last_price, init_db

    init_db()
    engine     = CompareEngine(duffel_client)
    checked_at = datetime.utcnow().isoformat()
    email_rows = []

    for route in MONITORED_ROUTES:
        dates = dates_in_range(route["date_start"], route["date_end"])
        log.info("Checking %s across %d dates", route["id"], len(dates))

        # Find best date in range
        best_result = None
        best_price  = None

        for d in dates:
            try:
                request = CompareRequest(
                    origin      = route["origin"],
                    destination = route["destination"],
                    date        = d,
                    hubs        = route["hubs"],
                    passengers  = [Passenger() for _ in range(route["passengers"])],
                    cabin       = CabinClass(route["cabin"]),
                )
                result = await engine.compare(request)

                # Track best (cheapest direct or split winner)
                price = result.winner_price
                if price and (best_price is None or price < best_price):
                    best_price  = price
                    best_result = result
                    best_result._best_date = d

            except Exception as e:
                log.error("Error checking %s on %s: %s", route["id"], d, e)
                continue

        if not best_result:
            log.warning("No results for %s", route["id"])
            continue

        # Get previous price for trend
        last_price = get_last_price(route["id"])
        dropped    = last_price and best_price and best_price < last_price
        is_new_low = dropped and (not last_price or best_price < last_price * 0.95)

        # Save to DB
        save_price_history(
            route_id         = route["id"],
            direct_price     = best_result.direct.total_amount if best_result.direct else None,
            best_split_hub   = best_result.best_split.hub if best_result.best_split else None,
            best_split_price = best_result.best_split.combined_price if best_result.best_split else None,
            winner           = best_result.winner.value,
            savings          = best_result.max_savings,
            raw_result       = best_result.to_summary_dict(),
        )

        # Write to Sheets
        write_compare_result(
            result     = best_result,
            route_id   = route["id"],
            label      = route["label"],
            checked_at = checked_at,
        )

        # Build email row
        email_rows.append({
            "label":           route["label"],
            "date":            getattr(best_result, "_best_date", str(best_result.date)),
            "cabin":           route["cabin"],
            "passengers":      route["passengers"],
            "direct_price":    best_result.direct.total_amount if best_result.direct else None,
            "best_split_hub":  best_result.best_split.hub if best_result.best_split else None,
            "best_split_price": best_result.best_split.combined_price if best_result.best_split else None,
            "savings":         best_result.max_savings,
            "winner":          best_result.winner.value,
            "dropped":         dropped,
            "is_new_low":      is_new_low,
            "gf_url":          best_result.direct.google_flights_url if best_result.direct else "",
            "kayak_url":       best_result.direct.kayak_url if best_result.direct else "",
        })

        log.info("Done: %s — best price $%.0f on %s",
                 route["id"], best_price or 0,
                 getattr(best_result, "_best_date", "?"))

    # Send email
    if email_rows:
        send_daily_email(email_rows)

    log.info("Daily check complete — %d routes processed", len(email_rows))
