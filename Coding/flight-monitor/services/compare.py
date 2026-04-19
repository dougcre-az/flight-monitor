from __future__ import annotations
import asyncio
import logging
import time
from datetime import date, timedelta
from typing import Optional
from app.config import get_hub_info, get_settings
from app.models import (
    BucketAnalysis, BucketBreak, BucketRequest, BucketRow,
    CabinClass, CompareRequest, CompareResult, CompareWinner,
    LegPrice, OfferSummary, Passenger, SplitTicketOption,
)
from services.duffel import DuffelClient, DuffelError

log = logging.getLogger(__name__)


class CompareEngine:
    def __init__(self, duffel: DuffelClient):
        self._duffel   = duffel
        self._settings = get_settings()

    async def compare(self, request: CompareRequest) -> CompareResult:
        t0 = time.monotonic()
        api_calls = 0
        log.info(
            "Compare: %s->%s on %s, %d hub(s), pax=%d, cabin=%s",
            request.origin, request.destination, request.date,
            len(request.hubs), request.passenger_count, request.cabin.value,
        )

        direct_offer, direct_error = await self._price_direct(request)
        api_calls += 1

        split_options, hub_api_calls = await self._price_all_hubs(request, direct_offer)
        api_calls += hub_api_calls

        winner, best_split, max_savings, max_savings_pct, winner_label = self._determine_winner(
            direct_offer, split_options
        )

        bucket = None
        if request.include_buckets:
            bucket_req = BucketRequest(
                origin      = request.origin,
                destination = request.destination,
                date        = request.date,
                cabin       = request.cabin,
                max_pax     = request.max_pax_for_buckets,
            )
            bucket     = await self.bucket_analysis(bucket_req)
            api_calls += bucket.api_calls_used

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        log.info(
            "Compare done: winner=%s savings=$%.0f (%d API calls, %dms)",
            winner.value, max_savings, api_calls, elapsed_ms,
        )

        return CompareResult(
            origin          = request.origin,
            destination     = request.destination,
            date            = request.date,
            cabin           = request.cabin,
            passengers      = request.passenger_count,
            direct          = direct_offer,
            direct_error    = direct_error,
            split_options   = split_options,
            winner          = winner,
            winner_price    = (
                direct_offer.total_amount if winner == CompareWinner.DIRECT and direct_offer
                else best_split.combined_price if best_split else None
            ),
            winner_label    = winner_label,
            best_split      = best_split,
            max_savings     = max_savings,
            max_savings_pct = max_savings_pct,
            bucket_analysis = bucket,
            total_api_calls = api_calls,
            search_duration_ms = elapsed_ms,
        )

    async def _price_direct(self, request: CompareRequest) -> tuple[Optional[OfferSummary], Optional[str]]:
        try:
            offer = await self._duffel.cheapest_one_way(
                origin      = request.origin,
                destination = request.destination,
                depart_date = request.date,
                passengers  = request.passenger_count,
                cabin       = request.cabin,
            )
            return offer, None if offer else (None, "No direct offers available")
        except DuffelError as e:
            log.warning("Direct price failed %s->%s: %s", request.origin, request.destination, e)
            return None, str(e)

    async def _price_all_hubs(self, request: CompareRequest, direct_offer: Optional[OfferSummary]) -> tuple[list[SplitTicketOption], int]:
        leg2_date = request.date + timedelta(days=self._settings.default_leg2_gap_days)
        legs: list[tuple[str, str, date]] = []
        for hub in request.hubs:
            legs.append((request.origin, hub, request.date))
            legs.append((hub, request.destination, leg2_date))

        results   = await self._duffel.search_legs_parallel(legs=legs, passengers=request.passenger_count, cabin=request.cabin)
        api_calls = len(legs)

        split_options: list[SplitTicketOption] = []
        for i, hub in enumerate(request.hubs):
            leg1_offer = results[i * 2]
            leg2_offer = results[i * 2 + 1]
            hub_info   = get_hub_info(hub)

            leg1 = LegPrice(
                origin      = request.origin,
                destination = hub,
                date        = request.date,
                offer       = leg1_offer,
                error       = None if leg1_offer else "No offers found",
            )
            leg2 = LegPrice(
                origin      = hub,
                destination = request.destination,
                date        = leg2_date,
                offer       = leg2_offer,
                error       = None if leg2_offer else "No offers found",
            )

            opt = SplitTicketOption(
                hub                       = hub,
                hub_name                  = hub_info["name"]    if hub_info else hub,
                hub_carrier_hint          = hub_info["carrier"] if hub_info else None,
                leg1                      = leg1,
                leg2                      = leg2,
                leg2_date                 = leg2_date,
                recommended_layover_hours = self._settings.default_layover_hours,
            )

            if opt.combined_price is not None and direct_offer is not None:
                savings     = direct_offer.total_amount - opt.combined_price
                savings_pct = (savings / direct_offer.total_amount * 100) if direct_offer.total_amount else 0
                opt.savings_vs_direct = round(savings, 2)
                opt.savings_pct       = round(savings_pct, 1)

            split_options.append(opt)

        return split_options, api_calls

    def _determine_winner(self, direct: Optional[OfferSummary], split_options: list[SplitTicketOption]) -> tuple[CompareWinner, Optional[SplitTicketOption], float, float, str]:
        threshold      = self._settings.min_savings_threshold
        priced_splits  = [s for s in split_options if s.combined_price is not None]

        if not direct and not priced_splits:
            return (CompareWinner.INSUFFICIENT_DATA, None, 0.0, 0.0, "Could not price any options")

        if not direct:
            best = min(priced_splits, key=lambda s: s.combined_price or 0)
            best.is_winner = True
            return (CompareWinner.SPLIT, best, 0.0, 0.0, f"Split via {best.hub} — ${best.combined_price:.0f} (no direct available)")

        if not priced_splits:
            return (CompareWinner.DIRECT, None, 0.0, 0.0, f"Direct — ${direct.total_amount:.0f} (no split options priced)")

        best_split  = min(priced_splits, key=lambda s: s.combined_price or float("inf"))
        savings     = direct.total_amount - (best_split.combined_price or 0)
        savings_pct = (savings / direct.total_amount * 100) if direct.total_amount else 0

        if savings >= threshold:
            best_split.is_winner = True
            label = (
                f"Split via {best_split.hub} — ${best_split.combined_price:.0f} combined, "
                f"saves ${savings:.0f} ({savings_pct:.0f}%) vs direct ${direct.total_amount:.0f}"
            )
            return (CompareWinner.SPLIT, best_split, round(savings, 2), round(savings_pct, 1), label)
        else:
            label = (
                f"Direct — ${direct.total_amount:.0f} "
                f"(best split ${best_split.combined_price:.0f} via {best_split.hub}, "
                f"only ${abs(savings):.0f} difference — not worth the extra logistics)"
            )
            return (CompareWinner.DIRECT, best_split, 0.0, 0.0, label)

    async def bucket_analysis(self, request: BucketRequest) -> BucketAnalysis:
        log.info("Bucket analysis: %s->%s on %s, 1..%d pax", request.origin, request.destination, request.date, request.max_pax)
        pax_results    = await self._duffel.search_pax_counts(request.origin, request.destination, request.date, request.max_pax, request.cabin)
        rows:   list[BucketRow]   = []
        breaks: list[BucketBreak] = []
        threshold_pct  = self._settings.bucket_break_threshold_pct

        for pax, offer in pax_results:
            if offer:
                rows.append(BucketRow(pax=pax, total_price=offer.total_amount, per_person=offer.per_passenger, carrier=offer.slices[0].carrier_label if offer.slices else None, available=True))
            else:
                rows.append(BucketRow(pax=pax, total_price=0.0, per_person=0.0, available=False))

        for i in range(1, len(rows)):
            prev = rows[i - 1]
            curr = rows[i]
            if not prev.available or not curr.available or prev.per_person <= 0:
                continue
            jump_amount = curr.per_person - prev.per_person
            jump_pct    = (jump_amount / prev.per_person) * 100
            if jump_pct >= threshold_pct and jump_amount >= self._settings.min_savings_threshold:
                savings_if_split = jump_amount * curr.pax
                rec = (
                    f"Price jumps ${jump_amount:.0f}/person ({prev.per_person:.0f}->{curr.per_person:.0f}) "
                    f"at {curr.pax} passengers. Consider booking {prev.pax} + {curr.pax - prev.pax} "
                    f"separately to save ~${savings_if_split:.0f} total."
                )
                breaks.append(BucketBreak(
                    at_pax       = curr.pax,
                    price_before = prev.per_person,
                    price_after  = curr.per_person,
                    jump_amount  = round(jump_amount, 2),
                    jump_percent = round(jump_pct, 1),
                    recommendation = rec,
                ))

        available      = [r for r in rows if r.available]
        cheapest_combo = min(available, key=lambda r: r.total_price) if available else None

        if not breaks:
            recommendation = "No significant fare bucket breaks detected. Safe to book all seats together."
        else:
            biggest = max(breaks, key=lambda b: b.jump_amount)
            recommendation = f"Fare bucket break at {biggest.at_pax} passengers (+${biggest.jump_amount:.0f}/person, {biggest.jump_percent:.0f}% jump). {biggest.recommendation}"

        log.info("Bucket analysis: %d rows, %d break(s)", len(rows), len(breaks))
        return BucketAnalysis(
            origin         = request.origin,
            destination    = request.destination,
            date           = request.date,
            cabin          = request.cabin,
            rows           = rows,
            breaks         = breaks,
            cheapest_combo = cheapest_combo,
            recommendation = recommendation,
            api_calls_used = request.max_pax,
        )
