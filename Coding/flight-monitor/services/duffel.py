from __future__ import annotations
import asyncio
import logging
import time
from datetime import date
from typing import Optional
import httpx
from app.config import attach_booking_urls, get_settings
from app.models import (
    CabinClass, OfferSummary, Passenger, SearchRequest,
    SegmentSummary, Slice, SliceResult,
)

log = logging.getLogger(__name__)


class DuffelError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, code: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.code        = code


class DuffelClient:
    def __init__(self, api_key: str, base_url: str, timeout: int = 30, version: str = "v2"):
        self._api_key  = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout  = timeout
        self._version  = version
        self._client: Optional[httpx.AsyncClient] = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(
            base_url = self._base_url,
            headers  = {
                "Authorization":   f"Bearer {self._api_key}",
                "Duffel-Version":  self._version,
                "Accept":          "application/json",
                "Content-Type":    "application/json",
                "Accept-Encoding": "gzip",
            },
            timeout = self._timeout,
        )
        log.info("Duffel client started (base_url=%s)", self._base_url)

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def ping(self) -> bool:
        try:
            resp = await self._get("/air/airlines", params={"limit": 1})
            return resp.get("data") is not None
        except DuffelError:
            return False

    async def _post(self, path: str, body: dict) -> dict:
        assert self._client, "Call client.start() first"
        try:
            resp = await self._client.post(path, json=body)
            self._raise_for_status(resp)
            return resp.json()
        except httpx.TimeoutException:
            raise DuffelError(f"Duffel request timed out: POST {path}")
        except httpx.RequestError as e:
            raise DuffelError(f"Duffel network error: {e}")

    async def _get(self, path: str, params: Optional[dict] = None) -> dict:
        assert self._client, "Call client.start() first"
        try:
            resp = await self._client.get(path, params=params)
            self._raise_for_status(resp)
            return resp.json()
        except httpx.TimeoutException:
            raise DuffelError(f"Duffel request timed out: GET {path}")
        except httpx.RequestError as e:
            raise DuffelError(f"Duffel network error: {e}")

    @staticmethod
    def _raise_for_status(resp: httpx.Response) -> None:
        if resp.status_code < 400:
            return
        try:
            body   = resp.json()
            errors = body.get("errors", [{}])
            msg    = errors[0].get("message", resp.text[:200])
            code   = errors[0].get("code", "unknown")
        except Exception:
            msg  = resp.text[:200]
            code = "parse_error"
        raise DuffelError(
            f"Duffel API error {resp.status_code}: {msg}",
            status_code = resp.status_code,
            code        = code,
        )

    async def create_offer_request(self, request: SearchRequest, max_connections: Optional[int] = None) -> list[OfferSummary]:
        body = self._build_offer_request_body(request, max_connections)
        log.info(
            "Duffel offer_request: %s pax=%d cabin=%s",
            " / ".join(f"{s.origin}->{s.destination}" for s in request.slices),
            request.passenger_count,
            request.cabin.value,
        )
        data       = await self._post("/air/offer_requests", {"data": body})
        raw_offers = data.get("data", {}).get("offers", [])
        if not raw_offers:
            log.info("  -> No offers returned")
            return []
        offers = [self._parse_offer(o, request) for o in raw_offers]
        offers = [o for o in offers if o is not None]
        log.info("  -> %d offers parsed", len(offers))
        return offers

    def _build_offer_request_body(self, request: SearchRequest, max_connections: Optional[int] = None) -> dict:
        slices     = [{"origin": s.origin, "destination": s.destination, "departure_date": s.date.isoformat()} for s in request.slices]
        passengers = [{"type": p.type} for p in request.passengers]
        body: dict = {
            "slices":        slices,
            "passengers":    passengers,
            "cabin_class":   request.cabin.to_duffel(),
            "return_offers": True,
        }
        conn = max_connections if max_connections is not None else request.max_connections
        if conn is not None:
            body["max_connections"] = conn
        return body

    def _parse_offer(self, raw: dict, request: SearchRequest) -> Optional[OfferSummary]:
        try:
            price     = raw["total_amount"]
            base      = raw.get("base_amount", price)
            tax       = raw.get("tax_amount", "0")
            pax_count = request.passenger_count
            per_pax   = float(price) / pax_count if pax_count else float(price)
            slices    = [self._parse_slice(s) for s in raw.get("slices", [])]
            offer     = OfferSummary(
                offer_id       = raw["id"],
                total_amount   = float(price),
                total_currency = raw.get("total_currency", "USD"),
                base_amount    = float(base),
                tax_amount     = float(tax),
                per_passenger  = round(per_pax, 2),
                passengers     = pax_count,
                cabin          = request.cabin,
                slices         = slices,
                expires_at     = raw.get("expires_at"),
            )
            return attach_booking_urls(offer, pax_count)
        except (KeyError, ValueError, TypeError) as e:
            log.warning("Failed to parse offer %s: %s", raw.get("id", "?"), e)
            return None

    @staticmethod
    def _parse_slice(raw_slice: dict) -> SliceResult:
        segments = []
        for seg in raw_slice.get("segments", []):
            operating = seg.get("operating_carrier", seg.get("marketing_carrier", {}))
            marketing = seg.get("marketing_carrier", operating)
            segments.append(SegmentSummary(
                origin        = seg["origin"]["iata_code"],
                destination   = seg["destination"]["iata_code"],
                departing_at  = seg["departing_at"],
                arriving_at   = seg["arriving_at"],
                carrier_code  = marketing.get("iata_code", "??"),
                carrier_name  = marketing.get("name", "Unknown"),
                flight_number = seg.get("marketing_carrier_flight_number", ""),
                duration      = seg.get("duration"),
            ))
        return SliceResult(
            origin      = raw_slice["origin"]["iata_code"],
            destination = raw_slice["destination"]["iata_code"],
            duration    = raw_slice.get("duration"),
            segments    = segments,
        )

    async def search_one_way(self, origin: str, destination: str, depart_date: date, passengers: int = 1, cabin: CabinClass = CabinClass.ECONOMY, max_offers: int = 5) -> list[OfferSummary]:
        request = SearchRequest(
            slices     = [Slice(origin=origin, destination=destination, date=depart_date)],
            passengers = [Passenger() for _ in range(passengers)],
            cabin      = cabin,
        )
        offers = await self.create_offer_request(request)
        return offers[:max_offers]

    async def cheapest_one_way(self, origin: str, destination: str, depart_date: date, passengers: int = 1, cabin: CabinClass = CabinClass.ECONOMY) -> Optional[OfferSummary]:
        offers = await self.search_one_way(origin, destination, depart_date, passengers, cabin, max_offers=1)
        return offers[0] if offers else None

    async def search_legs_parallel(self, legs: list[tuple[str, str, date]], passengers: int = 1, cabin: CabinClass = CabinClass.ECONOMY) -> list[Optional[OfferSummary]]:
        semaphore = asyncio.Semaphore(4)

        async def _search(origin: str, dest: str, d: date) -> Optional[OfferSummary]:
            async with semaphore:
                try:
                    return await self.cheapest_one_way(origin, dest, d, passengers, cabin)
                except DuffelError as e:
                    log.warning("Leg search failed %s->%s on %s: %s", origin, dest, d, e)
                    return None

        tasks = [_search(o, d_dest, d_date) for (o, d_dest, d_date) in legs]
        return list(await asyncio.gather(*tasks))

    async def search_pax_counts(self, origin: str, destination: str, depart_date: date, max_pax: int = 4, cabin: CabinClass = CabinClass.ECONOMY) -> list[tuple[int, Optional[OfferSummary]]]:
        semaphore = asyncio.Semaphore(3)

        async def _search(n: int) -> tuple[int, Optional[OfferSummary]]:
            async with semaphore:
                try:
                    offer = await self.cheapest_one_way(origin, destination, depart_date, n, cabin)
                    return (n, offer)
                except DuffelError as e:
                    log.warning("Pax=%d search failed %s->%s: %s", n, origin, destination, e)
                    return (n, None)

        tasks   = [_search(n) for n in range(1, max_pax + 1)]
        results = list(await asyncio.gather(*tasks))
        return sorted(results, key=lambda x: x[0])


_client_instance: Optional[DuffelClient] = None


def get_duffel_client() -> DuffelClient:
    global _client_instance
    if _client_instance is None:
        s = get_settings()
        _client_instance = DuffelClient(
            api_key  = s.duffel_api_key,
            base_url = s.duffel_base_url,
            timeout  = s.duffel_timeout,
            version  = s.duffel_version,
        )
    return _client_instance
