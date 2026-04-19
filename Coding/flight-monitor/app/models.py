from __future__ import annotations
from datetime import date, datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, model_validator


class CabinClass(str, Enum):
    ECONOMY         = "economy"
    PREMIUM_ECONOMY = "premium_economy"
    BUSINESS        = "business"
    FIRST           = "first"

    def to_duffel(self) -> str:
        return self.value

    def to_kayak(self) -> str:
        return {"economy":"e","premium_economy":"p","business":"b","first":"f"}[self.value]

    def to_skyscanner(self) -> str:
        return {"economy":"economy","premium_economy":"premiumeconomy","business":"business","first":"first"}[self.value]

    def to_gf_text(self) -> str:
        return {"economy":"","premium_economy":"premium economy ","business":"business class ","first":"first class "}[self.value]


class TripType(str, Enum):
    ONE_WAY    = "one_way"
    ROUND_TRIP = "round_trip"
    MULTI_CITY = "multi_city"


class CompareWinner(str, Enum):
    DIRECT            = "direct"
    SPLIT             = "split"
    INSUFFICIENT_DATA = "insufficient_data"


class Slice(BaseModel):
    origin:      str  = Field(..., min_length=3, max_length=3)
    destination: str  = Field(..., min_length=3, max_length=3)
    date:        date

    @model_validator(mode="after")
    def uppercase_codes(self) -> "Slice":
        self.origin      = self.origin.upper()
        self.destination = self.destination.upper()
        return self


class Passenger(BaseModel):
    type: str = Field(default="adult", pattern="^(adult|child|infant_without_seat)$")


class SearchRequest(BaseModel):
    slices:          list[Slice]     = Field(..., min_length=1, max_length=8)
    passengers:      list[Passenger] = Field(default_factory=lambda: [Passenger()])
    cabin:           CabinClass      = Field(default=CabinClass.ECONOMY)
    max_connections: Optional[int]   = Field(default=None, ge=0, le=2)

    @property
    def passenger_count(self) -> int:
        return len(self.passengers)

    @property
    def trip_type(self) -> TripType:
        if len(self.slices) == 1:
            return TripType.ONE_WAY
        if len(self.slices) == 2 and self.slices[0].origin == self.slices[1].destination:
            return TripType.ROUND_TRIP
        return TripType.MULTI_CITY


class CompareRequest(BaseModel):
    origin:       str              = Field(..., min_length=3, max_length=3)
    destination:  str              = Field(..., min_length=3, max_length=3)
    date:         date
    return_date:  Optional[date]   = None
    hubs:         list[str]        = Field(..., min_length=1, max_length=12)
    passengers:   list[Passenger]  = Field(default_factory=lambda: [Passenger()])
    cabin:        CabinClass       = Field(default=CabinClass.ECONOMY)
    include_buckets: bool          = Field(default=False)
    max_pax_for_buckets: int       = Field(default=4, ge=2, le=9)

    @model_validator(mode="after")
    def uppercase_all(self) -> "CompareRequest":
        self.origin      = self.origin.upper()
        self.destination = self.destination.upper()
        self.hubs        = [h.strip().upper() for h in self.hubs]
        return self

    @property
    def passenger_count(self) -> int:
        return len(self.passengers)


class BucketRequest(BaseModel):
    origin:      str        = Field(..., min_length=3, max_length=3)
    destination: str        = Field(..., min_length=3, max_length=3)
    date:        date
    cabin:       CabinClass = Field(default=CabinClass.ECONOMY)
    max_pax:     int        = Field(default=4, ge=2, le=9)

    @model_validator(mode="after")
    def uppercase_codes(self) -> "BucketRequest":
        self.origin      = self.origin.upper()
        self.destination = self.destination.upper()
        return self


class SegmentSummary(BaseModel):
    origin:        str
    destination:   str
    departing_at:  str
    arriving_at:   str
    carrier_code:  str
    carrier_name:  str
    flight_number: str
    duration:      Optional[str] = None


class SliceResult(BaseModel):
    origin:      str
    destination: str
    duration:    Optional[str]        = None
    segments:    list[SegmentSummary]

    @property
    def stops(self) -> int:
        return max(0, len(self.segments) - 1)

    @property
    def carriers(self) -> list[str]:
        seen: list[str] = []
        for s in self.segments:
            if s.carrier_name not in seen:
                seen.append(s.carrier_name)
        return seen

    @property
    def carrier_label(self) -> str:
        return " / ".join(self.carriers)


class OfferSummary(BaseModel):
    offer_id:       str
    total_amount:   float
    total_currency: str          = "USD"
    base_amount:    float
    tax_amount:     float
    per_passenger:  float
    passengers:     int
    cabin:          CabinClass
    slices:         list[SliceResult]
    expires_at:     Optional[str] = None
    duffel_checkout_url: Optional[str] = None
    google_flights_url:  Optional[str] = None
    kayak_url:           Optional[str] = None
    skyscanner_url:      Optional[str] = None

    @property
    def route_label(self) -> str:
        if not self.slices:
            return "Unknown"
        return f"{self.slices[0].origin} -> {self.slices[-1].destination}"

    @property
    def stop_summary(self) -> str:
        if not self.slices:
            return ""
        stops = self.slices[0].stops
        return "Nonstop" if stops == 0 else f"{stops} stop{'s' if stops > 1 else ''}"


class BucketRow(BaseModel):
    pax:         int
    total_price: float
    per_person:  float
    carrier:     Optional[str] = None
    available:   bool          = True


class BucketBreak(BaseModel):
    at_pax:         int
    price_before:   float
    price_after:    float
    jump_amount:    float
    jump_percent:   float
    recommendation: str


class BucketAnalysis(BaseModel):
    origin:         str
    destination:    str
    date:           date
    cabin:          CabinClass
    rows:           list[BucketRow]
    breaks:         list[BucketBreak]
    cheapest_combo: Optional[BucketRow] = None
    recommendation: str                 = ""
    api_calls_used: int                 = 0


class LegPrice(BaseModel):
    origin:      str
    destination: str
    date:        date
    offer:       Optional[OfferSummary] = None
    error:       Optional[str]          = None

    @property
    def price(self) -> Optional[float]:
        return self.offer.total_amount if self.offer else None

    @property
    def available(self) -> bool:
        return self.offer is not None


class SplitTicketOption(BaseModel):
    hub:               str
    hub_name:          Optional[str] = None
    hub_carrier_hint:  Optional[str] = None
    leg1:              LegPrice
    leg2:              LegPrice
    combined_price:     Optional[float] = None
    savings_vs_direct:  Optional[float] = None
    savings_pct:        Optional[float] = None
    is_winner:          bool            = False
    leg2_date:                  Optional[date] = None
    recommended_layover_hours:  int            = 3
    fully_available:     bool           = True
    unavailability_note: Optional[str]  = None

    @model_validator(mode="after")
    def compute_combined(self) -> "SplitTicketOption":
        if self.leg1.price is not None and self.leg2.price is not None:
            self.combined_price = round(self.leg1.price + self.leg2.price, 2)
        elif self.leg1.price is None or self.leg2.price is None:
            self.fully_available = False
            missing = []
            if not self.leg1.available:
                missing.append(f"{self.leg1.origin}->{self.leg1.destination}")
            if not self.leg2.available:
                missing.append(f"{self.leg2.origin}->{self.leg2.destination}")
            self.unavailability_note = f"No offers: {', '.join(missing)}"
        return self


class CompareResult(BaseModel):
    origin:      str
    destination: str
    date:        date
    cabin:       CabinClass
    passengers:  int
    direct:       Optional[OfferSummary] = None
    direct_error: Optional[str]          = None
    split_options: list[SplitTicketOption] = Field(default_factory=list)
    winner:       CompareWinner
    winner_price: Optional[float] = None
    winner_label: str             = ""
    best_split:       Optional[SplitTicketOption] = None
    max_savings:      float                       = 0.0
    max_savings_pct:  float                       = 0.0
    bucket_analysis: Optional[BucketAnalysis] = None
    searched_at:     str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    total_api_calls: int = 0
    search_duration_ms: Optional[int] = None

    def splits_by_savings(self) -> list[SplitTicketOption]:
        available   = [s for s in self.split_options if s.combined_price is not None]
        unavailable = [s for s in self.split_options if s.combined_price is None]
        return sorted(available, key=lambda s: s.combined_price or 0) + unavailable

    def to_summary_dict(self) -> dict:
        return {
            "route":            f"{self.origin}->{self.destination}",
            "date":             str(self.date),
            "cabin":            self.cabin.value,
            "passengers":       self.passengers,
            "direct_price":     self.direct.total_amount if self.direct else None,
            "best_split_hub":   self.best_split.hub if self.best_split else None,
            "best_split_price": self.best_split.combined_price if self.best_split else None,
            "winner":           self.winner.value,
            "max_savings":      self.max_savings,
            "max_savings_pct":  self.max_savings_pct,
            "searched_at":      self.searched_at,
        }


class SearchResponse(BaseModel):
    offers:        list[OfferSummary]
    total_results: int
    searched_at:   str
    cabin:         CabinClass
    trip_type:     TripType


class HealthResponse(BaseModel):
    status:  str = "ok"
    duffel:  str = "unknown"
    version: str = "0.1.0"


class ErrorResponse(BaseModel):
    error:   str
    detail:  Optional[str] = None
    code:    Optional[str] = None
