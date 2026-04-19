from __future__ import annotations
from functools import lru_cache
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    duffel_api_key:  str  = ""
    duffel_base_url: str  = "https://api.duffel.com"
    duffel_timeout:  int  = 30
    duffel_version:  str  = "v2"

    app_env:        str  = "development"
    log_level:      str  = "INFO"
    cors_origins:   str  = "*"

    min_savings_threshold:      float = 15.0
    bucket_break_threshold_pct: float = 8.0
    default_leg2_gap_days:      int   = 1
    default_layover_hours:      int   = 3

    @property
    def duffel_configured(self) -> bool:
        return bool(self.duffel_api_key)

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


HUB_REGIONS: dict[str, dict] = {
    "ATLANTIC": {
        "desc": "Europe / Transatlantic",
        "dest_flags": [
            "🇬🇧","🇫🇷","🇩🇪","🇪🇸","🇮🇹","🇳🇱","🇵🇹","🇨🇭","🇦🇹","🇧🇪",
            "🇩🇰","🇸🇪","🇳🇴","🇫🇮","🇮🇪","🇬🇷","🇨🇿","🇭🇺","🇵🇱",
        ],
        "hubs": [
            {"code":"JFK","name":"New York JFK",       "carrier":"Multiple",        "note":"Most transatlantic options"},
            {"code":"EWR","name":"Newark",             "carrier":"United",          "note":"United hub, strong to EU"},
            {"code":"ORD","name":"Chicago O'Hare",     "carrier":"American/United", "note":"Strong to London, Frankfurt"},
            {"code":"ATL","name":"Atlanta",            "carrier":"Delta",           "note":"Delta international hub"},
            {"code":"MIA","name":"Miami",              "carrier":"American",        "note":"AA hub, strong to Iberia"},
            {"code":"BOS","name":"Boston",             "carrier":"Multiple",        "note":"Strong to UK/Ireland"},
            {"code":"IAD","name":"Washington Dulles",  "carrier":"United",          "note":"United hub"},
            {"code":"PHL","name":"Philadelphia",       "carrier":"American",        "note":"AA hub"},
            {"code":"DFW","name":"Dallas Fort Worth",  "carrier":"American",        "note":"AA hub"},
            {"code":"LAX","name":"Los Angeles",        "carrier":"Multiple",        "note":"Many EU codeshares"},
        ],
    },
    "ASIA": {
        "desc": "Asia Pacific",
        "dest_flags": [
            "🇯🇵","🇰🇷","🇨🇳","🇹🇼","🇭🇰","🇸🇬","🇲🇾","🇹🇭","🇮🇩","🇵🇭","🇻🇳","🇦🇺","🇳🇿","🇮🇳",
        ],
        "hubs": [
            {"code":"LAX","name":"Los Angeles",        "carrier":"Multiple",        "note":"Best for Asia"},
            {"code":"SFO","name":"San Francisco",      "carrier":"United",          "note":"United transpacific hub"},
            {"code":"SEA","name":"Seattle",            "carrier":"Alaska/Delta",    "note":"Good for Japan/Korea"},
            {"code":"ORD","name":"Chicago O'Hare",     "carrier":"United",          "note":"United transpacific"},
            {"code":"JFK","name":"New York JFK",       "carrier":"Multiple",        "note":"Codeshares to Asia"},
            {"code":"IAD","name":"Washington Dulles",  "carrier":"United",          "note":"United hub"},
        ],
    },
    "MIDEAST_AFRICA": {
        "desc": "Middle East and Africa",
        "dest_flags": [
            "🇦🇪","🇶🇦","🇸🇦","🇰🇼","🇧🇭","🇪🇬","🇮🇱","🇯🇴","🇱🇧","🇹🇷",
            "🇿🇦","🇳🇬","🇰🇪","🇪🇹","🇬🇭","🇲🇦","🇹🇳",
        ],
        "hubs": [
            {"code":"JFK","name":"New York JFK",       "carrier":"Multiple",        "note":"Most ME/Africa connections"},
            {"code":"IAD","name":"Washington Dulles",  "carrier":"United",          "note":"United hub"},
            {"code":"ORD","name":"Chicago O'Hare",     "carrier":"Multiple",        "note":"Via EU carriers"},
            {"code":"ATL","name":"Atlanta",            "carrier":"Delta",           "note":"Delta international hub"},
            {"code":"MIA","name":"Miami",              "carrier":"American",        "note":"Good for Africa via Europe"},
            {"code":"EWR","name":"Newark",             "carrier":"United",          "note":"United hub"},
        ],
    },
    "LATAM": {
        "desc": "Latin America",
        "dest_flags": ["🇲🇽","🇧🇷","🇦🇷","🇨🇴","🇨🇱","🇵🇪","🇵🇷","🇨🇺","🇩🇴"],
        "hubs": [
            {"code":"MIA","name":"Miami",              "carrier":"American",        "note":"Best hub for LatAm"},
            {"code":"DFW","name":"Dallas Fort Worth",  "carrier":"American",        "note":"AA hub"},
            {"code":"IAH","name":"Houston",            "carrier":"United",          "note":"United LatAm hub"},
            {"code":"ATL","name":"Atlanta",            "carrier":"Delta",           "note":"Delta LatAm routes"},
            {"code":"JFK","name":"New York JFK",       "carrier":"Multiple",        "note":""},
            {"code":"LAX","name":"Los Angeles",        "carrier":"Multiple",        "note":"Mexico/Central America"},
        ],
    },
    "DOMESTIC": {
        "desc": "US Domestic",
        "dest_flags": [],
        "hubs": [
            {"code":"ORD","name":"Chicago O'Hare",     "carrier":"American/United", "note":""},
            {"code":"ATL","name":"Atlanta",            "carrier":"Delta",           "note":"Delta main hub"},
            {"code":"DFW","name":"Dallas Fort Worth",  "carrier":"American",        "note":"AA main hub"},
            {"code":"DEN","name":"Denver",             "carrier":"United",          "note":"United mountain hub"},
            {"code":"LAX","name":"Los Angeles",        "carrier":"Multiple",        "note":""},
            {"code":"MSP","name":"Minneapolis",        "carrier":"Delta",           "note":""},
            {"code":"IAH","name":"Houston",            "carrier":"United",          "note":""},
            {"code":"SLC","name":"Salt Lake City",     "carrier":"Delta",           "note":""},
            {"code":"SEA","name":"Seattle",            "carrier":"Alaska",          "note":""},
            {"code":"PHX","name":"Phoenix",            "carrier":"American",        "note":""},
        ],
    },
}


def suggest_hubs(origin: str, destination: str, dest_flag: Optional[str] = None, max_hubs: int = 8) -> list[dict]:
    region_key = "DOMESTIC"
    if dest_flag:
        for key, data in HUB_REGIONS.items():
            if key == "DOMESTIC":
                continue
            if dest_flag in data["dest_flags"]:
                region_key = key
                break
    hubs = HUB_REGIONS[region_key]["hubs"]
    filtered = [h for h in hubs if h["code"] not in (origin, destination)]
    return filtered[:max_hubs]


def get_hub_info(code: str) -> Optional[dict]:
    for region in HUB_REGIONS.values():
        for hub in region["hubs"]:
            if hub["code"] == code:
                return hub
    return None


def google_flights_url(origin: str, destination: str, depart_date: str, passengers: int = 1, cabin: str = "economy", return_date: Optional[str] = None) -> str:
    from app.models import CabinClass
    from urllib.parse import urlencode
    try:
        cab_text = CabinClass(cabin).to_gf_text()
    except ValueError:
        cab_text = ""
    trip = "roundtrip" if return_date else "one way"
    q = f"{cab_text}{trip} flights from {origin} to {destination} on {depart_date}"
    if return_date:
        q += f" returning {return_date}"
    return f"https://www.google.com/travel/flights/search?{urlencode({'q': q, 'hl': 'en', 'curr': 'USD'})}"


def kayak_url(origin: str, destination: str, depart_date: str, passengers: int = 1, cabin: str = "economy", return_date: Optional[str] = None) -> str:
    from app.models import CabinClass
    try:
        cab = CabinClass(cabin).to_kayak()
    except ValueError:
        cab = "e"
    d = depart_date or "anytime"
    if return_date:
        return f"https://www.kayak.com/flights/{origin}-{destination}/{d}/{return_date}/{passengers}adults?cabin={cab}&sort=price_a"
    return f"https://www.kayak.com/flights/{origin}-{destination}/{d}/{passengers}adults?cabin={cab}&sort=price_a"


def skyscanner_url(origin: str, destination: str, depart_date: str, passengers: int = 1, cabin: str = "economy") -> str:
    from app.models import CabinClass
    try:
        cab = CabinClass(cabin).to_skyscanner()
    except ValueError:
        cab = "economy"
    d = depart_date.replace("-", "") if depart_date else "anytime"
    return f"https://www.skyscanner.com/transport/flights/{origin.lower()}/{destination.lower()}/{d}/?adults={passengers}&cabinclass={cab}"


def attach_booking_urls(offer, passengers: int = 1):
    if not offer.slices:
        return offer
    origin      = offer.slices[0].origin
    destination = offer.slices[-1].destination
    depart_date = offer.slices[0].segments[0].departing_at[:10] if offer.slices[0].segments else ""
    cabin       = offer.cabin.value
    offer.google_flights_url = google_flights_url(origin, destination, depart_date, passengers, cabin)
    offer.kayak_url          = kayak_url(origin, destination, depart_date, passengers, cabin)
    offer.skyscanner_url     = skyscanner_url(origin, destination, depart_date, passengers, cabin)
    return offer
