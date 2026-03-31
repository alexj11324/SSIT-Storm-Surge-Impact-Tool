"""US state FIPS codes and abbreviations — single source of truth for NSI pipeline."""

from __future__ import annotations

import re
from dataclasses import dataclass

API_BASE = "https://nsi.sec.usace.army.mil/nsiapi/structures"


@dataclass(frozen=True)
class StateSpec:
    name: str
    abbr: str
    fips: str

    @property
    def path_name(self) -> str:
        return self.name.replace(" ", "_")

    @property
    def api_url(self) -> str:
        return f"{API_BASE}?fips={self.fips}&fmt=fs"


STATE_SPECS = [
    StateSpec("Alabama", "AL", "01"),
    StateSpec("Alaska", "AK", "02"),
    StateSpec("Arizona", "AZ", "04"),
    StateSpec("Arkansas", "AR", "05"),
    StateSpec("California", "CA", "06"),
    StateSpec("Colorado", "CO", "08"),
    StateSpec("Connecticut", "CT", "09"),
    StateSpec("Delaware", "DE", "10"),
    StateSpec("District Of Columbia", "DC", "11"),
    StateSpec("Florida", "FL", "12"),
    StateSpec("Georgia", "GA", "13"),
    StateSpec("Hawaii", "HI", "15"),
    StateSpec("Idaho", "ID", "16"),
    StateSpec("Illinois", "IL", "17"),
    StateSpec("Indiana", "IN", "18"),
    StateSpec("Iowa", "IA", "19"),
    StateSpec("Kansas", "KS", "20"),
    StateSpec("Kentucky", "KY", "21"),
    StateSpec("Louisiana", "LA", "22"),
    StateSpec("Maine", "ME", "23"),
    StateSpec("Maryland", "MD", "24"),
    StateSpec("Massachusetts", "MA", "25"),
    StateSpec("Michigan", "MI", "26"),
    StateSpec("Minnesota", "MN", "27"),
    StateSpec("Mississippi", "MS", "28"),
    StateSpec("Missouri", "MO", "29"),
    StateSpec("Montana", "MT", "30"),
    StateSpec("Nebraska", "NE", "31"),
    StateSpec("Nevada", "NV", "32"),
    StateSpec("New Hampshire", "NH", "33"),
    StateSpec("New Jersey", "NJ", "34"),
    StateSpec("New Mexico", "NM", "35"),
    StateSpec("New York", "NY", "36"),
    StateSpec("North Carolina", "NC", "37"),
    StateSpec("North Dakota", "ND", "38"),
    StateSpec("Ohio", "OH", "39"),
    StateSpec("Oklahoma", "OK", "40"),
    StateSpec("Oregon", "OR", "41"),
    StateSpec("Pennsylvania", "PA", "42"),
    StateSpec("Rhode Island", "RI", "44"),
    StateSpec("South Carolina", "SC", "45"),
    StateSpec("South Dakota", "SD", "46"),
    StateSpec("Tennessee", "TN", "47"),
    StateSpec("Texas", "TX", "48"),
    StateSpec("Utah", "UT", "49"),
    StateSpec("Vermont", "VT", "50"),
    StateSpec("Virginia", "VA", "51"),
    StateSpec("Washington", "WA", "53"),
    StateSpec("West Virginia", "WV", "54"),
    StateSpec("Wisconsin", "WI", "55"),
    StateSpec("Wyoming", "WY", "56"),
]

STATE_BY_FIPS = {s.fips: s for s in STATE_SPECS}
STATE_BY_ABBR = {s.abbr: s for s in STATE_SPECS}
STATE_BY_NAME = {re.sub(r"\s+", " ", s.name.replace("-", " ").strip()).lower(): s for s in STATE_SPECS}
# Title-case name -> FIPS (backward compat for NSIDownloader.STATE_FIPS)
STATE_FIPS = {s.name: s.fips for s in STATE_SPECS}
