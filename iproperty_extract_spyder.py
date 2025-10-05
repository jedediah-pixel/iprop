# -*- coding: utf-8 -*-
"""
iProperty extractor — Spyder-friendly (v2a)
- If ROOT is blank or not found, prompts you to select a folder (GUI if available; else console input).
- Traverses a ROOT directory (plain .html, .htm, .zip containing html, and gzipped html)
- Extracts listing fields with resilient fallbacks
- Writes a CSV named 'iproperty_extract.csv' inside the selected ROOT

How to run in Spyder:
1) Open this file.
2) (Optional) Put your folder path in ROOT below; or just Run and choose a folder when prompted.
3) Press Run ▶. When finished, see the CSV path printed at the end.
"""

import os, re, json, csv, zipfile, gzip, sys

# Try BeautifulSoup; if missing, print a clear hint.
try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing dependency: bs4. Install with:  pip install beautifulsoup4")
    raise

# ------------------- CONFIG -------------------
# Leave blank to be prompted at runtime
ROOT = r""

OUT_BASENAME = "iproperty_extract.csv"

# ------------------- RUNTIME FOLDER PICKER -------------------
def pick_root_if_needed(root):
    if root and os.path.isdir(root):
        return root

    # Try Tkinter folder picker first
    try:
        import tkinter as tk
        from tkinter import filedialog
        tk.Tk().withdraw()
        folder = filedialog.askdirectory(title="Select the adview folder (contains .html/.zip files)")
        if folder and os.path.isdir(folder):
            return folder
    except Exception:
        pass

    # Fallback to console input
    while True:
        try:
            folder = input("Enter folder path to scan (or leave blank to quit): ").strip('"').strip()
        except EOFError:
            folder = ""
        if not folder:
            print("No folder selected. Exiting.")
            sys.exit(0)
        if os.path.isdir(folder):
            return folder
        print("Path not found. Try again.\n")

# ------------------- HELPERS -------------------
def _num(s):
    if s is None:
        return None
    m = re.search(r"(-?\d+(?:\.\d+)?)", str(s).replace(',', ''))
    return float(m.group(1)) if m else None

def _digits_only(s):
    return re.sub(r"\D+", "", s or "")

def _is_blank(x):
    return x is None or (str(x).strip() in {"", "-", "N/A", "n/a", "None"})

def _is_sqft(u):
    u = (u or "").lower()
    return ("ft" in u) or ("sqft" in u) or ("sf" in u)

def _is_sqm(u):
    u = (u or "").lower()
    return ("sqm" in u) or ("m²" in u) or ("sq.m" in u) or ("square meter" in u)

def _area_to_sqft(value, unit_txt):
    if value is None:
        return None
    if _is_sqm(unit_txt):
        return value * 10.7639
    return value

LAND_UNIT_TOKENS = {
    "sq ft": {"sq ft", "sqft", "square feet", "sq. ft", "sf", "ft²", "ft2", "ft"},
    "sqm": {"sq m", "sqm", "square metre", "square meter", "sq.m", "m²", "m2"},
    "acre": {"acre", "ac", "acres"},
    "hectare": {"hectare", "ha", "hectares"},
}

LAND_UNIT_FACTORS = {
    "sq ft": 1.0,
    "sqm": 10.7639,
    "acre": 43560.0,
    "hectare": 107639.0,
}


def _canonical_land_unit(unit_txt):
    if not unit_txt:
        return ""
    u = str(unit_txt).strip().lower()
    if not u:
        return ""
    u = u.replace(".", " ").replace(",", " ")
    u = re.sub(r"\s+", " ", u)
    for canon, tokens in LAND_UNIT_TOKENS.items():
        if any(tok in u for tok in tokens):
            return canon
    return ""


def _land_to_sqft(value, unit):
    if value is None or unit not in LAND_UNIT_FACTORS:
        return None
    return value * LAND_UNIT_FACTORS[unit]


def _format_number(val):
    if val is None:
        return ""
    if abs(val - round(val)) < 1e-6:
        return f"{int(round(val)):,}"
    return f"{val:,.2f}".rstrip("0").rstrip(".")

def jget(obj, path):
    cur = obj
    try:
        for k in path:
            if isinstance(cur, list) and isinstance(k, int):
                cur = cur[k]
            elif isinstance(cur, dict):
                cur = cur.get(k)
            else:
                return None
        return cur
    except Exception:
        return None

def _iter_script_jsons(soup):
    for sc in soup.find_all("script"):
        t = (sc.get("type") or "").lower()
        if sc.get("id") == "__NEXT_DATA__" or t in ("application/json", "application/ld+json"):
            txt = (sc.string or sc.text or "").strip()
            if not txt:
                continue
            try:
                data = json.loads(txt)
            except Exception:
                continue
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, (dict, list)):
                        yield item
            elif isinstance(data, (dict, list)):
                yield data

def _collect_all_json(soup):
    out = []
    for obj in _iter_script_jsons(soup):
        out.append(obj)
        if isinstance(obj, dict):
            maybe = jget(obj, ["props", "pageProps"])
            if isinstance(maybe, dict):
                out.append(maybe)
                pd = maybe.get("pageData") or {}
                if isinstance(pd, dict):
                    out.append(pd)
                    dd = pd.get("data") or {}
                    if isinstance(dd, dict):
                        out.append(dd)
    return out

def _first_non_empty(*candidates):
    for c in candidates:
        if isinstance(c, (list, dict)):
            if c:
                return c
        else:
            if not _is_blank(c):
                return c
    return None

# ------------------- FIELD EXTRACTORS -------------------
REN_PAT = re.compile(r"\bREN[:\-]?\s*(\d{3,7})\b", re.I)

def extract_url(html, soup):
    m = re.search(r'"shareLink"\s*:\s*"([^"]+)"', html, re.I)
    if m:
        return m.group(1).strip()
    link = soup.find("link", rel=lambda v: v and "canonical" in v.lower())
    if link and link.get("href"):
        return link["href"].strip()
    og = soup.find("meta", property="og:url")
    if og and og.get("content"):
        return og["content"].strip()
    tw = soup.find("meta", attrs={"name": "twitter:url"})
    if tw and tw.get("content"):
        return tw.get("content").strip()
    return ""

def extract_ld_objects(soup, at_type=None):
    for obj in _iter_script_jsons(soup):
        if isinstance(obj, dict) and obj.get("@type"):
            if (at_type is None) or (obj.get("@type") == at_type):
                yield obj
        elif isinstance(obj, list):
            for o in obj:
                if isinstance(o, dict) and o.get("@type"):
                    if (at_type is None) or (o.get("@type") == at_type):
                        yield o

def extract_price(html, soup):
    m = re.search(
        r'"price"\s*:\s*{[^{}]*"currency"\s*:\s*"([A-Z]+)"[^{}]*?(?:"min"\s*:\s*([0-9,\.]+))?[^{}]*?(?:"max"\s*:\s*([0-9,\.]+))?',
        html, re.S | re.I
    )
    if m:
        p = _num(m.group(2)) if m.group(2) else _num(m.group(3))
        return "MYR", p
    for o in extract_ld_objects(soup, "RealEstateListing"):
        offers = o.get("offers") or {}
        if "price" in offers:
            return offers.get("priceCurrency") or "MYR", _num(offers.get("price"))
    for node in soup.find_all(string=True):
        if node.parent and node.parent.name in ("script", "style"):
            continue
        t = (node or "").strip()
        if not t or re.search(r"\bpsf|\bpsm|\bper\s+sq", t, re.I):
            continue
        mm = re.search(r"\bRM\s*([0-9][0-9,\.]*)\b", t, re.I)
        if mm:
            return "MYR", _num(mm.group(1))
    return "", None

def is_rent_page(soup):
    for item in soup.select(".meta-table__item"):
        if re.search(r"\bfor\s+rent\b", item.get_text(" ", strip=True), re.I):
            return True
    ttl = soup.title.get_text() if soup.title else ""
    if re.search(r"\bfor\s+rent\b", ttl, re.I):
        return True
    og = soup.find("meta", property="og:title")
    if og and re.search(r"\bfor\s+rent\b", og.get("content", ""), re.I):
        return True
    return False

def _extract_state_metatable_blocks(html):
    # STRICT: only scan metaTable/metatable items[]
    for mm in re.finditer(
        r'(?:"metatable"|"metaTable")\s*:\s*{[^{}]*"items"\s*:\s*\[(.*?)\]\s*}',
        html, re.S | re.I
    ):
        yield mm.group(1)


def extract_land_size_psf(html, soup, property_type, price, is_rent, built_up_value, built_up_unit):
    LAND_LABEL_RE = re.compile(
        r"\b(land\s*area|land\s*size|lot\s*size|site\s*area|keluasan\s*tanah|luas\s*tanah)\b",
        re.I,
    )
    FORBIDDEN_LABEL_RE = re.compile(r"built\s*-?up|floor\s*area", re.I)
    SIZE_WORD_RE = re.compile(r"\bsize\b", re.I)
    DIM_RE = re.compile(
        r"(\d+(?:[\.,]\d+)?)\s*[x×]\s*(\d+(?:[\.,]\d+)?)(?:\s*[x×]\s*(\d+(?:[\.,]\d+)?))?\s*([A-Za-z²°\s]*)",
        re.I,
    )
    AREA_RE = re.compile(
        r"([0-9][0-9,\.]*)\s*(sq\.?\s*ft|sqft|sf|square\s*feet|sq\.?\s*m|sqm|square\s*met(?:er|re)|m²|m2|acre(?:s)?|ac\b|hectare(?:s)?|ha\b)",
        re.I,
    )
    PSF_RE = re.compile(r"(?:RM\s*)?([0-9][0-9,\.]*)\s*psf", re.I)

    raw_hits = []
    seen_raw = set()
    land_candidates = []
    psf_candidates = []
    attr_land_field_found = False
    explicit_label_found = False

    def add_raw(text):
        if not text:
            return
        cleaned = re.sub(r"\s+", " ", str(text)).strip()
        if not cleaned:
            return
        key = cleaned.lower()
        if key in seen_raw:
            return
        seen_raw.add(key)
        raw_hits.append(cleaned)

    def add_land_candidate(value, unit, raw_text, source, priority, explicit):
        nonlocal attr_land_field_found, explicit_label_found
        if value is None:
            return
        if isinstance(value, (int, float)):
            num = float(value)
        else:
            num = _num(value)
        if num is None or num <= 0:
            return
        canon_unit = _canonical_land_unit(unit)
        if not canon_unit:
            return
        sqft = _land_to_sqft(num, canon_unit)
        if sqft is None or sqft < 200 or sqft > 10000000:
            return
        display = f"{_format_number(num)} {canon_unit}"
        add_raw(raw_text or display)
        land_candidates.append(
            {
                "priority": priority,
                "explicit": bool(explicit),
                "sqft": sqft,
                "value": num,
                "unit": canon_unit,
                "display": display,
                "source": source,
            }
        )
        if source == "attr.landArea":
            attr_land_field_found = True
        if explicit:
            explicit_label_found = True

    def add_psf_candidate(value, raw_text, source, priority):
        nonlocal attr_land_field_found
        if value is None:
            return
        if isinstance(value, (int, float)):
            num = float(value)
        else:
            num = _num(value)
        if num is None or num <= 0:
            return
        if num < 1 or num > 10000:
            return
        add_raw(raw_text or f"{num} psf")
        psf_candidates.append(
            {
                "priority": priority,
                "value": num,
                "source": source,
            }
        )
        if source.startswith("attr.landPSF"):
            attr_land_field_found = True

    def parse_land_text(label_text, value_text, source, priority, explicit=False):
        label = re.sub(r"\s+", " ", str(label_text or "")).strip()
        value = re.sub(r"\s+", " ", str(value_text or "")).strip()
        if not value:
            return
        label_lower = label.lower()
        value_lower = value.lower()
        if label:
            if not LAND_LABEL_RE.search(label_lower) and "land" not in label_lower:
                return
        else:
            if not LAND_LABEL_RE.search(value_lower):
                return
        if FORBIDDEN_LABEL_RE.search(label_lower) or FORBIDDEN_LABEL_RE.search(value_lower):
            return
        if SIZE_WORD_RE.search(label_lower) and "land" not in label_lower:
            return
        raw = value if not label else f"{label}: {value}"
        explicit_flag = explicit or bool(label and LAND_LABEL_RE.search(label_lower))
        parsed = False
        for dim in DIM_RE.finditer(value):
            w = _num(dim.group(1))
            l = _num(dim.group(2))
            h = _num(dim.group(3)) if dim.group(3) else None
            unit_hint = dim.group(4) or label
            unit = unit_hint or value
            if w and l:
                area = w * l
                if h and "sqm" in (unit_hint or "").lower():
                    area *= h
                add_land_candidate(area, unit, raw, source, priority, explicit_flag)
                parsed = True
        for match in AREA_RE.finditer(value):
            val = _num(match.group(1))
            unit = match.group(2)
            add_land_candidate(val, unit, raw, source, priority, explicit_flag)
            parsed = True
        if not parsed:
            num = _num(value)
            unit_hint = label if label else value
            if num and unit_hint:
                add_land_candidate(num, unit_hint, raw, source, priority, explicit_flag)

    def parse_land_psf(label_text, value_text, source, priority):
        label = re.sub(r"\s+", " ", str(label_text or "")).strip()
        value = re.sub(r"\s+", " ", str(value_text or "")).strip()
        if not value or "psf" not in value.lower():
            return
        combined = f"{label} {value}".lower()
        if "land" not in combined:
            return
        if "floor" in combined and "land" not in label.lower():
            return
        raw = value if not label else f"{label}: {value}"
        for match in PSF_RE.finditer(value):
            add_psf_candidate(match.group(1), raw, source, priority)

    def process_attribute_container(container):
        if not isinstance(container, dict):
            return
        la = container.get("landArea")
        unit = (
            container.get("sizeUnitLandArea")
            or container.get("landAreaUnit")
            or container.get("landAreaUnitType")
        )
        if la not in (None, "") and unit:
            add_land_candidate(la, unit, f"attributes.landArea={la} {unit}", "attr.landArea", 6, True)
        for key in [
            "landAreaText",
            "landAreaDisplay",
            "landAreaValue",
            "landSizeDisplay",
            "landSizeText",
        ]:
            txt = container.get(key)
            if txt:
                parse_land_text("Land area", txt, "attr.landArea", 6, True)
        for key, prio in [
            ("pricePerSizeUnitLandArea", 6),
            ("minimumPricePerSizeUnitLandArea", 5),
            ("maximumPricePerSizeUnitLandArea", 4),
        ]:
            val = container.get(key)
            if val not in (None, ""):
                add_psf_candidate(val, f"attributes.{key}={val}", "attr.landPSF", prio)

    for root in _collect_all_json(soup):
        listing = jget(root, ["listingData"])
        if isinstance(listing, dict):
            process_attribute_container(listing.get("attributes") or {})
            la = listing.get("landArea")
            unit_hint = (
                listing.get("landAreaUnit")
                or listing.get("landAreaUnitType")
                or listing.get("landSizeUnit")
                or listing.get("landSizeUnitType")
            )
            if la not in (None, "") and unit_hint:
                add_land_candidate(la, unit_hint, f"listing.landArea={la} {unit_hint}", "attr.landArea", 6, True)
            txt = (
                listing.get("landAreaText")
                or listing.get("landAreaDisplay")
                or listing.get("landAreaValue")
                or listing.get("landSizeDisplay")
                or listing.get("landSizeText")
            )
            if txt:
                parse_land_text("Land area", txt, "attr.landArea", 6, True)
            psf_val = listing.get("landAreaPsf")
            if psf_val not in (None, "", 0):
                add_psf_candidate(psf_val, f"listing.landAreaPsf={psf_val}", "attr.landPSF", 5)
        process_attribute_container(root.get("attributes") or {})
        details = jget(root, ["detailsData", "metatable", "items"]) or jget(
            root, ["detailsData", "metaTable", "items"]
        )
        if isinstance(details, list):
            for item in details:
                if not isinstance(item, dict):
                    continue
                label = (
                    item.get("label")
                    or item.get("title")
                    or item.get("name")
                    or item.get("amenityLabel")
                )
                value = item.get("value") or item.get("valueText") or item.get("text")
                parse_land_text(label, value, "state.metatable", 4, True)
                parse_land_psf(label, value, "state.metatable", 4)

    for block in _extract_state_metatable_blocks(html):
        for match in re.finditer(
            r'\{[^{}]*(?:"label"|"title"|"name")\s*:\s*"([^"]+)"[^{}]*(?:"value"|"valueText"|"text")\s*:\s*"([^"]+)"[^{}]*\}',
            block,
        ):
            parse_land_text(match.group(1), match.group(2), "state.metatable", 4, True)
            parse_land_psf(match.group(1), match.group(2), "state.metatable", 4)

    meta_root = soup.select_one('.meta-table-root[da-id="property-details"]')
    if meta_root:
        for item in meta_root.select('.meta-table__item'):
            label_el = item.select_one('.amenity-label')
            value_el = item.select_one('.amenity-value')
            label_txt = label_el.get_text(' ', strip=True) if label_el else ""
            value_txt = value_el.get_text(' ', strip=True) if value_el else item.get_text(' ', strip=True)
            explicit = bool(label_el and LAND_LABEL_RE.search(label_txt.lower()))
            parse_land_text(label_txt, value_txt, "dom.metatable", 3, explicit)
            parse_land_psf(label_txt, value_txt, "dom.metatable", 3)

    hero_container = soup.find(attrs={"dataautomationid": "property-details"})
    if hero_container:
        for text_node in hero_container.find_all(string=True):
            parent = getattr(text_node, "parent", None)
            if parent and parent.find_parent(class_="meta-table__item"):
                continue
            txt = (text_node or "").strip()
            if not txt:
                continue
            parse_land_text("", txt, "hero.details", 2, False)
            parse_land_psf("", txt, "hero.details", 2)

    strata_types = {"condominium", "apartment", "serviced residence", "soho", "flat"}
    prop_type = (property_type or "").strip().lower()
    built_up_sqft = None
    if isinstance(built_up_value, (int, float)) and built_up_value > 0:
        built_up_sqft = _area_to_sqft(float(built_up_value), built_up_unit)

    best_land = None
    for cand in land_candidates:
        key = (cand["priority"], 1 if cand["explicit"] else 0)
        if best_land is None or key > (best_land["priority"], 1 if best_land["explicit"] else 0):
            best_land = cand

    explicit_source_present = attr_land_field_found or explicit_label_found

    if prop_type in strata_types and not explicit_source_present:
        raw_hits = []
        land_candidates = []
        psf_candidates = []
        best_land = None

    if best_land and built_up_sqft:
        if prop_type in strata_types and abs(best_land["sqft"] - built_up_sqft) < 1:
            best_land = None
            psf_candidates = []
            raw_hits = []

    if best_land and not is_rent and isinstance(price, (int, float)) and price > 0 and best_land.get("sqft"):
        computed_psf = price / best_land["sqft"]
        add_psf_candidate(
            computed_psf,
            f"computed price={_format_number(price)} land_sqft={_format_number(best_land['sqft'])}",
            "computed",
            1,
        )

    best_psf = None
    for cand in psf_candidates:
        if best_psf is None or cand["priority"] > best_psf["priority"]:
            best_psf = cand

    land_size = best_land["display"] if best_land else ""
    land_source = best_land["source"] if best_land else ""
    land_psf = f"{best_psf['value']:.2f}" if best_psf else ""
    land_psf_source = best_psf["source"] if best_psf else ""

    if not best_land and not best_psf:
        raw_hits = []

    return land_size, land_psf, raw_hits, land_source, land_psf_source

def extract_builtup(html, soup):
    m = re.search(r'"attributes"\s*:\s*{[^{}]*"builtUp"\s*:\s*"([^"]+)"', html, re.S | re.I)
    if m:
        raw = m.group(1)
        val = _num(raw)
        mu = re.search(r'"attributes"\s*:\s*{[^{}]*"sizeUnit"\s*:\s*"([^"]+)"', html, re.S | re.I)
        unit = (mu.group(1) if mu
                else ("sq ft" if re.search(r"ft|sq", raw, re.I) else ("sqm" if re.search(r"m²|sqm|meter", raw, re.I) else "sq ft")))
        if val:
            return val, unit
    for block in _extract_state_metatable_blocks(html):
        for v in re.finditer(r'"(?:value|valueText|text)"\s*:\s*"([^"]+)"', block, re.I):

            txt = v.group(1)
            if re.search(r"(built[\s-]?up|floor\s*area|size|keluasan|luas)", txt, re.I):
                m2 = re.search(r"([0-9][0-9,\.]*)\s*(sq\.?\s*ft|sqft|sf|sqm|m²|sq\.m)", txt, re.I)
                if m2:
                    return _num(m2.group(1)), m2.group(2)
    for item in soup.select(".meta-table__item"):
        txt = item.get_text(" ", strip=True)
        if re.search(r"(built[\s-]?up|floor\s*area|size|keluasan|luas)", txt, re.I):
            m2 = re.search(r"([0-9][0-9,\.]*)\s*(sq\.?\s*ft|sqft|sf|sqm|m²|sq\.m)", txt, re.I)
            if m2:
                return _num(m2.group(1)), m2.group(2)
    details = soup.find(attrs={"dataautomationid": "more-details-widget"}) or soup.find(
        string=re.compile(r"Property details", re.I)
    )
    buckets = [details.parent if details and hasattr(details, "parent") else details]
    hero = soup.find("h1")
    if hero:
        buckets.append(hero.parent)
    for c in [x for x in buckets if x]:
        txt = c.get_text(" ", strip=True)
        m2 = re.search(
            r"(?:Built[-\s]?up(?:\s*(?:size|area))?:?\s*)?([0-9][0-9,\.]*)\s*(sq\.?\s*ft|sqft|sf|sqm|m²|sq\.m)",
            txt, re.I)
        if m2:
            return _num(m2.group(1)), m2.group(2)
    return None, ""

def extract_builtup_psf(html, soup):
    m = re.search(r'"pricePerSizeUnitBuiltUp"\s*:\s*"([^"]+)"', html, re.I)
    if m:
        n = _num(m.group(1))
        if n is not None:
            return n
    for key in [
        "minimumPricePerSizeUnitBuiltUp", "maximumPricePerSizeUnitBuiltUp",
        "minimumPricePerSizeUnit", "maximumPricePerSizeUnit"
    ]:
        m = re.search(rf'"{key}"\s*:\s*"([^"]+)"', html, re.I)
        if m:
            n = _num(m.group(1))
            if n is not None:
                return n
    for key in ["floorAreaPsf", "builtUpPsf"]:
        m = re.search(rf'"{key}"\s*:\s*"?([0-9][0-9,\.]*)"?', html, re.I)
        if m:
            n = _num(m.group(1))
            if n is not None:
                return n
    for block in _extract_state_metatable_blocks(html):
        for v in re.finditer(r'"value"\s*:\s*"([^"]+)"', block, re.I):
            txt = v.group(1)
            if re.search(r"\bpsf\b", txt, re.I) and not re.search(r"\bland\b", txt, re.I):
                n = _num(txt)
                if n is not None:
                    return n
    for item in soup.select(".meta-table__item"):
        txt = item.get_text(" ", strip=True)
        if re.search(r"\bpsf\b", txt, re.I) and not re.search(r"\bland\b", txt, re.I):
            n = _num(txt)
            if n is not None:
                return n
    faq = soup.find(string=re.compile(r"(Current\s+PSF|Price\s+per\s+square\s+foot)", re.I))
    if faq:
        n = _num(faq.parent.get_text(" ", strip=True))
        if n is not None:
            return n
    return None

def extract_tenure(html, soup):
    TENURE_RX = re.compile(r'\b(Freehold|Leasehold)(?:\s*tenure)?\b', re.I)
    NOISE_RX  = re.compile(r'psf|floor|built', re.I)

    for block in _extract_state_metatable_blocks(html):
        # some items use value/valueText/text
        for v in re.finditer(r'"(?:value|valueText|text)"\s*:\s*"([^"]+)"', block, re.I):
            val = (v.group(1) or "").strip()
            if TENURE_RX.search(val) and not NOISE_RX.search(val):
                return TENURE_RX.search(val).group(1).title()  # -> "Freehold"/"Leasehold"
    return ""


def extract_property_type(html, soup):
    paths = [
        ["listingData", "propertyType"],
        ["listingData", "propertyTypeText"],
        ["listingData", "propertyTypeLocalizedText"],
        ["listingData", "propertyTypeGroup"],
        ["propertyOverviewData", "propertyInfo", "propertyType"],
        ["propertyOverviewData", "propertyInfo", "propertyTypeText"],
    ]
    for root in _collect_all_json(soup):
        for path in paths:
            val = jget(root, path)
            if not _is_blank(val):
                return str(val).strip()
    m = re.search(r'"propertyType"\s*:\s*"([^"]+)"', html, re.I)
    if m:
        return m.group(1).strip()
    meta_root = soup.select_one('.meta-table-root[da-id="property-details"]')
    if meta_root:
        for item in meta_root.select('.meta-table__item'):
            label = item.select_one('.amenity-label')
            value = item.select_one('.amenity-value')
            label_txt = label.get_text(' ', strip=True) if label else ""
            if re.search(r'property\s*type', label_txt or "", re.I) and value:
                txt = value.get_text(' ', strip=True)
                if not _is_blank(txt):
                    return txt
    return ""


def extract_listing_id(html, soup):
    for root in _collect_all_json(soup):
        val = jget(root, ["listingData", "listingId"]) or jget(root, ["listingData", "id"])
        if not _is_blank(val):
            return str(val).strip()
    m = re.search(r'"listingId"\s*:\s*"?([0-9A-Za-z-]+)"?', html, re.I)
    if m:
        return m.group(1).strip()
    return ""


BED_RE = re.compile(r"\bbed(?:room)?s?\b|\bbilik(?:\s*tidur)?\b|\b\d+\s*R\b", re.I)
BATH_RE = re.compile(r"\bbath(?:room)?s?\b|\bbilik\s*air\b|\btoilet\b|\b\d+\s*B\b", re.I)

def _normalize_beds_baths_token(tok):
    tok = (tok or "").strip()
    if not tok:
        return None, None
    if re.search(r"^\s*\d+\s*\+\s*\d+\s*$", tok):
        parts = re.findall(r"\d+", tok)
        if len(parts) >= 2:
            return int(parts[0]) + int(parts[1]), tok
    m = re.search(r"(\d+)", tok)
    return (int(m.group(1)) if m else None), tok

def extract_bed_bath(html, soup):
    bed_raw = bath_raw = None
    bed_n = bath_n = None
    m_bed = re.search(r'"attributes"\s*:\s*{[^{}]*"bedroom"\s*:\s*"([^"]+)"', html, re.I)
    m_bath = re.search(r'"attributes"\s*:\s*{[^{}]*"bathroom"\s*:\s*"([^"]+)"', html, re.I)
    if m_bed:
        bed_n, bed_raw = _normalize_beds_baths_token(m_bed.group(1))
    if m_bath:
        bath_n, bath_raw = _normalize_beds_baths_token(m_bath.group(1))
    if bed_n or bath_n:
        return bed_n, bath_n, bed_raw, bath_raw
    for root in _collect_all_json(soup):
        try:
            amenities = jget(root, ["propertyOverviewData", "propertyInfo", "amenities"])
            if isinstance(amenities, list):
                for it in amenities:
                    name = (it.get("unit") or it.get("name") or it.get("label") or "").strip()
                    if name.lower() in {"beds", "bed", "bedrooms"}:
                        bed_n, bed_raw = _normalize_beds_baths_token(str(it.get("value") or it.get("text") or it.get("valueText") or ""))
                    if name.lower() in {"baths", "bath", "bathrooms"}:
                        bath_n, bath_raw = _normalize_beds_baths_token(str(it.get("value") or it.get("text") or it.get("valueText") or ""))
            if bed_n or bath_n:
                return bed_n, bath_n, bed_raw, bath_raw
        except Exception:
            pass
    bed_el = soup.select_one('.wide-property-snapshot-info [da-id="amenity-beds"] .amenity-value')
    bath_el = soup.select_one('.wide-property-snapshot-info [da-id="amenity-baths"] .amenity-value')
    if bed_el:
        bed_n, bed_raw = _normalize_beds_baths_token(bed_el.get_text(strip=True))
    if bath_el:
        bath_n, bath_raw = _normalize_beds_baths_token(bath_el.get_text(strip=True))
    if bed_n or bath_n:
        return bed_n, bath_n, bed_raw, bath_raw
    for o in extract_ld_objects(soup, "RealEstateListing"):
        for ap in (o.get("additionalProperty") or []):
            name = (ap.get("name") or "").strip().lower()
            if name in {"bedrooms", "bedroom", "beds"}:
                bed_n, bed_raw = _normalize_beds_baths_token(str(ap.get("value")))
            if name in {"bathrooms", "bathroom", "baths"}:
                bath_n, bath_raw = _normalize_beds_baths_token(str(ap.get("value")))
        if bed_n or bath_n:
            return bed_n, bath_n, bed_raw, bath_raw
    for item in soup.select('.meta-table-root[da-id="property-details"] [da-id="metatable-item"]'):
        txt = item.get_text(" ", strip=True)
        if "psf" in txt.lower() or "floor" in txt.lower() or "built" in txt.lower() or "title" in txt.lower():
            continue
        if BED_RE.search(txt):
            n, bed_raw = _normalize_beds_baths_token(txt)
            if n is not None:
                bed_n = bed_n or n
        if BATH_RE.search(txt):
            n, bath_raw = _normalize_beds_baths_token(txt)
            if n is not None:
                bath_n = bath_n or n
    if bed_n or bath_n:
        return bed_n, bath_n, bed_raw, bath_raw
    for meta in soup.select('meta[name="description"], meta[property="og:description"]'):
        t = meta.get("content") or ""
        m = re.search(r"(\d+)\s*bed(?:room)?(?:s)?\b.*?(\d+)\s*bath", t, re.I)
        if m:
            return int(m.group(1)), int(m.group(2)), t, t
        m2 = re.search(r"\b(\d+)R\b.*?\b(\d+)B\b", t, re.I)
        if m2:
            return int(m2.group(1)), int(m2.group(2)), t, t
    return None, None, None, None

CAR_PARK_RE = re.compile(
    r'\b(\d+)\s*(?:car\s*park(?:s)?|carpark(?:s)?|parking\s*(?:lot|lots|bay|bays|space|spaces|slot|slots))\b',
    re.I
)

def extract_car_park(html, soup):
    raw_list = []

    for block in _extract_state_metatable_blocks(html):
        for v in re.finditer(r'"(?:value|valueText|text)"\s*:\s*"([^"]+)"', block, re.I):
            val = (v.group(1) or "").strip()
            if re.search(r"psf|floor|built|title", val, re.I):
                continue
            if CAR_PARK_RE.search(val):
                raw_list.append(val)

    best_raw = raw_list[-1] if raw_list else ""
    max_n = 0
    for r in raw_list:
        for m in CAR_PARK_RE.finditer(r):
            max_n = max(max_n, int(m.group(1)))
    car_park = max_n if max_n > 0 else None
    return car_park, best_raw, raw_list


def extract_lister_phone(soup):
    digits = raw = ""
    best_candidates = []
    for root in _collect_all_json(soup):
        ag = jget(root, ["contactAgentData", "contactAgentCard", "agentInfoProps", "agent"]) or {}
        if isinstance(ag, dict):
            for k in ("mobile", "phone", "phonePretty"):
                v = ag.get(k)
                if v and not _is_blank(v):
                    best_candidates.append(v)
        ags = jget(root, ["contactAgentData", "contactAgentStickyBar", "agentInfoProps", "agent"]) or {}
        if isinstance(ags, dict):
            for k in ("mobile", "phone", "phonePretty"):
                v = ags.get(k)
                if v and not _is_blank(v):
                    best_candidates.append(v)
        lst = jget(root, ["listingData", "agent"]) or {}
        if isinstance(lst, dict):
            for k in ("mobile", "agentMobile", "phone", "phonePretty"):
                v = lst.get(k)
                if v and not _is_blank(v):
                    best_candidates.append(v)
    if best_candidates:
        def score(x):
            s = str(x)
            has_plus = 1 if s.strip().startswith("+") else 0
            dcount = len(_digits_only(s))
            mobile_bonus = 1 if re.search(r"\bmobile\b", s, re.I) else 0
            return (mobile_bonus, has_plus, dcount, len(s))
        best_candidates.sort(key=score, reverse=True)
        raw = str(best_candidates[0]).strip()
        digits = _digits_only(raw)
    return raw, digits

def extract_agent_name(html, soup):
    PRIORITY = {
        "contactAgentData": 4,
        "flight": 3,
        "dom": 2,
        "title": 1,
    }
    candidates = []
    seen = set()

    def _normalize_candidate(name):
        s = re.sub(r"\s+", " ", str(name or "")).strip()
        if not s:
            return ""
        s = re.sub(r"[|•]+", " ", s).strip(" ,-|•")
        if not (3 <= len(s) <= 40):
            return ""
        if re.search(r"private\s+advertiser", s, re.I):
            return ""
        if re.search(r"\b(realty|properties|property|estate|sdn|bhd|holdings|development|agency|group|team)\b", s, re.I):
            return ""
        if re.search(r"\d", s):
            return ""
        words = s.split()
        if len(words) < 2 or len(words) > 4:
            return ""
        letters = sum(ch.isalpha() for ch in s)
        if letters < max(3, int(len(s) * 0.6)):
            return ""
        normalized_words = []
        for w in words:
            if w.isupper() and len(w) <= 4:
                normalized_words.append(w)
            else:
                normalized_words.append(w.capitalize())
        return " ".join(normalized_words)

    def add_candidate(name, source):
        norm = _normalize_candidate(name)
        if not norm:
            return
        key = (norm.lower(), source)
        if key in seen:
            return
        seen.add(key)
        candidates.append({
            "priority": PRIORITY.get(source, 0),
            "source": source,
            "value": norm,
            "order": len(candidates),
        })

    json_paths = [
        (["contactAgentData", "contactAgentCard", "agentInfoProps", "agent", "name"], "contactAgentData"),
        (["contactAgentData", "contactAgentStickyBar", "agentInfoProps", "agent", "name"], "contactAgentData"),
        (["contactAgentData", "contactAgentSheet", "agentInfoProps", "agent", "name"], "contactAgentData"),
        (["enquiryModalData", "agent", "name"], "contactAgentData"),
        (["listingData", "agent", "name"], "flight"),
        (["listingData", "agentName"], "flight"),
        (["listingData", "listerName"], "flight"),
        (["listersInfo", 0, "listerName"], "flight"),
        (["listers", 0, "name"], "flight"),
    ]

    for root in _collect_all_json(soup):
        for path_vals, source in json_paths:
            val = jget(root, path_vals)
            if not _is_blank(val):
                add_candidate(val, source)
        agents = jget(root, ["agents"])
        if isinstance(agents, list):
            for ag in agents:
                name = ag.get("name")
                if not _is_blank(name):
                    add_candidate(name, "flight")
        listers = jget(root, ["listersInfo"])
        if isinstance(listers, list):
            for info in listers:
                if isinstance(info, dict):
                    name = info.get("listerName") or info.get("name")
                    if not _is_blank(name):
                        add_candidate(name, "flight")

    for sel in [
        '[da-id="agent-name"]',
        '[data-automation-id="agent-name"]',
        '.agent-info__name',
        '.agent-profile__name',
        '.contact-agent-card__agent-name',
        'a[href*="/property-agent/"]',
    ]:
        for el in soup.select(sel):
            txt = el.get_text(" ", strip=True)
            if not _is_blank(txt):
                add_candidate(txt, "dom")

    title_txt = soup.title.get_text() if soup.title else ""
    if title_txt:
        for match in re.finditer(r"\bby\s+([^|\-•]+)", title_txt, re.I):
            candidate = re.sub(r"\s+", " ", match.group(1)).strip(" ,-")
            if candidate:
                add_candidate(candidate, "title")

    best = None
    for cand in candidates:
        if best is None or cand["priority"] > best["priority"]:
            best = cand
    if best:
        return best["value"], best["source"]
    return "", ""


def extract_lister_id(html, soup, listing_id="", agent_name=""):
    PRIORITY = {
        "contactAgentData": 5,
        "flight": 4,
        "profileUrl": 3,
        "json.agent": 2,
        "dom.anchor": 1,
    }
    candidates = {}
    order_map = {}
    listing_id_str = str(listing_id).strip() if listing_id else ""
    agent_name_norm = re.sub(r"\s+", " ", agent_name or "").strip().lower()
    profile_re = re.compile(r"/property-agent/[^/-]+-(\d{4,9})(?:[#/?]|$)", re.I)

    def add_candidate(val, source, priority):
        if val is None:
            return
        s = str(val).strip()
        if not s.isdigit():
            return
        if len(s) < 4 or len(s) > 9:
            return
        if listing_id_str and s == listing_id_str:
            return
        if s not in order_map:
            order_map[s] = len(order_map)
        current = candidates.get(s)
        if current is None or priority > current["priority"]:
            candidates[s] = {"priority": priority, "source": source, "order": order_map[s]}

    json_id_paths = [
        (["contactAgentData", "contactAgentCard", "agentInfoProps", "agent", "id"], "contactAgentData", 5),
        (["contactAgentData", "contactAgentStickyBar", "agentInfoProps", "agent", "id"], "contactAgentData", 5),
        (["contactAgentData", "contactAgentSheet", "agentInfoProps", "agent", "id"], "contactAgentData", 5),
        (["enquiryModalData", "agent", "id"], "contactAgentData", 5),
        (["listingData", "listerId"], "flight", 4),
        (["listingData", "agentId"], "flight", 4),
        (["listingData", "agent", "id"], "flight", 4),
        (["listersInfo", 0, "listerId"], "flight", 4),
        (["listers", 0, "id"], "flight", 4),
    ]

    profile_paths = [
        ["contactAgentData", "contactAgentCard", "agentInfoProps", "agent", "profileUrl"],
        ["contactAgentData", "contactAgentStickyBar", "agentInfoProps", "agent", "profileUrl"],
        ["contactAgentData", "contactAgentSheet", "agentInfoProps", "agent", "profileUrl"],
        ["listingData", "agent", "profileUrl"],
    ]

    def walk_agent_ids(node):
        if isinstance(node, dict):
            for k, v in node.items():
                lk = str(k).lower()
                if lk in {"organisation", "organization", "agency", "user", "organisationid", "organizationid"}:
                    continue
                if lk in {"agentid", "agent_id"}:
                    add_candidate(v, "json.agent", 2)
                if lk == "agent" and isinstance(v, dict):
                    add_candidate(v.get("id"), "json.agent", 2)
                if isinstance(v, (dict, list)):
                    walk_agent_ids(v)
        elif isinstance(node, list):
            for item in node:
                walk_agent_ids(item)

    for root in _collect_all_json(soup):
        for path_vals, source, priority in json_id_paths:
            val = jget(root, path_vals)
            if not _is_blank(val):
                add_candidate(val, source, priority)
        for p in profile_paths:
            url = jget(root, p)
            if not _is_blank(url):
                match = profile_re.search(str(url))
                if match:
                    add_candidate(match.group(1), "profileUrl", 3)
        walk_agent_ids(root)

    for match in profile_re.finditer(html):
        add_candidate(match.group(1), "profileUrl", 3)

    for anchor in soup.select('a[href*="/property-agent/"]'):
        href = anchor.get("href", "")
        match = profile_re.search(href)
        if not match:
            continue
        text = anchor.get_text(" ", strip=True)
        source_priority = 1
        if agent_name_norm and re.sub(r"\s+", " ", text or "").strip().lower() == agent_name_norm:
            source_priority = 2
        add_candidate(match.group(1), "dom.anchor", source_priority)

    if not candidates:
        return "", ""

    best_id = None
    best_meta = None
    for id_str, meta in candidates.items():
        if best_meta is None or meta["priority"] > best_meta["priority"] or (
            meta["priority"] == best_meta["priority"] and meta["order"] < best_meta["order"]
        ):
            best_id = id_str
            best_meta = meta

    if best_id:
        return best_id, best_meta["source"]
    return "", ""


def extract_agency_name(soup):
    for root in _collect_all_json(soup):
        nm = jget(root, ["contactAgentData", "contactAgentCard", "agency", "name"])
        if not _is_blank(nm):
            return str(nm).strip()
    el = soup.select_one('[da-id="agent-agency-name"]')
    if el:
        txt = el.get_text(strip=True)
        if not _is_blank(txt):
            return txt
    return ""

def extract_agency_id(soup):
    source = ""
    for root in _collect_all_json(soup):
        v = jget(root, ["enquiryModalData", "agency", "id"])
        if not _is_blank(v):
            return str(v).strip(), "enquiryModalData.agency.id"
        v = jget(root, ["contactAgentData", "contactAgentCard", "agency", "id"])
        if not _is_blank(v):
            return str(v).strip(), "contactAgentData.contactAgentCard.agency.id"
        v = jget(root, ["contactAgentData", "contactAgentStickyBar", "agency", "id"])
        if not _is_blank(v):
            return str(v).strip(), "contactAgentData.contactAgentStickyBar.agency.id"
        v = jget(root, ["organisation", "organisationId"])
        if not _is_blank(v):
            return str(v).strip(), "flight.organisation.organisationId"
        orgs = jget(root, ["organisations"])
        if isinstance(orgs, list) and orgs:
            v = orgs[0].get("id")
            if not _is_blank(v):
                return str(v).strip(), "flight.organisations[0].id"
    return "", source

def extract_furnishing(html, soup):
    raw = ""
    m = re.search(r'"attributes"\s*:\s*{[^{}]*"furnishing"\s*:\s*"([^"]+)"', html, re.I)
    if m:
        raw = m.group(1).strip()
    if _is_blank(raw):
        for block in _extract_state_metatable_blocks(html):
            for v in re.finditer(r'"(?:value|valueText|text)"\s*:\s*"([^"]+)"', block, re.I):

                val = v.group(1)
                if re.search(r"psf|floor|built|title", val, re.I):
                    continue
                if re.search(r"\b(fully\s*furnished|part(?:ly|ially)\s*furnished|unfurnished|bare\s*unit)\b", val, re.I):
                    raw = val.strip()
                    break
            if raw:
                break
    if _is_blank(raw):
        for item in soup.select('.meta-table-root[da-id="property-details"] .meta-table__item__wrapper__value, .meta-table-root[da-id="property-details"] .meta-table__item__wrapper .amenity-value'):
            val = item.get_text(" ", strip=True)
            if re.search(r"psf|floor|built|title", val, re.I):
                continue
            if re.search(r"\b(fully\s*furnished|part(?:ly|ially)\s*furnished|unfurnished|bare\s*unit)\b", val, re.I):
                raw = val.strip()
                break
    canon = ""
    if raw:
        t = raw.lower()
        if re.search(r"fully\s*furnished", t):
            canon = "Fully Furnished"
        elif re.search(r"part(?:ly|ially)\s*furnished", t):
            canon = "Partially Furnished"
        elif re.search(r"bare\s*unit", t):
            canon = "Bare unit"
        elif re.search(r"\bunfurnished\b", t):
            canon = "Unfurnished"
    return canon, raw

def extract_full_address(soup):
    for root in _collect_all_json(soup):
        v = jget(root, ["propertyOverviewData", "propertyInfo", "fullAddress"])
        if not _is_blank(v):
            return _normalize_address(str(v).strip()), "state.fullAddress"
    for o in extract_ld_objects(soup, "RealEstateListing"):
        try:
            street = jget(o, ["spatialCoverage", "address", "streetAddress"])
            if not _is_blank(street):
                return _normalize_address(str(street).strip()), "jsonld.streetAddress"
        except Exception:
            pass
    el = soup.select_one('[da-id="property-full-address"]')
    if el:
        addr = el.get_text(" ", strip=True)
        if not _is_blank(addr):
            return _normalize_address(addr), "dom.fullAddress"
    return "", ""

def _normalize_address(s):
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r"\.\s*$", "", s)
    s = s.replace("&amp;", "&")
    return s

def extract_lister_url(soup):
    a = soup.select_one('a[href*="/property-agent/"]')
    if a and a.get("href"):
        href = a.get("href").strip()
        if href.startswith("/"):
            href = "https://www.iproperty.com.my" + href
        return href
    for root in _collect_all_json(soup):
        for p in [
            ["contactAgentData", "contactAgentCard", "agentInfoProps", "agent", "profileUrl"],
            ["contactAgentData", "contactAgentStickyBar", "agentInfoProps", "agent", "profileUrl"],
            ["listingData", "agent", "profileUrl"],
            ["contactAgentData", "contactAgentCard", "agentInfoProps", "agent", "website"],
            ["contactAgentData", "contactAgentStickyBar", "agentInfoProps", "agent", "website"],
            ["listingData", "agent", "website"],
            ["listers", 0, "website"],
            ["lister", "website"],
        ]:
            v = jget(root, p)
            if not _is_blank(v):
                href = str(v).strip()
                if href.startswith("/"):
                    href = "https://www.iproperty.com.my" + href
                if href.lower().startswith(("http://", "https://")):
                    return href
    return ""

LIC_KEYS = ["license", "licenseNumber", "renNo", "ren", "registrationNo"]

def extract_license_visible_text(soup):
    texts = []
    for node in soup.find_all(string=True):
        parent = node.parent
        if parent and parent.name in ("script", "style"):
            continue
        t = (node or "").strip()
        if t:
            texts.append(t)
    return " ".join(texts)

def extract_license_ren(soup, dom_text):
    for root in _collect_all_json(soup):
        for base in [
            ["contactAgentData", "contactAgentCard", "agentInfoProps", "agent"],
            ["contactAgentData", "contactAgentStickyBar", "agentInfoProps", "agent"],
            ["listingData", "agent"],
            ["props", "pageProps", "pageData", "data", "listingData", "agent"],
            ["pageProps", "pageData", "data", "listingData", "agent"],
        ]:
            node = jget(root, base)
            if isinstance(node, dict):
                for k in LIC_KEYS:
                    v = node.get(k)
                    if not _is_blank(v):
                        val = str(v).strip()
                        m = re.search(r"(?i)(REN|PEA|REA)\s*[:\-]?\s*(\d{3,7})", val)
                        if m:
                            return f"{m.group(1).upper()} {m.group(2)}"
    m = REN_PAT.search(dom_text)
    if m:
        return f"REN {m.group(1)}"
    return ""

def _iter_facility_texts(node):
    stack = [node]
    while stack:
        cur = stack.pop()
        if isinstance(cur, str):
            t = cur.strip()
            if t:
                yield t
        elif isinstance(cur, list):
            for item in reversed(cur):
                stack.append(item)
        elif isinstance(cur, dict):
            for key in ("text", "value", "valueText", "name", "label"):
                val = cur.get(key)
                if isinstance(val, str):
                    t = val.strip()
                    if t:
                        yield t
            for key in (
                "data",
                "items",
                "facilities",
                "amenities",
                "list",
                "values",
                "options",
                "chips",
                "tags",
            ):
                child = cur.get(key)
                if child:
                    stack.append(child)


def _gather_facilities_from_dom(container):
    texts = []
    if not container:
        return texts
    for chip in container.find_all(["li", "span", "a", "div", "p"], recursive=True):
        t = chip.get_text(" ", strip=True)
        if t:
            texts.append(t)
    return texts


def extract_amenities(soup, html):
    result = []

    for root in _collect_all_json(soup):
        for path in [
            ["props", "pageProps", "pageData", "data", "amenitiesData"],
            ["pageProps", "pageData", "data", "amenitiesData"],
            ["props", "pageProps", "pageData", "data", "facilitiesData"],
        ]:
            node = jget(root, path)
            if isinstance(node, (list, dict)):
                for text_val in _iter_facility_texts(node):
                    result.append(text_val)

        for key in ("Facilities", "Amenities"):
            items = _scan_label_items(root, key)
            for cand in items:
                result.append(cand)

        for key in ("facilities", "amenities"):
            arr = jget(root, [key])
            if isinstance(arr, (list, dict)):
                for text_val in _iter_facility_texts(arr):
                    result.append(text_val)

    if not result:
        dom_targets = []
        selectors = [
            '[data-automation-id="property-facilities-section"]',
            '[data-automation-id="property-amenities-section"]',
            '[da-id="property-facilities-section"]',
            '[da-id="property-amenities-section"]',
            '.property-facilities-section',
            '.property-amenities-section',
            '#property-facilities-section',
            '#property-amenities-section',
        ]
        for sel in selectors:
            dom_targets.extend(soup.select(sel))

        if dom_targets:
            for container in dom_targets:
                result.extend(_gather_facilities_from_dom(container))
        else:
            for htxt in ("Facilities", "Amenities"):
                hdr = soup.find(
                    lambda tag: tag.name in ("h2", "h3", "h4")
                    and tag.get_text(strip=True) == htxt
                )
                if not hdr:
                    continue
                sib = hdr.find_next_sibling()
                while sib and sib.name not in ("h2", "h3", "h4"):
                    result.extend(_gather_facilities_from_dom(sib))
                    sib = sib.find_next_sibling()

    cleaned = []
    seen = set()
    DROP_RE = re.compile(r"\b(psf|floor|built|tenure|title)\b", re.I)
    SKIP_RE = re.compile(r"\b(see all|common facilities)\b", re.I)
    for x in result:
        t = re.sub(r"\s+", " ", x).strip()
        if not t or DROP_RE.search(t) or SKIP_RE.search(t):
            continue
        key = t.lower()
        if key not in seen:
            seen.add(key)
            cleaned.append(t)
        if len(cleaned) >= 50:
            break
    return cleaned


def extract_bumi_lot(html, soup):
    flag = None
    raw_hits = []
    seen_raw = set()

    def add_raw(text):
        if not text:
            return
        cleaned = re.sub(r"\s+", " ", str(text)).strip()
        key = cleaned.lower()
        if cleaned and key not in seen_raw:
            seen_raw.add(key)
            raw_hits.append(cleaned)

    def register(flag_val, raw_text=None):
        nonlocal flag
        if raw_text:
            add_raw(raw_text)
        if flag_val is True:
            flag = True
        elif flag_val is False and flag is None:
            flag = False

    def interpret_text(text):
        t = (text or "").strip().lower()
        if not t:
            return None
        if "bumi" in t and "lot" in t:
            if any(x in t for x in ["non-bumi", "non bumi", "not bumi", "no bumi"]):
                return False
            return True
        return None

    def walk(obj, path=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                lk = str(k).lower()
                new_path = f"{path}.{k}" if path else str(k)
                if lk in {"bumilot", "isbumilot"}:
                    if isinstance(v, bool):
                        register(v, f"{new_path}={v}")
                    elif isinstance(v, (int, float)):
                        register(bool(v), f"{new_path}={v}")
                    elif isinstance(v, str):
                        guess = interpret_text(v)
                        register(guess, f"{new_path}={v}")
                    else:
                        add_raw(f"{new_path}={v}")
                if isinstance(v, (dict, list)):
                    walk(v, new_path)
                else:
                    if isinstance(v, str):
                        guess = interpret_text(v)
                        if guess is not None:
                            register(guess, v)
                        elif "bumi" in v.lower() and "lot" in v.lower():
                            add_raw(v)
        elif isinstance(obj, list):
            for idx, item in enumerate(obj):
                walk(item, f"{path}[{idx}]")

    for root in _collect_all_json(soup):
        walk(root)
        if flag is True:
            break

    if flag is None:
        selectors = [
            ".meta-table__item",
            ".meta-table__item__wrapper",
            ".meta-table__item__wrapper__value",
            "[class*='chip']",
            "[da-id='feature-chip']",
            "[da-id='feature-chips'] span",
        ]
        for node in soup.select(", ".join(selectors)):
            txt = node.get_text(" ", strip=True)
            if not txt:
                continue
            guess = interpret_text(txt)
            if guess is not None:
                register(guess, txt)
            elif "bumi" in txt.lower() and "lot" in txt.lower():
                add_raw(txt)

    flag_str = "Yes" if flag is True else ("No" if flag is False else "")
    return flag_str, raw_hits

def _scan_label_items(obj, label_name):
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                out.extend(_scan_label_items(v, label_name))
        if str(obj.get("label") or obj.get("title") or "").strip().lower() == label_name.lower():
            arr = obj.get("items") or obj.get("data") or obj.get("values") or obj.get("value")
            if isinstance(arr, list):
                for it in arr:
                    if isinstance(it, str):
                        out.append(it.strip())
                    elif isinstance(it, dict):
                        cand = (it.get("text") or it.get("value") or it.get("valueText") or it.get("name") or it.get("label") or "").strip()
                        if cand:
                            out.append(cand)
    elif isinstance(obj, list):
        for it in obj:
            out.extend(_scan_label_items(it, label_name))
    return out

# ------------------- FILE ITERATOR -------------------
def iter_html_payloads(root):
    for dirpath, dirnames, filenames in os.walk(root):
        for d in list(dirnames):
            if d.lower().endswith(".html"):
                sub = os.path.join(dirpath, d)
                for ddp, _, fns in os.walk(sub):
                    for fn in fns:
                        if fn.lower().endswith(".html"):
                            p = os.path.join(ddp, fn)
                            try:
                                yield p, open(p, "rb").read().decode("utf-8", "ignore")
                            except Exception:
                                pass
        for fn in filenames:
            path = os.path.join(dirpath, fn)
            try:
                with open(path, "rb") as fh:
                    head = fh.read(4)
                    fh.seek(0)
                    blob = fh.read()
            except Exception:
                continue
            if head.startswith(b"PK\x03\x04"):
                try:
                    with zipfile.ZipFile(path) as z:
                        for n in z.namelist():
                            if n.lower().endswith(".html"):
                                try:
                                    yield f"{path}|{n}", z.read(n).decode("utf-8", "ignore")
                                except Exception:
                                    continue
                except Exception:
                    pass
                continue
            if len(blob) >= 2 and blob[:2] == b"\x1f\x8b":
                try:
                    html = gzip.decompress(blob).decode("utf-8", "ignore")
                    yield path, html
                except Exception:
                    continue
                continue
            if fn.lower().endswith((".html", ".htm")):
                try:
                    yield path, blob.decode("utf-8", "ignore")
                except Exception:
                    continue

# ------------------- MAIN -------------------
def run():
    root = pick_root_if_needed(ROOT)
    rows = []
    seen = processed = 0
    print(f"Scanning: {root}")

    for name, html in iter_html_payloads(root):
        seen += 1
        soup = BeautifulSoup(html, "html.parser")

        url = extract_url(html, soup) or ""
        listing_id = extract_listing_id(html, soup)
        property_type = extract_property_type(html, soup)
        rent = is_rent_page(soup)
        _, price = extract_price(html, soup)
        b_val, b_unit = extract_builtup(html, soup)
        psf = extract_builtup_psf(html, soup)
        if psf is None and (not rent) and price and b_val:
            area_sqft = _area_to_sqft(b_val, b_unit)
            if area_sqft and 400 <= area_sqft <= 20000 and 10000 <= price <= 50000000:
                psf = round(price / area_sqft, 2)
        if b_val:
            unit_str = "sq ft" if _is_sqft(b_unit) or (not b_unit) else ("sqm" if _is_sqm(b_unit) else str(b_unit))
            built_up_str = f"{int(b_val) if float(b_val).is_integer() else b_val} {unit_str}"
        else:
            built_up_str = ""
        tenure = extract_tenure(html, soup)
        bed_n, bath_n, bed_raw, bath_raw = extract_bed_bath(html, soup)
        car_park, car_park_raw, car_park_list = extract_car_park(html, soup)
        lister_phone_raw, lister_phone_digits = extract_lister_phone(soup)
        agent_name, agent_name_source = extract_agent_name(html, soup)
        lister_id, lister_id_source = extract_lister_id(html, soup, listing_id, agent_name)
        agency_name = extract_agency_name(soup)
        agency_id, agency_id_source = extract_agency_id(soup)
        furnishing, furnishing_raw = extract_furnishing(html, soup)
        address, address_source = extract_full_address(soup)
        lister_url = extract_lister_url(soup)
        dom_text = extract_license_visible_text(soup)
        license_no = extract_license_ren(soup, dom_text)
        amenities = extract_amenities(soup, html)
        bumi_lot, bumi_lot_raw_list = extract_bumi_lot(html, soup)
        bumi_lot_raw = " | ".join(bumi_lot_raw_list) if bumi_lot_raw_list else ""
        land_size, land_psf, land_raw_list, land_source, land_psf_source = extract_land_size_psf(
            html,
            soup,
            property_type,
            price,
            rent,
            b_val,
            b_unit,
        )
        land_raw = " | ".join(land_raw_list) if land_raw_list else ""

        rows.append({
            "file": name,
            "url": url,
            "tenure": tenure,
            "bedroom": bed_n or "",
            "bathroom": bath_n or "",
            "bedroom_raw": bed_raw or "",
            "bathroom_raw": bath_raw or "",
            "car_park": car_park or "",
            "car_park_raw": car_park_raw or "",
            "car_park_raw_list": " | ".join(car_park_list) if car_park_list else "",
            "lister_phone_raw": lister_phone_raw,
            "lister_phone_digits": lister_phone_digits,
            "agent_name": agent_name,
            "agent_name_source": agent_name_source,
            "agency_name": agency_name,
            "agency_id": agency_id,
            "agency_id_source": agency_id_source,
            "lister_id": lister_id,
            "lister_id_source": lister_id_source,
            "furnishing": furnishing,
            "furnishing_raw": furnishing_raw,
            "address": address,
            "address_source": address_source,
            "lister_url": lister_url,
            "license": license_no,
            "amenities": "; ".join(amenities) if amenities else "",
            "bumi_lot": bumi_lot,
            "bumi_lot_raw": bumi_lot_raw,
            "land_size": land_size,
            "land_psf": land_psf,
            "land_raw": land_raw,
            "land_source": land_source,
            "land_psf_source": land_psf_source,
            "built_up": built_up_str,
            "built_up_psf": (f"{psf:.2f}" if isinstance(psf, (int, float)) else ""),
        })
        processed += 1

    out_csv = os.path.join(root, OUT_BASENAME)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "file","url","tenure",
            "bedroom","bathroom","bedroom_raw","bathroom_raw",
            "car_park","car_park_raw","car_park_raw_list",
            "lister_phone_raw","lister_phone_digits",
            "agent_name","agent_name_source",
            "agency_name","agency_id","agency_id_source",
            "lister_id","lister_id_source",
            "furnishing","furnishing_raw",
            "address","address_source",
            "lister_url","license",
            "amenities",
            "bumi_lot","bumi_lot_raw",
            "land_size","land_psf","land_raw","land_source","land_psf_source",
            "built_up","built_up_psf",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"Files seen: {seen} | processed: {processed}")
    print(f"Saved: {out_csv}")
    if rows:
        print('--- Preview (first 5 rows) ---')
        for r in rows[:5]:
            print({k: r[k] for k in ['file','tenure','bedroom','bathroom','built_up','built_up_psf','license']})

if __name__ == "__main__":
    run()
