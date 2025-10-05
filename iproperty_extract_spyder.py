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

def _normalize_land_unit(unit_txt):
    if not unit_txt:
        return ""
    u = str(unit_txt).strip().lower()
    if not u:
        return ""
    u = u.replace(".", " ").replace(",", " ")
    u = re.sub(r"\s+", " ", u)
    if "acre" in u or re.fullmatch(r"ac", u):
        return "acre"
    if "hectar" in u or re.fullmatch(r"ha", u):
        return "hectare"
    if "sq" in u and ("ft" in u or "feet" in u or "foot" in u or "sf" in u):
        return "sq ft"
    if "sf" == u:
        return "sq ft"
    if "sq" in u and ("m" in u or "metre" in u or "meter" in u or "m2" in u or "m²" in u):
        return "sqm"
    if "m2" == u or "m²" == u:
        return "sqm"
    if "sq" in u and ("yd" in u or "yard" in u):
        return "sq yd"
    if "perch" in u:
        return "perch"
    return u

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


def extract_land_area(html, soup):
    DIM_RE = re.compile(
        r"(\d+(?:[\.,]\d+)?)\s*[x×]\s*(\d+(?:[\.,]\d+)?)(?:\s*(?:[x×]\s*(\d+(?:[\.,]\d+)?)))?\s*([A-Za-z²°\s]*)",
        re.I,
    )
    AREA_RE = re.compile(
        r"([0-9][0-9,\.]*)(?:\s*-\s*[0-9][0-9,\.]*)?\s*(sq\.?\s*ft|sqft|sf|square\s*feet|sq\.?\s*m|sqm|square\s*met(?:er|re)|m²|ha\b|hectare(?:s)?|ac\b|acre(?:s)?|sq\.?\s*yd|square\s*y(?:ard|d)|perch(?:es)?)",
        re.I,
    )
    KEYWORDS_RE = re.compile(r"\b(land|lot|site|dimension|dimensi|tanah)\b", re.I)

    raw_hits = []
    seen_raw = set()
    candidates = []  # (value, unit, raw, priority)

    def add_raw(text):
        if not text:
            return
        cleaned = re.sub(r"\s+", " ", str(text)).strip()
        if cleaned:
            key = cleaned.lower()
            if key not in seen_raw:
                seen_raw.add(key)
                raw_hits.append(cleaned)

    def add_candidate(value, unit, raw_text, priority):
        if value is None:
            return
        if isinstance(value, (int, float)):
            num = float(value)
        else:
            num = _num(value)
        if num is None or num <= 0:
            return
        unit_norm = _normalize_land_unit(unit)
        if not unit_norm and raw_text:
            unit_norm = _normalize_land_unit(raw_text)
        add_raw(raw_text or value)
        candidates.append((num, unit_norm, raw_text or str(value), priority))

    def find_unit_hint(text):
        t = (text or "").lower()
        for token in ["sq ft", "sqft", "sf", "square feet", "sq. ft", "ft²", "ft2"]:
            if token in t:
                return "sq ft"
        for token in ["sq m", "sqm", "square metre", "square meter", "m²", "m2"]:
            if token in t:
                return "sqm"
        for token in ["sq yd", "square yard", "square yards", "sq. yd", "sqyd"]:
            if token in t:
                return "sq yd"
        for token in ["acre", "acres", "ac "]:
            if token in t:
                return "acre"
        for token in ["hectare", "hectares", "ha "]:
            if token in t:
                return "hectare"
        if "perch" in t:
            return "perch"
        return ""

    def parse_text(text, force=False):
        if not text:
            return
        cleaned = re.sub(r"\s+", " ", str(text)).strip()
        if not cleaned:
            return
        dim_found = False
        for dim in DIM_RE.finditer(cleaned):
            w = _num(dim.group(1))
            l = _num(dim.group(2))
            h = _num(dim.group(3)) if dim.group(3) else None
            unit_hint = dim.group(4) or ""
            unit = _normalize_land_unit(unit_hint) or find_unit_hint(cleaned)
            if w and l and unit:
                area = w * l
                if h and unit == "sqm":
                    area *= h
                add_candidate(area, unit, cleaned, 3)
                dim_found = True
        area_found = False
        for match in AREA_RE.finditer(cleaned):
            val = _num(match.group(1))
            unit = _normalize_land_unit(match.group(2))
            if val and unit:
                add_candidate(val, unit, cleaned, 2)
                area_found = True
        if dim_found or area_found:
            return
        if not force and not KEYWORDS_RE.search(cleaned):
            return
        val = _num(cleaned)
        if val:
            unit = find_unit_hint(cleaned)
            priority = 1 if unit else 0
            add_candidate(val, unit, cleaned, priority)

    for root in _collect_all_json(soup):
        listing = jget(root, ["listingData"])
        if isinstance(listing, dict):
            la = listing.get("landArea")
            la_text = (
                listing.get("landAreaText")
                or listing.get("landAreaDisplay")
                or listing.get("landAreaValue")
                or listing.get("landSizeDisplay")
                or listing.get("landSizeText")
            )
            unit_hint = (
                listing.get("landAreaUnit")
                or listing.get("landAreaUnitType")
                or listing.get("landSizeUnit")
                or listing.get("landSizeUnitType")
            )
            if la is not None and la != "":
                add_candidate(la, unit_hint or find_unit_hint(la_text or ""), f"landArea={la}", 1)
            parse_text(la_text, force=True)
        details = jget(root, ["detailsData", "metatable", "items"]) or jget(root, ["detailsData", "metaTable", "items"])
        if isinstance(details, list):
            for item in details:
                if isinstance(item, dict):
                    txt = item.get("value") or item.get("valueText") or item.get("text")
                    parse_text(txt)
        overview = jget(root, ["propertyOverviewData", "propertyInfo"])
        if isinstance(overview, dict):
            parse_text(overview.get("landArea"), force=True)
            parse_text(overview.get("landAreaDisplay"), force=True)

    for match in re.finditer(r'"landArea(?:Text|Display|Value)?"\s*:\s*"([^"]+)"', html, re.I):
        parse_text(match.group(1), force=True)
    for match in re.finditer(r'"(?:landArea|landSize)"\s*:\s*([0-9][0-9,\.]*)', html, re.I):
        add_candidate(match.group(1), "", match.group(0), 1)

    selectors = [
        ".meta-table__item",
        ".meta-table__item__wrapper",
        ".meta-table__item__wrapper__value",
        "[class*='land']",
        "[da-id='metatable-item']",
    ]
    try:
        for node in soup.select(", ".join(selectors)):
            txt = node.get_text(" ", strip=True)
            parse_text(txt)
    except Exception:
        pass

    if candidates:
        candidates.sort(key=lambda x: (x[3], 1 if x[1] else 0, x[0]), reverse=True)
        best = candidates[0]
        return best[0], best[1], raw_hits
    return None, "", raw_hits


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
    paths = [
        ["props", "pageProps", "pageData", "data", "listingData", "agent", "name"],
        ["props", "pageProps", "pageData", "data", "listingData", "agentName"],
        ["props", "pageProps", "pageData", "data", "contactAgentData", "contactAgentCard", "agentInfoProps", "agent", "name"],
        ["props", "pageProps", "pageData", "data", "contactAgentData", "contactAgentStickyBar", "agentInfoProps", "agent", "name"],
        ["props", "pageProps", "pageData", "data", "contactAgentData", "contactAgentSheet", "agentInfoProps", "agent", "name"],
        ["props", "pageProps", "pageData", "data", "enquiryModalData", "agent", "name"],
        ["props", "pageProps", "pageData", "data", "listersInfo", 0, "listerName"],
        ["pageProps", "pageData", "data", "listingData", "agent", "name"],
        ["pageProps", "pageData", "data", "listingData", "agentName"],
        ["pageProps", "pageData", "data", "contactAgentData", "contactAgentCard", "agentInfoProps", "agent", "name"],
        ["pageProps", "pageData", "data", "contactAgentData", "contactAgentStickyBar", "agentInfoProps", "agent", "name"],
        ["pageProps", "pageData", "data", "listersInfo", 0, "listerName"],
        ["pageData", "data", "listingData", "agent", "name"],
        ["pageData", "data", "listingData", "agentName"],
        ["pageData", "data", "listersInfo", 0, "listerName"],
        ["listingData", "agent", "name"],
        ["listingData", "agentName"],
        ["listingData", "listerName"],
        ["contactAgentData", "contactAgentCard", "agentInfoProps", "agent", "name"],
        ["contactAgentData", "contactAgentStickyBar", "agentInfoProps", "agent", "name"],
        ["enquiryModalData", "agent", "name"],
        ["listersInfo", 0, "listerName"],
        ["lister", "name"],
        ["agent", "name"],
    ]

    for root in _collect_all_json(soup):
        for path in paths:
            val = jget(root, path)
            if not _is_blank(val):
                return str(val).strip()
        agents = jget(root, ["agents"])
        if isinstance(agents, list):
            for ag in agents:
                name = ag.get("name")
                if not _is_blank(name):
                    return str(name).strip()
        listers = jget(root, ["listersInfo"])
        if isinstance(listers, list):
            for info in listers:
                if isinstance(info, dict):
                    name = info.get("listerName") or info.get("name")
                    if not _is_blank(name):
                        return str(name).strip()

    for sel in [
        '[da-id="agent-name"]',
        '[data-automation-id="agent-name"]',
        '.agent-info__name',
        '.agent-profile__name',
        '.contact-agent-card__agent-name',
    ]:
        el = soup.select_one(sel)
        if el:
            txt = el.get_text(" ", strip=True)
            if not _is_blank(txt):
                return txt

    m = re.search(r'"(?:agentName|listerName)"\s*:\s*"([^"]+)"', html, re.I)
    if m:
        return m.group(1).strip()
    return ""

def extract_lister_id(html, soup):
    PRIORITY = {
        "listerId": 5,
        "lister.id": 5,
        "listersInfo.listerId": 5,
        "agentId": 4,
        "agent.id": 3,
        "contactAgent.agent.id": 3,
        "regex:listerId": 2,
        "regex:agentId": 1,
    }
    candidates = []
    seen = set()

    def add_candidate(val, source):
        if _is_blank(val):
            return
        s = str(val).strip()
        if not s:
            return
        key = s.lower()
        if key in seen:
            return
        seen.add(key)
        digits = sum(c.isdigit() for c in s)
        is_numeric = 1 if s.isdigit() else 0
        priority = PRIORITY.get(source, 0)
        candidates.append((priority, is_numeric, digits, len(s), s))

    def walk(obj):
        visited = set()

        def _walk(node):
            oid = id(node)
            if oid in visited:
                return
            visited.add(oid)
            if isinstance(node, dict):
                for k, v in node.items():
                    lk = str(k).lower()
                    if lk in {"listerid", "lister_id"}:
                        add_candidate(v, "listerId")
                    if lk in {"agentid", "agent_id"}:
                        add_candidate(v, "agentId")
                    if lk == "agent" and isinstance(v, dict):
                        add_candidate(v.get("id"), "agent.id")
                    if lk == "lister" and isinstance(v, dict):
                        add_candidate(v.get("id"), "lister.id")
                    if isinstance(v, (dict, list)):
                        _walk(v)
            elif isinstance(node, list):
                for item in node:
                    _walk(item)

        _walk(obj)

    for root in _collect_all_json(soup):
        for path, src in [
            (["listingData", "listerId"], "listerId"),
            (["listingData", "agentId"], "agentId"),
            (["listingData", "agent", "id"], "agent.id"),
            (["contactAgentData", "contactAgentCard", "agentInfoProps", "agent", "id"], "contactAgent.agent.id"),
            (["contactAgentData", "contactAgentStickyBar", "agentInfoProps", "agent", "id"], "contactAgent.agent.id"),
            (["contactAgentData", "contactAgentSheet", "agentInfoProps", "agent", "id"], "contactAgent.agent.id"),
            (["enquiryModalData", "agent", "id"], "agent.id"),
            (["listersInfo", 0, "listerId"], "listersInfo.listerId"),
            (["lister", "id"], "lister.id"),
            (["agent", "id"], "agent.id"),
        ]:
            val = jget(root, path)
            if not _is_blank(val):
                add_candidate(val, src)
        walk(root)

    for key in ("listerId", "agentId"):
        for match in re.finditer(rf'"{key}"\s*:\s*"?([0-9A-Za-z_-]+)"?', html, re.I):
            add_candidate(match.group(1), f"regex:{key}")

    if candidates:
        candidates.sort(reverse=True)
        return candidates[0][4]
    return ""

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

def extract_amenities(soup, html):
    result = []
    for root in _collect_all_json(soup):
        for p in [
            ["props", "pageProps", "pageData", "data", "amenitiesData"],
            ["pageProps", "pageData", "data", "amenitiesData"],
            ["props", "pageProps", "pageData", "data", "facilitiesData"],
        ]:
            arr = jget(root, p)
            if isinstance(arr, list):
                for it in arr:
                    cand = (it.get("text") or it.get("value") or it.get("valueText") or it.get("name") or it.get("label") or "").strip()
                    if cand:
                        result.append(cand)
        for key in ("Facilities", "Amenities"):
            items = _scan_label_items(root, key)
            for cand in items:
                result.append(cand)
        for key in ("facilities", "amenities"):
            arr = jget(root, [key])
            if isinstance(arr, list):
                for it in arr:
                    if isinstance(it, str):
                        result.append(it.strip())
                    elif isinstance(it, dict):
                        cand = (it.get("text") or it.get("value") or it.get("valueText") or it.get("name") or it.get("label") or "").strip()
                        if cand:
                            result.append(cand)
    if not result:
        for htxt in ("Facilities", "Amenities"):
            hdr = soup.find(lambda tag: tag.name in ("h2", "h3", "h4") and (tag.get_text(strip=True) == htxt))
            if hdr:
                sib = hdr.find_next_sibling()
                while sib and sib.name not in ("h2", "h3", "h4"):
                    for chip in sib.find_all(["li", "span", "div"], recursive=True):
                        t = chip.get_text(" ", strip=True)
                        if t:
                            result.append(t)
                    sib = sib.find_next_sibling()
    cleaned = []
    seen = set()
    DROP_RE = re.compile(r"\b(psf|floor|built|tenure|title)\b", re.I)
    for x in result:
        t = re.sub(r"\s+", " ", x).strip()
        if not t or DROP_RE.search(t):
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
        b_val, b_unit = extract_builtup(html, soup)
        psf = extract_builtup_psf(html, soup)
        if psf is None:
            rent = is_rent_page(soup)
            cur, price = extract_price(html, soup)
            if (not rent) and price and b_val:
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
        agent_name = extract_agent_name(html, soup)
        lister_id = extract_lister_id(html, soup)
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
        land_area_val, land_area_unit, land_area_raw_list = extract_land_area(html, soup)
        land_area_unit = land_area_unit or ""
        if isinstance(land_area_val, (int, float)):
            land_area_value = float(land_area_val)
            land_area_str = (
                str(int(land_area_value))
                if land_area_value.is_integer()
                else (f"{land_area_value:.2f}".rstrip("0").rstrip("."))
            )
        else:
            land_area_str = ""
        land_area_raw = " | ".join(land_area_raw_list) if land_area_raw_list else ""

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
            "agency_name": agency_name,
            "agency_id": agency_id,
            "agency_id_source": agency_id_source,
            "lister_id": lister_id,
            "furnishing": furnishing,
            "furnishing_raw": furnishing_raw,
            "address": address,
            "address_source": address_source,
            "lister_url": lister_url,
            "license": license_no,
            "amenities": "; ".join(amenities) if amenities else "",
            "bumi_lot": bumi_lot,
            "bumi_lot_raw": bumi_lot_raw,
            "land_area": land_area_str,
            "land_area_unit": land_area_unit,
            "land_area_raw": land_area_raw,
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
            "agent_name",
            "agency_name","agency_id","agency_id_source",
            "lister_id",
            "furnishing","furnishing_raw",
            "address","address_source",
            "lister_url","license",
            "amenities",
            "bumi_lot","bumi_lot_raw",
            "land_area","land_area_unit","land_area_raw",
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
