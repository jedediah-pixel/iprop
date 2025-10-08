# -*- coding: utf-8 -*-
"""
Two-Phase iProperty Runner: ADLIST (SRP) -> ADVIEW (Detail)
- Mirrors your PG pipeline but targets iProperty
- ADLIST: fetch 4 categories (res/com × sale/rent), 100 pages each (total 400)
- FULL HTML capture for both ADLIST and ADVIEW, each saved as .html.gz
- ADLIST CSV + ADVIEW CSV built; ADVIEW merges ADLIST timing/IDs like PG
- Threaded, per-thread MV3 proxy extensions, staggered launches
- Page load timeout randomized per navigation
- FIXED: Proxy handling identical to your PG “correct” implementation
- NEW: Network usage metering (MB) for ADLIST/ADVIEW, reported in Discord on CSV upload
"""

import os, re, io, time, json, math, gzip, zipfile, random, queue, heapq, threading, logging, shutil, sys, csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse

import iproperty_extract_spyder as spyder

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

try:
    import requests
except Exception:
    requests = None

# ====================== USER CONFIG ======================
ADLIST_THREADS = 5
ADVIEW_THREADS = 5
VERSION_MAIN = 139
TIMEOUT_MIN = 40
TIMEOUT_MAX = 45
THREAD_LAUNCH_DELAY_STEP = 2  # T0=2s, T1=4s, ...

# Category page caps (exactly 100 each)
CATEGORIES = [
    {"intent": "sale", "segment": "residential", "pages": 1},
    {"intent": "rent", "segment": "residential", "pages": 1},
    {"intent": "sale", "segment": "commercial",  "pages": 1},
    {"intent": "rent", "segment": "commercial",  "pages": 1},
]

# Discord webhooks
DASHBOARD_WEBHOOK = "https://discord.com/api/webhooks/1405420190652567682/qOKf09vjntEdCRth8A6D9AkUsfPN_oWx5Yjbtz43QCqcZnzARrx_EX_qSwJosc9lhQ-y"
RETRY_WEBHOOK     = "https://discord.com/api/webhooks/1405420193756217394/LvtHVEmX4GjQQrQ_8W0O7MFSoAeaevTPJ0yScmMF4tScfAmrBM3dotWgUZdnjUTl0HFs"
EXHAUSTED_WEBHOOK = "https://discord.com/api/webhooks/1405545971122966549/DxH-c9pKo2J4BJka0FulzB55IdNIFPOQRatAKmg-CL6Il5UUM_xJfoPQafn4-zK_MQ4D"
CSV_WEBHOOK       = "https://discord.com/api/webhooks/1405554758726717573/akd645rjb2bS-GhUbTJei-GMVsWmd9c7FPb-aTL852s9Cc-Zx9Q3SDEdiPLidxyOQH4x"

DASHBOARD_UPDATE_MIN = 10
DASHBOARD_UPDATE_MAX = 20
DASHBOARD_BAR_WIDTH  = 16

# Proxy auth mode: "whitelist" (IP whitelisted → no creds) or "userpass" (use MV3 extension)
PROXY_MODE = "whitelist"  # or "userpass"
SYS_IP_OVERRIDE = ""      # e.g. "161.142.139.172" if you want to skip external probe

# Proxy pool
proxies = [
    # --- OLD 10 ---
    # {"server": "isp.decodo.com:10001", "username": "user-spkttgw7rl-ip-92.113.225.103", "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10002", "username": "user-spkttgw7rl-ip-92.113.225.238", "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10003", "username": "user-spkttgw7rl-ip-92.113.164.122", "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10004", "username": "user-spkttgw7rl-ip-92.113.225.147", "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10005", "username": "user-spkttgw7rl-ip-92.113.225.123", "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10006", "username": "user-spkttgw7rl-ip-92.113.164.255", "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10007", "username": "user-spkttgw7rl-ip-92.113.225.56",  "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10008", "username": "user-spkttgw7rl-ip-92.113.164.39",  "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10009", "username": "user-spkttgw7rl-ip-92.113.164.98",  "password": "718IbwlQ5b~ckmPlqE"},
    # {"server": "isp.decodo.com:10010", "username": "user-spkttgw7rl-ip-92.113.164.195", "password": "718IbwlQ5b~ckmPlqE"},
    # --- NEW 10 (corrected) ---
    {"server": "isp.decodo.com:10001", "username": "user-spqfamqoeg-ip-92.113.225.14",  "password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10002", "username": "user-spqfamqoeg-ip-92.113.164.242", "password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10003", "username": "user-spqfamqoeg-ip-92.113.164.179", "password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10004", "username": "user-spqfamqoeg-ip-92.113.225.53",  "password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10005", "username": "user-spqfamqoeg-ip-92.113.164.79",  "password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10006", "username": "user-spqfamqoeg-ip-92.113.164.163", "password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10007", "username": "user-spqfamqoeg-ip-92.113.225.182", "password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10008", "username": "user-spqfamqoeg-ip-92.113.164.66",  "password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10009", "username": "user-spqfamqoeg-ip-92.113.164.119", "password": "8e2~wuIbCEshvtu18K"},
    {"server": "isp.decodo.com:10010", "username": "user-spqfamqoeg-ip-92.113.225.175", "password": "8e2~wuIbCEshvtu18K"},
]
# optional per-proxy scheme (defaults to http), e.g. add "scheme": "http"|"https"|"socks5" to entries

# ====== Paths & Globals ======
TS = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
BASE_DIR = os.path.abspath(".")

ADLIST_DIR = os.path.join(BASE_DIR, f"adlist_iproperty_{TS}")
ADVIEW_DIR = os.path.join(BASE_DIR, f"adview_iproperty_{TS}")
LOG_DIR    = os.path.join(BASE_DIR, f"logs_{TS}")
EXT_DIR    = os.path.join(LOG_DIR, "proxy_exts")
os.makedirs(ADLIST_DIR, exist_ok=True)
os.makedirs(ADVIEW_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(EXT_DIR, exist_ok=True)

AUDIT_DIR  = os.path.join(ADVIEW_DIR, "audit")
os.makedirs(AUDIT_DIR, exist_ok=True)

ADLIST_EXT_ROOT = os.path.join(EXT_DIR, "adlist")
ADVIEW_EXT_ROOT = os.path.join(EXT_DIR, "adview")
os.makedirs(ADLIST_EXT_ROOT, exist_ok=True)
os.makedirs(ADVIEW_EXT_ROOT, exist_ok=True)

# ====== Logging ======
def setup_logging():
    def _prep(name,fname):
        lg = logging.getLogger(name); lg.setLevel(logging.INFO)
        fh = logging.FileHandler(os.path.join(LOG_DIR,fname), encoding="utf-8")
        fh.setFormatter(logging.Formatter('%(asctime)s - Thread%(thread_id)s - %(message)s'))
        lg.addHandler(fh); lg.propagate = False
        return lg
    return (_prep("performance","performance.log"),
            _prep("detection","detection.log"),
            _prep("errors","errors.log"))

perf_logger, detection_logger, error_logger = setup_logging()

# ====== IP / Proxy Helpers ======
def system_public_ipv4():
    ov = (globals().get("SYS_IP_OVERRIDE") or "").strip()
    if ov:
        return ov
    try:
        if requests:
            return requests.get("https://ipv4.api.ipify.org?format=json", timeout=10).json()["ip"]
    except Exception:
        pass
    return "unknown"

def _browser_ipv4_newtab(driver):
    cur = driver.current_window_handle
    driver.switch_to.new_window('tab')
    try:
        driver.get("https://ipv4.icanhazip.com/")
        ip = (driver.find_element(By.TAG_NAME, "body").text or "").strip()
    finally:
        driver.close()
        driver.switch_to.window(cur)
    return ip

def verify_proxy(driver, label="iP", sys_ip=None, thread_id=0):
    sys_ip = (sys_ip or system_public_ipv4()).strip()
    try:
        script = """
        const cb = arguments[0];
        fetch('https://ipv4.api.ipify.org?format=json', {cache:'no-store'})
          .then(r=>r.json()).then(j=>cb({ok:true, ip:j.ip}))
          .catch(e=>cb({ok:false, err:String(e)}));
        """
        res = driver.execute_async_script(script)
        pg_ip = res.get("ip") if res and res.get("ok") else None
    except Exception:
        pg_ip = None
    if not pg_ip:
        pg_ip = _browser_ipv4_newtab(driver)
    msg = f"[IP] {label}: system={sys_ip}  browser={pg_ip}"
    print(msg)
    try: detection_logger.info(msg, extra={'thread_id': thread_id})
    except: pass
    return bool(pg_ip) and (not str(pg_ip).startswith("error")) and (sys_ip != pg_ip)

def get_proxy_ip(idx:int)->str:
    try:
        u = proxies[idx]["username"]
        return u.split("-ip-")[1] if "-ip-" in u else f"proxy_{idx}"
    except Exception:
        return f"proxy_{idx}"

# ====== Discord Client ======
class DiscordClient:
    def __init__(self, webhook_url:str):
        self.enabled = (requests is not None) and bool(webhook_url)
        self.webhook = webhook_url
        self.msg_id = None
        self.queue = queue.Queue()
        self.sender_thread = None
        self.session = requests.Session() if (self.enabled and requests) else None
        self._stop = threading.Event()

    def start(self):
        if not self.enabled: return
        self.sender_thread = threading.Thread(target=self._run_sender, name="discord_sender", daemon=True)
        self.sender_thread.start()

    def stop(self):
        if not self.enabled: return
        self._stop.set()
        try: self.queue.put_nowait(("__STOP__", None))
        except: pass
        if self.sender_thread: self.sender_thread.join(timeout=5)

    def send_event(self, content:str):
        if not self.enabled: return
        self.queue.put(("event", {"content": content}))

    def send_file(self, file_path: str, content: str = None):
        if not self.enabled:
            print(f"[WEBHOOK] enqueue skipped (disabled): {file_path}")
            return
        try:
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
        except Exception:
            size_mb = -1
        print(f"[WEBHOOK] enqueue file: {os.path.basename(file_path)} ({size_mb:.2f} MB); q={self.queue.qsize()+1}")
        self.queue.put(("file", {"path": file_path, "content": content or ""}))

    def set_dashboard(self, content:str):
        if not self.enabled: return
        if self.msg_id is None:
            self.queue.put(("create", {"content": content}))
        else:
            self.queue.put(("edit", {"content": content}))

    def _run_sender(self):
        while not self._stop.is_set():
            try:
                kind, payload = self.queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if kind == "__STOP__": break
            try:
                if kind == "create":
                    url = self.webhook + "?wait=true"
                    r = self.session.post(url, json=payload, timeout=15)
                    if r.status_code == 200:
                        data = r.json()
                        self.msg_id = data.get("id")
                elif kind == "edit" and self.msg_id:
                    edit_url = self.webhook + f"/messages/{self.msg_id}"
                    self.session.patch(edit_url, json=payload, timeout=15)
                elif kind == "event":
                    self.session.post(self.webhook, json=payload, timeout=15)
                elif kind == "file":
                    path = payload["path"]; text = payload.get("content", "")
                    try:
                        size = os.path.getsize(path)
                        if size >= 10 * 1024 * 1024:
                            self.session.post(self.webhook, json={"content": f"⚠️ File too large to upload ({size/1024/1024:.2f} MB): {os.path.basename(path)}"}, timeout=15)
                        else:
                            upload_url = self.webhook + "?wait=true"
                            with open(path, "rb") as f:
                                files = {"file": (os.path.basename(path), f, "application/octet-stream")}
                                data = {"content": text or ""}
                                r = self.session.post(upload_url, data=data, files=files, timeout=60)
                            print(f"[WEBHOOK] status={r.status_code}")
                            if r.status_code >= 300:
                                print(f"[WEBHOOK] ERROR body: {(r.text or '')[:400]}")
                    except Exception:
                        pass
            except Exception:
                pass
            finally:
                time.sleep(0.3)

# ====== Utilities ======
def text_bar(pct: float, width: int = DASHBOARD_BAR_WIDTH) -> str:
    pct = max(0.0, min(1.0, pct))
    filled = int(round(pct * width))
    return "█" * filled + "░" * (width - filled)

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
]
def pick_ua(): return random.choice(user_agents)

def mask_ip(ip:str)->str:
    try:
        parts = ip.split("."); parts[-1] = "xxx"; return ".".join(parts)
    except: return ip

def safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", str(s)).strip("-")[:120]

# ====== Proxy + Driver ======
def build_proxy_ext(proxy_cfg:dict, thread_id:int, root:str)->str:
    host, port_str = proxy_cfg["server"].split(":")
    port = int(port_str)
    scheme = proxy_cfg.get("scheme", "http")
    ext_dir = os.path.join(root, f"t{thread_id}_{host}_{port}")
    os.makedirs(ext_dir, exist_ok=True)

    manifest = {
        "name": f"ProxyAuth t{thread_id}",
        "version": "1.0",
        "manifest_version": 3,
        "permissions": ["proxy", "webRequest", "webRequestBlocking", "storage"],
        "host_permissions": ["<all_urls>"],
        "background": {"service_worker": "background.js"}
    }
    bg_js = f"""
const cfg = {{
  scheme: "{scheme}",
  host: "{host}",
  port: {port},
  username: "{proxy_cfg.get('username','')}",
  password: "{proxy_cfg.get('password','')}"
}};

function setProxy() {{
  chrome.proxy.settings.set({{
    value: {{
      mode: "fixed_servers",
      rules: {{
        singleProxy: {{ scheme: cfg.scheme, host: cfg.host, port: cfg.port }},
        bypassList: ["<local>"]
      }}
    }},
    scope: "regular"
  }});
}}
chrome.runtime.onInstalled.addListener(setProxy);
chrome.runtime.onStartup.addListener(setProxy);

// Only answer PROXY auth (MV3 requires asyncBlocking)
chrome.webRequest.onAuthRequired.addListener(
  (details, callback) => {{
    if (details.isProxy && cfg.username && cfg.password) {{
      callback({{ authCredentials: {{ username: cfg.username, password: cfg.password }} }});
    }} else {{
      callback();
    }}
  }},
  {{ urls: ["<all_urls>"] }},
  ["asyncBlocking"]
);
"""
    with open(os.path.join(ext_dir, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    with open(os.path.join(ext_dir, "background.js"), "w", encoding="utf-8") as f:
        f.write(bg_js)
    return ext_dir

def _enable_net_logging(opts: uc.ChromeOptions):
    # Perf log for Network.* events
    try:
        opts.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    except Exception:
        pass

def safe_driver_quit(driver, timeout_s: float = 4.0):
    """
    Quit Chrome without hanging the worker; hard-kill chromedriver if quit() stalls.
    Never call itself recursively.
    """
    try:
        import threading

        def _q():
            try:
                driver.quit()
            except Exception:
                pass

        t = threading.Thread(target=_q, daemon=True)
        t.start()
        t.join(timeout_s)

        if t.is_alive():
            # quit() is hung → best-effort hard kill
            try:
                # kill chromedriver process
                driver.service.process.kill()
            except Exception:
                pass
    except Exception:
        pass


def start_driver(user_agent:str, proxy_cfg:dict, thread_id:int, ext_root:str):
    opts = uc.ChromeOptions()

    # Force proxy at process start (critical)
    scheme = proxy_cfg.get("scheme", "http")
    # opts.add_argument(f"--proxy-server={scheme}://{proxy_cfg['server']}")
    # opts.add_argument("--proxy-bypass-list=<-loopback>")
    opts.add_argument("--no-proxy-server")


    # Hardening / perf
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1366,768")
    opts.add_argument("--lang=en-US,en")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-features=InterestCohort")
    opts.add_argument("--disable-quic")
    opts.add_argument("--force-webrtc-ip-handling-policy=disable_non_proxied_udp")
    opts.page_load_strategy = "eager"
    opts.add_argument(f"user-agent={user_agent}")

    _enable_net_logging(opts)

    if PROXY_MODE == "userpass":
        ext_path = build_proxy_ext(proxy_cfg, thread_id, ext_root)
        opts.add_argument(f"--disable-extensions-except={ext_path}")
        opts.add_argument(f"--load-extension={ext_path}")

    driver = uc.Chrome(options=opts, version_main=VERSION_MAIN)
    try:
        # Enable Network domain for richer events
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Page.enable", {})
    except Exception:
        pass
    driver.set_page_load_timeout(random.uniform(TIMEOUT_MIN, TIMEOUT_MAX))
    return driver

# ====== Network metering ======
def drain_network_bytes(driver) -> int:
    """Sum encodedDataLength from performance log and drain it."""
    total = 0
    try:
        logs = driver.get_log("performance")
        for entry in logs:
            try:
                msg = json.loads(entry.get("message", "{}")).get("message", {})
                if msg.get("method") == "Network.loadingFinished":
                    params = msg.get("params", {})
                    total += int(params.get("encodedDataLength", 0))
            except Exception:
                continue
    except Exception:
        pass
    return total

# ====== HTML save + compress ======
def save_gz_html(dir_path:str, base_name:str, html:str) -> str:
    os.makedirs(dir_path, exist_ok=True)
    gz_path = os.path.join(dir_path, f"{base_name}.html.gz")
    with gzip.open(gz_path, "wb") as f:
        f.write(html.encode("utf-8", errors="ignore"))
    return gz_path

# ====== Stage Container ======
class Stage:
    def __init__(self, name:str, threads:int, ext_root:str):
        self.name = name
        self.threads = threads
        self.ext_root = ext_root

        self.ready_q = queue.Queue()
        self.deferred_q = queue.Queue()
        self.delayed_heap = []
        self.delayed_lock = threading.Lock()
        self.delayed_seq = 0

        self.state_lock = threading.Lock()
        self.in_flight = set()
        self.done_set = set()
        self.deferred_set = set()

        self.used_proxies = set()
        self.assigned_proxy = {}
        self.initial_proxy_indices = set()

        self.thread_stats = {}
        self.thread_stats_lock = threading.Lock()

        self.metrics = {"total": 0, "completed": 0, "ok": 0, "retried": 0, "deferred": 0, "final_exhausted": 0, "start_ts": time.time()}
        self.overall_bar = None
        self.thread_bars = {}

        # Network usage
        self.net_bytes = 0
        self.net_lock = threading.Lock()

    def add_net_bytes(self, n:int):
        if n <= 0: return
        with self.net_lock:
            self.net_bytes += int(n)

    def assign_initial_proxy(self, thread_id:int, exclude:set|None=None)->int:
        preferred = thread_id % len(proxies)
        candidates = list(range(len(proxies)))
        if exclude:
            candidates = [i for i in candidates if i not in exclude] or list(range(len(proxies)))
        with self.state_lock:
            idx = None
            if preferred in candidates and preferred not in self.used_proxies:
                idx = preferred
            else:
                for i in candidates:
                    if i not in self.used_proxies:
                        idx = i; break
                if idx is None:
                    idx = preferred
            self.used_proxies.add(idx)
            self.assigned_proxy[thread_id] = idx
            self.initial_proxy_indices.add(idx)
            return idx

    def rotate_proxy_for_thread(self, thread_id:int, current_idx:int)->int:
        with self.state_lock:
            self.used_proxies.discard(current_idx)
            idx = None
            for i in range(len(proxies)):
                if i != current_idx and i not in self.used_proxies:
                    idx = i; break
            if idx is None:
                idx = (current_idx + 1) % len(proxies)
                if idx == current_idx: idx = (idx + 1) % len(proxies)
            self.used_proxies.add(idx)
            self.assigned_proxy[thread_id] = idx
            return idx

    def release_proxy(self, idx:int):
        with self.state_lock:
            self.used_proxies.discard(idx)

    def schedule_retry(self, task:dict, seconds:int):
        ready_at = time.time() + seconds
        with self.delayed_lock:
            self.delayed_seq += 1
            heapq.heappush(self.delayed_heap, (ready_at, self.delayed_seq, task))

    def pop_due_delayed(self, to_ready_max:int=100):
        moved = 0
        now = time.time()
        while True:
            with self.delayed_lock:
                if self.delayed_heap and self.delayed_heap[0][0] <= now and moved < to_ready_max:
                    _, _, task = heapq.heappop(self.delayed_heap)
                else:
                    break
            self.ready_q.put(task); moved += 1
        return moved

# ====== iProperty specifics ======
BASE_DOMAIN = "https://www.iproperty.com.my"
ALLOW_NETLOC = {"www.iproperty.com.my", "iproperty.com.my"}
DOMAIN_NETLOC = "www.iproperty.com.my"

ID_PAT = re.compile(r"-(\d{5,})/?(?:[#?].*)?$", re.I)
HREF_PAT = re.compile(r"/(property|listing|rent|sale)/", re.I)
TRAILING_ID_PAT = re.compile(r"-(\d{5,})/?(?:[#?].*)?$")
STATE_HEAD_RX = re.compile(r'window\.__INITIAL_STATE__\s*=\s*({)', re.I)

def _ld_blocks(html: str):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            out.append(json.loads(s.string or s.text or ""))
        except Exception:
            pass
    return out

def _ld_first(blocks, typ):
    for blk in blocks:
        if isinstance(blk, dict) and blk.get("@type") == typ:
            return blk
        if isinstance(blk, list):
            for e in blk:
                if isinstance(e, dict) and e.get("@type") == typ:
                    return e
    return None

def _ld_additional_props(listing_dict: dict) -> dict:
    out = {}
    props = listing_dict.get("additionalProperty") or []
    if isinstance(props, dict):
        props = [props]
    for p in props:
        if not isinstance(p, dict): 
            continue
        name = (p.get("name") or "").strip().lower()
        val  = p.get("value")
        if name:
            out[name] = val
    return out

def _extract_breadcrumb_names_from_ld(html: str) -> list[str]:
    blocks = _ld_blocks(html)
    bl = _ld_first(blocks, "BreadcrumbList")
    names = []
    if isinstance(bl, dict):
        for el in bl.get("itemListElement", []):
            if isinstance(el, dict):
                nm = el.get("name") or (isinstance(el.get("item"), dict) and el["item"].get("name")) or ""
                nm = str(nm).strip()
                if nm:
                    names.append(nm)
    return names

def _parse_meta_facts(html: str) -> dict:
    """
    Extract structured facts only from the meta strip (.meta-table__item).
    Examples seen in your pages:
      - 'Freehold tenure'
      - 'Not Bumi Lot'
      - 'Listed on 16 Aug 2025'
      - '1,830 sqft floor area'
      - 'RM 10.40 psf (floor)'
      - 'Listing ID - 108488217'
    """
    soup = BeautifulSoup(html, "html.parser")
    facts = [el.get_text(" ", strip=True) for el in soup.select(".meta-table__item")]
    out = {
        "tenure": "",
        "bumi_lot": "",
        "listed_date": "",
        "listed_time": "",
        "floor_area": "",
        "land_area": "",
        "psf": "",
        "listing_id": ""
    }

    for t in facts:
        low = t.lower()
        # tenure
        if "tenure" in low:
            # e.g. 'Freehold tenure' → 'Freehold'
            out["tenure"] = re.sub(r'(?i)\s*tenure\s*', '', t).strip()
        # bumi
        if "bumi" in t:
            out["bumi_lot"] = "No" if re.search(r'(?i)not\s+bumi', t) else "Yes"
        # listed on
        m = re.search(r'(?i)\bListed on\s+([0-9]{1,2}\s+\w+\s+\d{4})(?:\s+(\d{1,2}:\d{2}))?', t)
        if m:
            out["listed_date"] = m.group(1)
            out["listed_time"] = m.group(2) or ""
        # floor/land
        mfa = re.search(r'([\d,]+)\s*sqft\s*(?:built[-\s]?up|floor)\s*area', low, flags=0)
        if mfa:
            # read from original t to keep commas
            out["floor_area"] = re.sub(r'[^\d]', '', re.search(r'([\d,]+)', t).group(1))
        mla = re.search(r'([\d,]+)\s*sqft\s*land\s*area', low, flags=0)
        if mla:
            out["land_area"] = re.sub(r'[^\d]', '', re.search(r'([\d,]+)', t).group(1))
        # psf
        mpsf = re.search(r'RM\s*([\d,]+(?:\.\d+)?)\s*psf', t, re.I)
        if mpsf and not out["psf"]:
            out["psf"] = mpsf.group(1).replace(",", "")
        # listing id
        mid = re.search(r'Listing ID\s*-\s*(\d+)', t, re.I)
        if mid and not out["listing_id"]:
            out["listing_id"] = mid.group(1)

    return out

def _extract_facilities(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    sec = soup.select_one(".property-facilities-section")
    if not sec:
        return []
    vals = [p.get_text(" ", strip=True)
            for p in sec.select(".property-amenities__row-item__value")]
    vals = [v for v in vals if v and len(v) < 60]
    # dedupe preserving order
    out = []
    for v in vals:
        if v not in out:
            out.append(v)
    return out

# ---- JSON helpers (read window.__INITIAL_STATE__) ----
def _load_initial_state(html: str) -> dict:
    m = STATE_HEAD_RX.search(html)
    if not m: return {}
    js = _extract_balanced_json(html, m.start(1))
    if not js: return {}
    try:
        return json.loads(_json_sanitize(js))
    except Exception:
        return {}
    
def _dedupe_keep_order(items):
    out = []
    for v in items or []:
        if v and v not in out:
            out.append(v)
    return out

def _extract_facilities_and_amenities_from_state(html: str) -> tuple[list[str], list[str]]:
    """Read lists from window.__INITIAL_STATE__.detailsData.(facilitiesData|amenitiesData).data[].text"""
    st = _load_initial_state(html)
    dd = (st.get("detailsData") or {})

    def _pull_list(node):
        vals = []
        if not isinstance(node, dict):
            return vals
        data = node.get("data") or []
        for it in data:
            if isinstance(it, dict):
                t = (it.get("text") or it.get("name") or "").strip()
                if t and len(t) < 80:
                    vals.append(t)
        return vals

    fac = _pull_list(dd.get("facilitiesData") or {})
    ame = _pull_list(dd.get("amenitiesData") or {})  # sometimes null/absent; safe

    return _dedupe_keep_order(fac), _dedupe_keep_order(ame)

def _extract_amenities_from_dom(html: str) -> list[str]:
    """Fallback DOM parse for amenities if STATE is missing."""
    soup = BeautifulSoup(html, "html.parser")
    # try common containers; keep it permissive
    candidates = []
    for sel in (".property-amenities-section",
                "[data-automation-id='amenities']",
                ".amenities", ".amenities-list"):
        sec = soup.select_one(sel)
        if sec:
            candidates.append(sec)
    vals = []
    for sec in candidates:
        for n in sec.find_all(True):
            txt = (n.get_text(" ", strip=True) or "").strip()
            if txt and len(txt) < 80:
                vals.append(txt)
    return _dedupe_keep_order(vals)


def _extract_facilities_from_state(html: str) -> list[str]:
    st = _load_initial_state(html)
    out = []
    dd = (st.get("detailsData") or {})
    fd = dd.get("facilitiesData") or {}
    for it in (fd.get("data") or []):
        if isinstance(it, dict):
            t = (it.get("text") or "").strip()
            if t and t not in out:
                out.append(t)
    return out

def _extract_agent_from_state(html: str) -> dict:
    """
    Pull agent name / profileUrl / REN / agency / agency_reg straight from the JSON you showed.
    """
    st = _load_initial_state(html)
    out = {"name":"", "profile_url":"", "ren":"", "agency":"", "agency_reg":"", "agent_id":""}

    # contactAgentData.contactAgentCard.agentInfoProps.agent
    cad = (st.get("contactAgentData") or {}).get("contactAgentCard") or {}
    agent = ((cad.get("agentInfoProps") or {}).get("agent") or {})
    if isinstance(agent, dict):
        out["name"] = (agent.get("name") or "").strip() or out["name"]
        out["profile_url"] = (agent.get("profileUrl") or "").strip() or out["profile_url"]
        lic = agent.get("licenseNumber") or ""
        if isinstance(lic, str):
            m = re.search(r'\bREN\s*([0-9]{3,6})\b', lic, re.I)
            if m: out["ren"] = m.group(1)

    # contactAgentData.contactAgentCard.agency
    agency = cad.get("agency") or {}
    if isinstance(agency, dict):
        out["agency"] = (agency.get("name") or "").strip() or out["agency"]
        out["agency_reg"] = (agency.get("registrationNumber") or agency.get("licenseNo") or "").strip() or out["agency_reg"]

    # agent_id from profile URL
    if out["profile_url"]:
        m = re.search(r'-(\d+)(?:[#/]|$)', out["profile_url"])
        if m: out["agent_id"] = m.group(1)

    return out

def _extract_bumi_text_from_state(html: str) -> str:
    """
    Return the exact Bumi label (e.g. 'Not Bumi Lot', 'Bumi Lot', 'Non-Bumi Lot')
    from window.__INITIAL_STATE__. Falls back to attributes booleans.
    """
    st = _load_initial_state(html)
    dd = (st.get("detailsData") or {})

    # 1) meta table items (preferred: exact phrase like 'Not Bumi Lot')
    for key in ("metaTable", "metatable"):
        node = dd.get(key) or {}
        items = node.get("items") or []
        if isinstance(items, dict):
            items = [items]
        for it in items:
            if not isinstance(it, dict):
                continue
            for vk in ("value", "text", "valueText"):
                v = it.get(vk)
                if isinstance(v, str) and re.search(r"\bbumi\b", v, re.I):
                    return v.strip()

    # 2) attributes booleans (map to a readable phrase)
    attrs = dd.get("attributes") or {}
    for k in ("bumiLot", "isBumiLot", "isBumi"):
        if k in attrs:
            v = attrs.get(k)
            if isinstance(v, bool):
                return "Bumi Lot" if v else "Not Bumi Lot"
            if isinstance(v, str):
                s = v.strip().lower()
                if re.search(r"\bnon[-\s]?bumi|not\b", s):
                    return "Not Bumi Lot"
                if re.search(r"\bbumi|yes|true\b", s):
                    return "Bumi Lot"
                return v.strip()

    # 3) short strings under propertyInfo (rare, but keep as last resort)
    pinfo = dd.get("propertyInfo") or {}
    for val in pinfo.values():
        if isinstance(val, str) and re.search(r"\bbumi\b", val, re.I):
            return val.strip()

    return ""

def _extract_agent_block(html: str, base="https://www.iproperty.com.my"):
    """
    Agent card only. Avoids description/comments.
    """
    soup = BeautifulSoup(html, "html.parser")
    name = ""
    profile_url = ""
    agency = ""
    ren = ""
    agent_id = ""

    # anchor for profile near the agent name block
    name_el = soup.select_one("[class*='agent-name']")
    if name_el:
        name = name_el.get_text(" ", strip=True)
        # prefer link within the same card container
        container = name_el
        for i in range(3):
            if container.parent: container = container.parent
        a = container.select_one("a[href*='/property-agent/']") or name_el.find("a", href=True)
        if a and a.get("href"):
            profile_url = a["href"]
            if profile_url.startswith("/"):
                profile_url = urljoin(base, profile_url)

        # REN near the agent card
        nearby_text = container.get_text(" ", strip=True)
        m_ren = re.search(r'\bREN\s*([0-9]{3,6})\b', nearby_text, re.I)
        if m_ren:
            ren = m_ren.group(1)

        # agency often appears in a nearby contact/brand node
        branding = container.select_one("[class*='brand'], [class*='agency'], [class*='name']")
        if branding:
            val = branding.get_text(" ", strip=True)
            if val and len(val) < 80 and val.lower() != (name or "").lower():
                agency = val

    # fallback: scan page (still avoiding description)
    if not profile_url:
        a = soup.select_one("a[href*='/property-agent/']")
        if a and a.get("href"):
            profile_url = a["href"]
            if profile_url.startswith("/"):
                profile_url = urljoin(base, profile_url)

    # agent_id from profile URL
    if profile_url:
        m = re.search(r'-(\d+)(?:[#/]|$)', profile_url)
        if m:
            agent_id = m.group(1)

    # last resort REN anywhere (not in description blocks)
    if not ren:
        all_text = soup.get_text(" ", strip=True)
        m_ren = re.search(r'\bREN\s*([0-9]{3,6})\b', all_text, re.I)
        if m_ren:
            ren = m_ren.group(1)

    return name, profile_url, agency, ren, agent_id

def is_srp_self_link(url: str) -> bool:
    return ("/all-residential" in url) or ("/all-commercial" in url)

def build_adlist_url(intent:str, segment:str, page:int)->str:
    path = f"/{intent}/all-{segment}/?page={page}"
    return urljoin(BASE_DOMAIN, path)

def _extract_balanced_json(text: str, open_brace_idx: int) -> str | None:
    depth = 0
    in_str = False
    esc = False
    for i in range(open_brace_idx, len(text)):
        ch = text[i]
        if in_str:
            if esc: esc = False
            elif ch == '\\': esc = True
            elif ch == '"': in_str = False
        else:
            if ch == '"': in_str = True
            elif ch == '{': depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    return text[open_brace_idx:i+1]
    return None

def _json_sanitize(js: str) -> str:
    return re.sub(r'(?<=[:\s])undefined\b', 'null', js)

def _get_by_path(d: dict, dotted: str):
    cur = d
    for tok in dotted.split("."):
        if isinstance(cur, dict) and tok in cur:
            cur = cur[tok]
        else:
            return None
    return cur

def extract_from_iproperty_initial_state(html: str):
    rows = []
    m = STATE_HEAD_RX.search(html)
    if not m:
        return rows
    js = _extract_balanced_json(html, m.start(1))
    if not js:
        return rows
    try:
        state = json.loads(_json_sanitize(js))
    except Exception:
        return rows

    candidate_paths = [
        "listings.items",
        "searchResults.listings.items",
        "results.listings.items",
    ]
    items = None
    for p in candidate_paths:
        items = _get_by_path(state, p)
        if isinstance(items, list) and items:
            break
    if not isinstance(items, list):
        return rows

    seen = set()
    for it in items:
        if not isinstance(it, dict):
            continue
        url = normalize_url(
            it.get("shareLink")
            or it.get("url")
            or it.get("canonicalUrl")
            or it.get("detailUrl")
            or ""
        )
        if not url or url in seen or is_srp_self_link(url):
            continue
        seen.add(url)
        title = (it.get("name") or it.get("title") or it.get("seoTitle") or "").strip()
        rows.append({"url": url, "title": title})
    return rows

def extract_listing_id(url: str) -> str | None:
    m = ID_PAT.search(url or "")
    return m.group(1) if m else None

def normalize_url(u: str) -> str:
    if not u:
        return ""
    full = u if u.startswith("http") else urljoin(BASE_DOMAIN, u)
    p = urlparse(full)
    if p.netloc.lower() not in ALLOW_NETLOC:
        return ""
    cleaned = p._replace(query="", fragment="")
    return urlunparse(cleaned)

def parse_ld_json(html: str) -> list[dict]:
    out = []
    soup = BeautifulSoup(html, "html.parser")
    for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(s.string or "")
            if isinstance(data, list):
                out.extend([d for d in data if isinstance(d, dict)])
            elif isinstance(data, dict):
                out.append(data)
        except Exception:
            continue
    return out

def extract_adlist_rows_from_html(html: str, intent: str, segment: str, page_no: int):
    all_rows = []
    seen = set()
    # 1) window.__INITIAL_STATE__
    try:
        rows_state = extract_from_iproperty_initial_state(html)
    except Exception:
        rows_state = []
    for r in rows_state:
        url = r.get("url") or ""
        if not url or url in seen or is_srp_self_link(url):
            continue
        seen.add(url)
        if not any(ch.isdigit() for ch in url):
            continue
        all_rows.append({
            "intent": intent,
            "segment": segment,
            "url": url,
            "title": (r.get("title") or "").strip(),
            "listed_unix": None,
            "agent_name": None,
            "agent_id": None,
            "listing_id": extract_listing_id(url),
            "page_no": page_no
        })
    # 2) JSON-LD ItemList
    try:
        ld = parse_ld_json(html)
    except Exception:
        ld = []
    for block in ld:
        if (block.get("@type") or "").lower() == "itemlist":
            for el in block.get("itemListElement") or []:
                item = el.get("item") if isinstance(el, dict) else None
                if not isinstance(item, dict):
                    continue
                url = normalize_url(item.get("url", ""))
                if not url or url in seen or is_srp_self_link(url):
                    continue
                seen.add(url)
                title = (item.get("name") or "").strip()
                posted_iso = item.get("datePosted") or item.get("datePublished")
                listed_unix = None
                if posted_iso:
                    try:
                        listed_unix = int(pd.Timestamp(posted_iso).timestamp())
                    except Exception:
                        pass
                if not any(ch.isdigit() for ch in url):
                    continue
                all_rows.append({
                    "intent": intent,
                    "segment": segment,
                    "url": url,
                    "title": title,
                    "listed_unix": listed_unix,
                    "agent_name": None,
                    "agent_id": None,
                    "listing_id": extract_listing_id(url),
                    "page_no": page_no
                })
    # 3) Anchors (fallback)
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        soup = None
    if soup is not None:
        for a in soup.find_all("a", href=True):
            url = normalize_url(a["href"])
            if not url or url in seen or is_srp_self_link(url):
                continue
            if not HREF_PAT.search(url):
                continue
            seen.add(url)
            if not any(ch.isdigit() for ch in url):
                continue
            all_rows.append({
                "intent": intent,
                "segment": segment,
                "url": url,
                "title": a.get_text(" ", strip=True) or "",
                "listed_unix": None,
                "agent_name": None,
                "agent_id": None,
                "listing_id": extract_listing_id(url),
                "page_no": page_no
            })
    return all_rows

# === React Flight (self.__next_f.push) helpers — same sourcing as Script 1 ===
RF_FLIGHT_PUSH_RX = re.compile(
    r'self\.__next_f\.push\(\s*\[\s*1\s*,\s*"(.*?)"\s*\]\s*\)',
    re.DOTALL
)

def rf_unescape_js_string(s: str) -> str:
    return json.loads(f'"{s}"')

def rf_find_flight_strings(html: str) -> list[str]:
    out = []
    for m in RF_FLIGHT_PUSH_RX.finditer(html):
        try:
            decoded = rf_unescape_js_string(m.group(1))
            if decoded.startswith("d:"):
                out.append(decoded)
        except Exception:
            continue
    return out

def rf_balanced_json_from(decoded: str, start_idx: int) -> str | None:
    s = decoded[start_idx:]
    depth, in_str, esc = 0, False, False
    for i, ch in enumerate(s):
        if in_str:
            if esc: esc = False
            elif ch == '\\': esc = True
            elif ch == '"': in_str = False
        else:
            if ch == '"': in_str = True
            elif ch == '{': depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    return s[:i+1]
    return None

def rf_balanced_array_from(decoded: str, start_idx: int) -> str | None:
    s = decoded[start_idx:]
    depth, in_str, esc = 0, False, False
    for i, ch in enumerate(s):
        if in_str:
            if esc: esc = False
            elif ch == '\\': esc = True
            elif ch == '"': in_str = False
        else:
            if ch == '"': in_str = True
            elif ch == '[': depth += 1
            elif ch == ']':
                depth -= 1
                if depth == 0:
                    return s[:i+1]
    return None

def rf_json_load(text: str):
    try:
        return json.loads(text)
    except Exception:
        return None

def rf_json_from_d_block(decoded: str):
    if not decoded.startswith("d:"):
        return None
    return rf_json_load(decoded[2:])

def rf_walk(n):
    if isinstance(n, dict):
        yield n
        for v in n.values():
            yield from rf_walk(v)
    elif isinstance(n, list):
        for v in n:
            yield from rf_walk(v)

RF_WANTED_KEYS = ("listingDetail", "listers", "lister", "organisation", "organisations")

def rf_collect_candidates(root) -> list[dict]:
    cands = []
    for n in rf_walk(root):
        if isinstance(n, dict) and any(k in n for k in RF_WANTED_KEYS):
            cands.append(n)
    return cands

def rf_regex_extract_by_key(decoded: str, key: str) -> list[dict]:
    out, start = [], 0
    while True:
        i = decoded.find(f'"{key}"', start)
        if i < 0:
            break
        j_obj = decoded.find("{", i)
        j_arr = decoded.find("[", i)
        if j_arr != -1 and (j_arr < j_obj or j_obj == -1):
            blob = rf_balanced_array_from(decoded, j_arr)
            obj = rf_json_load(blob) if blob else None
            if obj is not None:
                out.append({key: obj})
            start = j_arr + (len(blob) if blob else 1)
        else:
            blob = rf_balanced_json_from(decoded, j_obj)
            obj = rf_json_load(blob) if blob else None
            if obj is not None:
                out.append({key: obj})
            start = j_obj + (len(blob) if blob else 1)
    return out

def rf_score_node(n: dict) -> int:
    s = 0
    if "listingDetail" in n: s += 10
    if "listers" in n:       s += 4
    if "lister" in n:        s += 3
    if "organisation" in n:  s += 3
    if "organisations" in n: s += 1
    ld = n.get("listingDetail") or {}
    if isinstance(ld, dict):
        for k in ("id","shareLink","title","propertyType","address","attributes","languagePlace"):
            if k in ld: s += 1
    return s

def rf_merge_best_components(cands: list[dict]) -> dict:
    best = {}
    # richest listingDetail
    best_ld, best_score = {}, -1
    for c in cands:
        ld = c.get("listingDetail")
        if isinstance(ld, dict):
            score = len(ld)
            if score > best_score:
                best_ld, best_score = ld, score
    if best_ld: best["listingDetail"] = best_ld
    # one of each aux
    for k in ("listers","lister","organisation","organisations"):
        for c in cands:
            v = c.get(k)
            if (isinstance(v, list) and v) or (isinstance(v, dict) and v):
                best[k] = v; break
    return best or (max(cands, key=rf_score_node) if cands else {})

def pick_best_node_from_flight(html: str) -> dict:
    flights = rf_find_flight_strings(html)
    cands = []
    for s in flights:
        root = rf_json_from_d_block(s)
        if isinstance(root, list):
            cands.extend(rf_collect_candidates(root))
    for s in flights:
        decoded = s[2:]
        for k in ("listingDetail","listers","lister","organisation","organisations"):
            cands.extend(rf_regex_extract_by_key(decoded, k))
    return rf_merge_best_components(cands)

def rf_pick_first(d, paths):
    def _get(cur, tok):
        if isinstance(cur, dict) and tok in cur: return cur[tok]
        if isinstance(cur, list) and tok.isdigit():
            i = int(tok); return cur[i] if 0 <= i < len(cur) else None
        return None
    for path in paths:
        cur = d; ok = True
        for tok in path.split("."):
            cur = _get(cur, tok)
            if cur is None: ok = False; break
        if ok and cur not in (None, "", []): return cur
    return ""

def rf_normalize_emails(val_list):
    out = []
    for v in val_list or []:
        s = str(v or "").strip()
        if not s or s == "[object Object]": continue
        out.append(s.lower())
    return "; ".join(sorted(set(out)))

def rf_normalize_phones(phones):
    nums = []
    for p in phones or []:
        if isinstance(p, dict):
            num = (p.get("number") or "").strip()
            label = (p.get("label") or "").strip()
        else:
            num, label = (str(p or "").strip(), "")
        if not num or num == "[object Object]": continue
        nums.append(f"{num}({label})" if label else num)
    return "; ".join(nums)

def rf_normalize_facilities(ld: dict) -> str:
    fac = []
    for it in ld.get("buildingFacilities", []) or []:
        desc = rf_pick_first(it, ["description.enGB"])
        if desc: fac.append(str(desc).strip())
    fd = ld.get("featureDescription") or ""
    if fd:
        parts = [p.strip() for p in str(fd).split(",")]
        fac.extend([p for p in parts if p])
    seen, out = set(), []
    for f in fac:
        key = f.lower()
        if key not in seen:
            seen.add(key); out.append(f)
    return ", ".join(out)


def extract_adview_fields_from_html(html: str, url_in: str) -> dict:
    """Extract a single detail page into the Spyder-aligned CSV row structure."""
    soup = BeautifulSoup(html, "html.parser")

    url = url_in or spyder.extract_url(html, soup) or ""
    short_title, short_title_source = spyder.extract_short_title(soup, url)
    long_title, long_title_source, long_title_suspect = spyder.extract_long_title(soup, short_title)
    listing_id, listing_id_source = spyder.extract_listing_id(html, soup)
    ad_id = listing_id
    property_type = spyder.extract_property_type(html, soup)
    rent = spyder.is_rent_page(soup)
    price_currency, price_value, price_source = spyder.extract_price(html, soup, rent)
    posted_date, posted_time, posted_date_source = spyder.extract_posted_datetime(soup)
    built_up_value, built_up_unit = spyder.extract_builtup(html, soup)
    psf = spyder.extract_builtup_psf(html, soup)
    if psf is None and (rent is False) and price_value and built_up_value:
        area_sqft = spyder._area_to_sqft(built_up_value, built_up_unit)
        if area_sqft and 400 <= area_sqft <= 20000 and 10000 <= price_value <= 50000000:
            psf = round(price_value / area_sqft, 2)

    if built_up_value:
        try:
            numeric = float(built_up_value)
            numeric_str = str(int(round(numeric)))
        except Exception:
            numeric_str = str(built_up_value)
        built_up_clean = spyder._digits_only(numeric_str)
    else:
        built_up_clean = ""

    tenure = spyder.extract_tenure(html, soup)
    bed_n, bath_n, bed_raw, bath_raw = spyder.extract_bed_bath(html, soup)
    car_park, car_park_raw, car_park_list = spyder.extract_car_park(html, soup)
    lister_phone_raw, lister_phone_digits = spyder.extract_lister_phone(soup)
    agent_name, agent_name_source = spyder.extract_agent_name(html, soup)
    lister_id, lister_id_source = spyder.extract_lister_id(html, soup, listing_id, agent_name)
    agency_name = spyder.extract_agency_name(soup)
    agency_id, agency_id_source = spyder.extract_agency_id(soup)
    furnishing, furnishing_raw = spyder.extract_furnishing(html, soup)
    state, _state_source, district, _district_source = spyder.extract_state_district(soup)
    address, address_source = spyder.extract_full_address(soup)
    lister_url = spyder.extract_lister_url(soup)
    dom_text = spyder.extract_license_visible_text(soup)
    license_no = spyder.extract_license_ren(soup, dom_text)
    amenities = spyder.extract_amenities(soup, html)
    bumi_lot, bumi_lot_raw_list = spyder.extract_bumi_lot(html, soup)
    land_size, land_psf, land_raw_list, land_source, land_psf_source = spyder.extract_land_size_psf(
        html,
        soup,
        property_type,
        price_value,
        rent,
        built_up_value,
        built_up_unit,
    )

    land_raw = " | ".join(land_raw_list) if land_raw_list else ""
    if land_size:
        land_numeric = spyder._num(land_size)
    else:
        land_numeric = None
    if land_numeric is not None:
        try:
            land_area_clean = spyder._digits_only(str(int(round(float(land_numeric)))))
        except Exception:
            land_area_clean = spyder._digits_only(str(land_numeric))
    else:
        land_area_clean = spyder._digits_only(land_size)

    source_domain = spyder._base_website(url)
    primary_phone_value = lister_phone_digits or lister_phone_raw
    phone_display = spyder._ensure_plus_prefix(primary_phone_value, force_text=True)
    phone_digits_display = spyder._ensure_plus_prefix(lister_phone_digits, force_text=True)
    description_title = spyder.extract_description_title(soup)

    if price_value is not None:
        if abs(price_value - round(price_value)) < 1e-6:
            price_str = str(int(round(price_value)))
        else:
            price_str = f"{price_value:.2f}".rstrip("0").rstrip(".")
    else:
        price_str = ""

    rent_sale = "rent" if rent else ("sale" if rent is not None else "")

    row = {
        "url": url or "",
        "type": property_type or "",
        "title": description_title or "",
        "currency": (price_currency or ("RM" if price_value else "")) or "",
        "price": price_str or "",
        "posted_date": posted_date or "",
        "posted_time": posted_time or "",
        "rooms": bed_raw or "",
        "toilets": bath_raw or "",
        "car_park": car_park or "",
        "phone": phone_display or "",
        "lister": agent_name or "",
        "agency": agency_name or "",
        "furnishing": furnishing or "",
        "location": address or "",
        "region": state or "",
        "subregion": district or "",
        "ren": license_no or "",
        "land_area": land_area_clean or "",
        "property_type": property_type or "",
        "build_up": built_up_clean or "",
        "listing_id": listing_id or "",
        "ad_id": ad_id or "",
        "phone_number": phone_digits_display or "",
        "phone_number2": "",
        "rent_sale": rent_sale,
        "seller_name": agent_name or "",
        "state": state or "",
    }

    for phone_key in ("phone", "phone_number", "phone_number2"):
        if row.get(phone_key):
            row[phone_key] = spyder._ensure_plus_prefix(row[phone_key], force_text=True)
        else:
            row[phone_key] = ""

    for default_key in [
        "activate_date",
        "ad_id",
        "created_time",
        "email",
        "id",
        "market",
        "scrape_date",
        "source",
        "updated_date",
    ]:
        row.setdefault(default_key, "")

    if source_domain:
        row["source"] = source_domain

    return row



# ====== Dashboard Builder ======
def build_dashboard_text(adlist: Stage, adview: Stage, phase:str) -> str:
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M') + " MYT"
    adlist_total = adlist.metrics["total"]; adview_total = adview.metrics["total"]
    adlist_done  = adlist.metrics["completed"]; adview_done = adview.metrics["completed"]

    if adview_total > 0:
        overall_total = adlist_total + adview_total
        overall_done  = adlist_done + adview_done
    else:
        overall_total = max(1, adlist_total)
        overall_done  = adlist_done
    overall_pct = overall_done / overall_total if overall_total else 0.0
    overall_bar = text_bar(overall_pct, DASHBOARD_BAR_WIDTH)

    def sec_fmt(sec:float)->str:
        if sec <= 0: return "--"
        return f"{int(sec//3600)}h{int((sec%3600)//60):02d}m"

    def stage_line(stage: Stage, label:str):
        total = stage.metrics["total"]; comp  = stage.metrics["completed"]
        ok    = stage.metrics["ok"];    retr  = stage.metrics["retried"]
        deff  = stage.metrics["deferred"]; errs  = stage.metrics["final_exhausted"]
        elapsed = max(1.0, time.time() - stage.metrics["start_ts"])
        rate = comp / elapsed
        remaining = max(0, total - comp)
        eta = sec_fmt(remaining / rate if rate > 0 else 0)
        pct = (comp / total) if total else 0.0
        bar = text_bar(pct, DASHBOARD_BAR_WIDTH)
        return f"{label}  [{bar}] {int(pct*100)}% • {comp:,}/{total:,} • ok={ok:,} • retried={retr:,} • deferred={deff:,} • errors={errs:,} • {rate:.2f} u/s • ETA {eta}"

    lines = []
    lines.append(f"🏠🔵 iProperty Multi-Phase — Run {now_str}")
    lines.append("")
    lines.append(f"Overall [{overall_bar}] {int(overall_pct*100)}% • {overall_done:,}/{overall_total:,}")
    lines.append(f"Phase: {phase}")
    lines.append("")
    lines.append(stage_line(adlist, "ADLIST"))
    lines.append(stage_line(adview, "ADVIEW" if adview.metrics["total"] > 0 else "ADVIEW (Pending)"))
    lines.append("")
    active = adlist if phase == "ADLIST" else adview
    fair = math.ceil(max(1, active.metrics["total"]) / max(1, active.threads))
    with active.thread_stats_lock:
        for tid in range(active.threads):
            st = active.thread_stats.get(tid, {"done":0,"state":"init","proxy":"-"})
            pct = (st["done"] / fair) if fair else 0.0
            tbar = text_bar(pct, 12)
            lines.append(f"{active.name[:1]}{tid}  [{tbar}] {int(pct*100)}% • {st['done']:,}/{fair:,} • {st.get('state','')} • {st.get('proxy','-')}")
    return "\n".join(lines)

# ====== Background loops ======
def dispatcher_loop(stage: Stage, stop_event: threading.Event):
    while not stop_event.is_set():
        stage.pop_due_delayed(100)
        time.sleep(0.5)

def dashboard_loop(stop_event: threading.Event, dashboard_bot: DiscordClient, adlist: Stage, adview: Stage, get_phase):
    last_pct = -1
    while not stop_event.is_set():
        try:
            phase = get_phase()
            text = build_dashboard_text(adlist, adview, phase)
            adlist_pct = (adlist.metrics["completed"] / adlist.metrics["total"]) if adlist.metrics["total"] else 0.0
            adview_pct = (adview.metrics["completed"] / adview.metrics["total"]) if adview.metrics["total"] else 0.0
            overall_pct = (adlist_pct + adview_pct) / (1 if adview.metrics["total"] == 0 else 2)
            significant = (int(overall_pct*100) != int(last_pct*100))
            if significant and dashboard_bot.enabled:
                dashboard_bot.set_dashboard(text)
                last_pct = overall_pct
            time.sleep(random.uniform(DASHBOARD_UPDATE_MIN, DASHBOARD_UPDATE_MAX))
            if dashboard_bot.enabled:
                dashboard_bot.set_dashboard(text)
        except Exception:
            time.sleep(2)

def looks_blocked_or_invalid_fast(html: str, cur_url: str) -> str | None:
    """
    Zero-wait: detect blocks / invalid detail pages. Return reason or None if OK.
    Allows through when expected data anchors exist.
    """
    h = html or ""
    hl = h.lower()
    # If page clearly has structured data for details, treat as OK
    if ("window.__initial_state__" in hl) or ('application/ld+json' in hl) or ('"@type":"product"' in hl) or ('"@type":"realestate' in hl):
        return None

    # Hard blocks / challenges
    block_terms = (
        "this site can’t be reached",  # chrome error html included in source sometimes
        "this site can't be reached",
        "access denied", "forbidden", "request unsuccessful", "bot detection",
        "verify you are human", "captcha", "attention required", "cloudflare",
        "temporarily blocked", "the page you requested has been blocked"
    )
    if any(t in hl for t in block_terms):
        return "block_marker"

    # Redirects to SRP or non-detail
    low = (cur_url or "").lower()
    if "/property-for-sale" in low or "/property-for-rent" in low or low.rstrip("/").endswith("/property"):
        return "redirected_to_srp"

    # Ultra-thin shell (very conservative)
    if len(h) < 12000:
        return f"tiny_shell({len(h)})"

    # Missing the usual data anchors for detail pages
    return "missing_state_and_ldjson"

def looks_blocked_or_invalid_srp_fast(html: str, cur_url: str) -> str | None:
    """
    Zero-wait: detect blocks / invalid SRP pages (ADLIST).
    SRP should usually have window.__INITIAL_STATE__ or many listing anchors.
    """
    h = html or ""
    hl = h.lower()

    # Hard blocks / challenges (same list)
    block_terms = (
        "this site can’t be reached", "this site can't be reached",
        "access denied", "forbidden", "request unsuccessful", "bot detection",
        "verify you are human", "captcha", "attention required", "cloudflare",
        "temporarily blocked", "the page you requested has been blocked"
    )
    if any(t in hl for t in block_terms):
        return "block_marker"

    # Meta refresh / error shells
    if '<meta http-equiv="refresh"' in hl:
        return "meta_refresh"

    # SRP usually has INITIAL_STATE
    if "window.__initial_state__" in hl:
        return None

    # Otherwise require a decent number of listing anchors as fallback
    # (cheap heuristic without parsing)
    listing_hint_count = hl.count('/property-listing/') + hl.count('/property-for-sale/') + hl.count('/property-for-rent/')
    if listing_hint_count >= 8:
        return None

    if len(h) < 12000:
        return f"tiny_shell({len(h)})"

    return "srp_missing_state_and_links"


# ====== ADLIST Worker ======
def adlist_worker(thread_id:int, stage: Stage, retry_bot: DiscordClient, exhausted_bot: DiscordClient):
    """ADLIST worker for iProperty with:
       - per-thread proxy
       - zero-wait SRP block detection
       - full HTML .html.gz snapshot
       - rows extraction
       - proper in_flight clearing in finally
       - network metering via stage.add_net_bytes(drain_network_bytes)
    """
    time.sleep((thread_id + 1) * THREAD_LAUNCH_DELAY_STEP)

    ua = pick_ua()
    proxy_idx = stage.assign_initial_proxy(thread_id, exclude=None)
    driver = start_driver(ua, proxies[proxy_idx], thread_id, ADLIST_EXT_ROOT)

    # Optional proxy verification (also meters tiny bytes)
    sys_ip = system_public_ipv4()
    try:
        driver.get("https://ipv4.icanhazip.com/")
        time.sleep(1.0)
    except Exception:
        pass
    verify_proxy(driver, label=f"ADLIST T{thread_id}", sys_ip=sys_ip, thread_id=thread_id)

    # drain any network bytes from the startup/IP probe
    stage.add_net_bytes(drain_network_bytes(driver))

    proxy_ip = mask_ip(get_proxy_ip(proxy_idx))
    with stage.thread_stats_lock:
        stage.thread_stats[thread_id] = {"done": 0, "state": "OK", "proxy": proxy_ip}

    try:
        while True:
            # Pull a task or recycle deferred/final tasks
            try:
                task = stage.ready_q.get(timeout=1.0)
            except queue.Empty:
                with stage.state_lock, stage.delayed_lock:
                    nothing_left = stage.ready_q.qsize() == 0 and len(stage.delayed_heap) == 0 and len(stage.in_flight) == 0
                    deferred_empty = stage.deferred_q.qsize() == 0
                if nothing_left and deferred_empty:
                    break
                else:
                    if nothing_left and not deferred_empty:
                        try:
                            while True:
                                t2 = stage.deferred_q.get_nowait()
                                stage.ready_q.put(t2)
                        except queue.Empty:
                            pass
                    continue

            intent  = task["intent"]
            segment = task["segment"]
            page_no = task["page"]
            attempt = task.get("attempt", 1)
            in_final = task.get("phase") == "Final"
            url = build_adlist_url(intent, segment, page_no)

            # de-dupe/in-flight guard
            with stage.state_lock:
                key = (intent, segment, page_no)
                if key in stage.done_set or key in stage.in_flight:
                    continue
                stage.in_flight.add(key)

            try:
                # nav with randomized timeout
                driver.set_page_load_timeout(random.uniform(TIMEOUT_MIN, TIMEOUT_MAX))
                driver.get(url)

                # meter network once per nav
                stage.add_net_bytes(drain_network_bytes(driver))

                # zero-wait SRP validity / block checks
                _raw = driver.page_source or ""
                why = looks_blocked_or_invalid_srp_fast(_raw, driver.current_url)
                if why:
                    detection_logger.info(f"[ADLIST] block_detected:{why} {url}", extra={'thread_id': thread_id})
                    raise RuntimeError(f"adlist_block_or_invalid:{why}")

                # snapshot HTML (one per page)
                html = _raw
                gz_path = save_gz_html(ADLIST_DIR, f"{intent}_{segment}_page_{page_no}", html)

                # extract rows from this page
                rows = extract_adlist_rows_from_html(html, intent, segment, page_no)
                scrape_unix = int(time.time())
                for r in rows:
                    r["scrape_unix"] = scrape_unix
                    r["snapshot_path"] = gz_path

                if not hasattr(stage, "adlist_rows"):
                    stage.adlist_rows = []
                    stage.adlist_rows_lock = threading.Lock()
                with stage.adlist_rows_lock:
                    stage.adlist_rows.extend(rows)

                # progress bookkeeping
                with stage.state_lock:
                    stage.metrics["ok"] += 1
                    stage.metrics["completed"] += 1
                    stage.done_set.add(key)
                with stage.thread_stats_lock:
                    st = stage.thread_stats[thread_id]
                    st["done"] += 1
                    st["state"] = "OK"

                if stage.overall_bar is not None:
                    stage.overall_bar.update(1)
                if stage.thread_bars.get(thread_id):
                    stage.thread_bars[thread_id].update(1)

                time.sleep(random.uniform(3.8, 4.6))

            except Exception as e:
                err_msg = str(e)[:180]
                error_logger.error(f"[ADLIST] {url} err: {err_msg}", extra={'thread_id': thread_id})

                old_mask = proxy_ip
                # hard-rotate proxy & restart driver
                try:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    proxy_idx = stage.rotate_proxy_for_thread(thread_id, proxy_idx)
                    proxy_ip  = mask_ip(get_proxy_ip(proxy_idx))
                    ua = pick_ua()
                    driver = start_driver(ua, proxies[proxy_idx], thread_id, ADLIST_EXT_ROOT)
                    with stage.thread_stats_lock:
                        stage.thread_stats[thread_id]["state"] = "Restarted"
                        stage.thread_stats[thread_id]["proxy"] = proxy_ip
                except Exception:
                    pass

                if in_final:
                    # Final attempt failed → mark exhausted so the run can terminate cleanly
                    with stage.state_lock:
                        stage.metrics["final_exhausted"] += 1
                        stage.metrics["completed"] += 1
                        stage.done_set.add(key)
                    with stage.thread_stats_lock:
                        st = stage.thread_stats[thread_id]
                        st["done"] += 1
                        st["state"] = "Final Exhausted"
                else:
                    if attempt == 1:
                        backoff = int(random.uniform(6, 18))
                        with stage.state_lock:
                            stage.metrics["retried"] += 1
                        if retry_bot.enabled:
                            retry_bot.send_event(
                                f"🔁 Retry A • ADLIST • T{thread_id}\nURL: {url}\nWhy: {type(e).__name__}: {err_msg}\n"
                                f"Fix: Restarted + rotated proxy ({old_mask} → {proxy_ip}); backoff {backoff//60}m{backoff%60:02d}s → reattempt (2/3)"
                            )
                        task["attempt"] = 2
                        stage.schedule_retry(task, backoff)
                    elif attempt == 2:
                        backoff = int(random.uniform(6, 10))
                        with stage.state_lock:
                            stage.metrics["retried"] += 1
                        if retry_bot.enabled:
                            retry_bot.send_event(
                                f"🔁 Retry B • ADLIST • T{thread_id}\nURL: {url}\nWhy: {type(e).__name__}: {err_msg}\n"
                                f"Fix: Restarted + rotated proxy ({old_mask} → {proxy_ip}); backoff {backoff//60}m{backoff%60:02d}s → reattempt (3/3)"
                            )
                        task["attempt"] = 3
                        stage.schedule_retry(task, backoff)
                    else:
                        # Move to deferred queue for one FINAL pass
                        with stage.state_lock:
                            stage.metrics["deferred"] += 1
                        if key not in stage.deferred_set:
                            stage.deferred_set.add(key)
                            task2 = dict(task)
                            task2["phase"] = "Final"   # <<< ensure final pass is tagged
                            stage.deferred_q.put(task2)

            finally:
                # Always clear in_flight for this page so ADLIST can progress/finish
                with stage.state_lock:
                    if key in stage.in_flight:
                        stage.in_flight.discard(key)

    finally:
        # Clean shutdown for this worker
        try:
            driver.quit()
        except Exception:
            pass
        stage.release_proxy(proxy_idx)
        with stage.thread_stats_lock:
            stage.thread_stats[thread_id]["state"] = "finished"


# ====== ADVIEW Worker ======
SUCCESS_F = os.path.join(AUDIT_DIR, "successes.ndjson")
DEFER_F   = os.path.join(AUDIT_DIR, "deferred.ndjson")
FAIL_F    = os.path.join(AUDIT_DIR, "failures_exhausted.ndjson")
audit_lock = threading.Lock()

def audit_append(path:str, obj:dict):
    try:
        with audit_lock, open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    except Exception:
        pass
    
def adview_worker(thread_id:int, stage: Stage, retry_bot: DiscordClient, exhausted_bot: DiscordClient):
    time.sleep((thread_id + 1) * THREAD_LAUNCH_DELAY_STEP)

    ua = pick_ua()
    proxy_idx = stage.assign_initial_proxy(thread_id, exclude=None)
    driver = start_driver(ua, proxies[proxy_idx], thread_id, ADVIEW_EXT_ROOT)

    # Optional proxy verification
    sys_ip = system_public_ipv4()
    try:
        driver.get("https://ipv4.icanhazip.com/"); time.sleep(1.0)
    except Exception:
        pass
    verify_proxy(driver, label=f"ADVIEW T{thread_id}", sys_ip=sys_ip, thread_id=thread_id)

    stage.add_net_bytes(drain_network_bytes(driver))

    proxy_ip = mask_ip(get_proxy_ip(proxy_idx))
    with stage.thread_stats_lock:
        stage.thread_stats[thread_id] = {"done": 0, "state": "OK", "proxy": proxy_ip}

    try:
        while True:
            try:
                task = stage.ready_q.get(timeout=1.0)
            except queue.Empty:
                with stage.state_lock, stage.delayed_lock:
                    nothing_left = stage.ready_q.qsize() == 0 and len(stage.delayed_heap) == 0 and len(stage.in_flight) == 0
                    deferred_empty = stage.deferred_q.qsize() == 0
                if nothing_left and deferred_empty:
                    break
                else:
                    if nothing_left and not deferred_empty:
                        try:
                            while True:
                                t2 = stage.deferred_q.get_nowait()
                                if t2.get("phase") != "Final":
                                    t2 = dict(t2); t2["phase"] = "Final"
                                stage.ready_q.put(t2)
                        except queue.Empty:
                            pass

                    continue

            url      = task["url"]
            intent   = task.get("intent","unknown")
            segment  = task.get("segment","unknown")
            l_id_in  = task.get("listing_id")
            attempt  = task.get("attempt", 1)
            in_final = task.get("phase") == "Final"

            with stage.state_lock:
                if url in stage.done_set or url in stage.in_flight:
                    continue
                stage.in_flight.add(url)

            try:
                driver.set_page_load_timeout(random.uniform(TIMEOUT_MIN, TIMEOUT_MAX))
                driver.get(url)

                # Meter network (once)
                stage.add_net_bytes(drain_network_bytes(driver))

                # Block/invalid detail detection (zero-wait)
                _raw = driver.page_source or ""
                why = looks_blocked_or_invalid_fast(_raw, driver.current_url)
                if why:
                    detection_logger.info(f"[ADVIEW] block_detected:{why} {url}", extra={'thread_id': thread_id})
                    raise RuntimeError(f"adview_block_or_invalid:{why}")

                # Save once
                html = _raw
                base_name = f"adview_{safe_name(intent)}_{safe_name(segment)}_{safe_name(l_id_in or url)}"
                gz_path = save_gz_html(ADVIEW_DIR, base_name, html)

                # Extract once
                row = extract_adview_fields_from_html(html, url)

                # Extraction sanity (no waits)
                if not any([bool(row.get("title")), bool(row.get("state")), bool(row.get("price")), bool(row.get("phone_number"))]):
                    raise RuntimeError("adview_extract_empty:missing_core_fields")

                if not row.get("listing_id") and l_id_in:
                    row["listing_id"] = l_id_in
                row["snapshot_path"] = gz_path

                if not hasattr(stage, "adview_rows"):
                    stage.adview_rows = []; stage.adview_rows_lock = threading.Lock()
                with stage.adview_rows_lock:
                    stage.adview_rows.append(row)

                perf_logger.info(f"[ADVIEW] OK {url}", extra={'thread_id': thread_id})

                with stage.state_lock:
                    stage.metrics["ok"] += 1; stage.metrics["completed"] += 1; stage.done_set.add(url)
                with stage.thread_stats_lock:
                    st = stage.thread_stats[thread_id]; st["done"] += 1; st["state"] = "OK"

                if stage.overall_bar is not None: stage.overall_bar.update(1)
                if stage.thread_bars.get(thread_id): stage.thread_bars[thread_id].update(1)
                time.sleep(random.uniform(3.8, 4.6))

            except Exception as e:
                err_msg = str(e)[:180]
                error_logger.error(f"[ADVIEW] {url} err: {err_msg}", extra={'thread_id': thread_id})

                old_mask = proxy_ip
                try:
                    try: safe_driver_quit(driver)
                    except: pass
                    proxy_idx = stage.rotate_proxy_for_thread(thread_id, proxy_idx)
                    proxy_ip  = mask_ip(get_proxy_ip(proxy_idx))
                    ua = pick_ua()
                    driver = start_driver(ua, proxies[proxy_idx], thread_id, ADVIEW_EXT_ROOT)
                    with stage.thread_stats_lock:
                        stage.thread_stats[thread_id]["state"] = "Restarted"; stage.thread_stats[thread_id]["proxy"] = proxy_ip
                except Exception:
                    pass

                if in_final:
                    with stage.state_lock:
                        stage.metrics["final_exhausted"] += 1; stage.metrics["completed"] += 1; stage.done_set.add(url)
                    with stage.thread_stats_lock:
                        stage.thread_stats[thread_id]["done"] += 1; stage.thread_stats[thread_id]["state"] = "Final Exhausted"

                    audit_append(FAIL_F, {
                        "url": url, "attempts": attempt, "why": f"{type(e).__name__}: {err_msg}",
                        "thread_id": thread_id, "proxy": proxy_ip, "ua_label": f"Chrome/{VERSION_MAIN}",
                        "first_failure_unix": int(time.time()), "last_attempt_unix": int(time.time())
                    })
                else:
                    if attempt == 1:
                        backoff = int(random.uniform(6, 18))
                        with stage.state_lock: stage.metrics["retried"] += 1
                        if retry_bot.enabled:
                            retry_bot.send_event(
                                f"🔁 Retry A • ADVIEW • T{thread_id}\nURL: {url}\nWhy: {type(e).__name__}: {err_msg}\n"
                                f"Fix: Restarted + rotated proxy ({old_mask} → {proxy_ip}); backoff {backoff//60}m{backoff%60:02d}s → reattempt (2/3)"
                            )
                        task["attempt"] = 2; stage.schedule_retry(task, backoff)
                    elif attempt == 2:
                        backoff = int(random.uniform(6, 7))
                        with stage.state_lock: stage.metrics["retried"] += 1
                        if retry_bot.enabled:
                            retry_bot.send_event(
                                f"🔁 Retry B • ADVIEW • T{thread_id}\nURL: {url}\nWhy: {type(e).__name__}: {err_msg}\n"
                                f"Fix: Restarted + rotated proxy ({old_mask} → {proxy_ip}); backoff {backoff//60}m{backoff%60:02d}s → reattempt (3/3)"
                            )
                        task["attempt"] = 3; stage.schedule_retry(task, backoff)
                    else:
                        with stage.state_lock: stage.metrics["deferred"] += 1
                        if url not in stage.deferred_set:
                            stage.deferred_set.add(url)
                            task2 = dict(task); task2["phase"] = "Final"
                            stage.deferred_q.put(task2)
                            audit_append(DEFER_F, {
                                "url": url, "attempts": attempt, "why": f"{type(e).__name__}: {err_msg}",
                                "thread_id": thread_id, "proxy": proxy_ip, "ua_label": f"Chrome/{VERSION_MAIN}",
                                "deferred_unix": int(time.time())
                            })
            finally:
                with stage.state_lock:
                    if url in stage.in_flight: stage.in_flight.discard(url)

    finally:
        try: safe_driver_quit(driver)
        except: pass
        stage.release_proxy(proxy_idx)
        with stage.thread_stats_lock:
            stage.thread_stats[thread_id]["state"] = "finished"


# ====== Compression + Upload (CSV only) ======
def _fmt_mb(n_bytes:int) -> str:
    try:
        return f"{(n_bytes or 0) / (1024*1024):.2f} MB"
    except Exception:
        return "0.00 MB"

def compress_and_upload(csv_path:str, csv_bot: DiscordClient, label:str, adlist_bytes:int=0, adview_bytes:int=0):
    """Uploads CSV (zip or gz) and mentions total network used for both phases."""
    try:
        both_usage = f"Network used — ADLIST: {_fmt_mb(adlist_bytes)} • ADVIEW: {_fmt_mb(adview_bytes)}"

        zip_path = os.path.splitext(csv_path)[0] + ".zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
            zf.write(csv_path, arcname=os.path.basename(csv_path))
        zip_size_mb = os.path.getsize(zip_path) / (1024 * 1024)

        if csv_bot.enabled and zip_size_mb < 9.8:
            csv_bot.send_file(zip_path, content=f"📦 iProperty {label} ZIP • {zip_size_mb:.2f} MB\n{both_usage}")
            return

        gz_path = csv_path + ".gz"
        with open(csv_path, "rb") as fin, gzip.open(gz_path, "wb") as fout:
            shutil.copyfileobj(fin, fout)
        gz_size_mb = os.path.getsize(gz_path) / (1024 * 1024)

        if csv_bot.enabled and gz_size_mb < 9.8:
            csv_bot.send_file(gz_path, content=f"🗜️ iProperty {label} GZIP • {gz_size_mb:.2f} MB\n{both_usage}")
        else:
            if csv_bot.enabled:
                csv_bot.send_event(f"⚠️ {label} CSV too large to send (zip={zip_size_mb:.2f} MB, gzip={gz_size_mb:.2f} MB). Saved locally at: {csv_path}\n{both_usage}")
    except Exception:
        pass

# ====== MAIN ======
if __name__ == "__main__":
    print(f"📁 ADLIST → {ADLIST_DIR}")
    print(f"📁 ADVIEW → {ADVIEW_DIR}")
    print(f"📋 Logs   → {LOG_DIR}")

    dashboard_bot = DiscordClient(DASHBOARD_WEBHOOK)
    retry_bot     = DiscordClient(RETRY_WEBHOOK)
    exhausted_bot = DiscordClient(EXHAUSTED_WEBHOOK)
    csv_bot       = DiscordClient(CSV_WEBHOOK)
    for bot in (dashboard_bot, retry_bot, exhausted_bot, csv_bot): bot.start()

    adlist = Stage("ADLIST", ADLIST_THREADS, ADLIST_EXT_ROOT)
    adview = Stage("ADVIEW", ADVIEW_THREADS, ADVIEW_EXT_ROOT)

    # Seed ADLIST tasks (400 total)
    total_pages = 0
    for cfg in CATEGORIES:
        for p in range(1, cfg["pages"] + 1):
            adlist.ready_q.put({"intent": cfg["intent"], "segment": cfg["segment"], "page": p, "attempt": 1})
            total_pages += 1
    adlist.metrics["total"] = total_pages

    current_phase = {"phase": "ADLIST"}
    stop_event = threading.Event()
    dash_thr = threading.Thread(target=dashboard_loop, args=(stop_event, dashboard_bot, adlist, adview, lambda: current_phase["phase"]), daemon=True)
    dash_thr.start()

    # Progress bars (optional)
    if tqdm is not None:
        adlist.overall_bar = tqdm(total=adlist.metrics["total"], desc="ADLIST Overall", position=0, dynamic_ncols=True)
        fair = math.ceil(adlist.metrics["total"] / max(1, adlist.threads))
        for t in range(adlist.threads):
            adlist.thread_bars[t] = tqdm(total=fair, desc=f"A{t}", position=t+1, dynamic_ncols=True)

    disp_list = threading.Thread(target=dispatcher_loop, args=(adlist, stop_event), daemon=True, name="adlist_dispatcher")
    disp_list.start()

    # Run ADLIST
    with ThreadPoolExecutor(max_workers=adlist.threads) as ex:
        _ = [ex.submit(adlist_worker, i, adlist, retry_bot, exhausted_bot) for i in range(adlist.threads)]
        while True:
            time.sleep(1)
            with adlist.state_lock, adlist.delayed_lock:
                main_done = adlist.ready_q.qsize() == 0 and len(adlist.delayed_heap) == 0 and len(adlist.in_flight) == 0
                deferred_empty = adlist.deferred_q.qsize() == 0
            if main_done and deferred_empty:
                break

    if tqdm is not None:
        if adlist.overall_bar: adlist.overall_bar.close()
        for t in list(adlist.thread_bars.keys()):
            try: adlist.thread_bars[t].close()
            except Exception: pass

    # Build ADLIST CSV
    adlist_csv_path = os.path.join(ADLIST_DIR, f"iP_adlist_{TS}.csv")
    total_rows = 0
    if hasattr(adlist, "adlist_rows") and adlist.adlist_rows:
        df = pd.DataFrame(adlist.adlist_rows)
        if set(["url","intent","segment"]).issubset(df.columns):
            df = df.drop_duplicates(subset=["url","intent","segment"])
        listed_dt_local = pd.to_datetime(df.get("listed_unix"), unit="s", utc=True, errors="coerce") + pd.Timedelta(hours=8)
        scrape_dt_local = pd.to_datetime(df.get("scrape_unix"), unit="s", utc=True, errors="coerce") + pd.Timedelta(hours=8)
        df["listed_date"]     = listed_dt_local.dt.strftime("%Y-%m-%d")
        df["listed_time"]     = listed_dt_local.dt.strftime("%H:%M:%S")
        df["scrape_datetime"] = scrape_dt_local.dt.strftime("%Y-%m-%d %H:%M:%S")
        cols = ["intent","segment","url","title","listed_date","listed_time","scrape_datetime","agent_name","agent_id","listing_id","snapshot_path"]
        for c in cols:
            if c not in df.columns: df[c] = None
        df_final = df[cols]
        df_final.to_csv(adlist_csv_path, index=False, encoding="utf-8-sig")
        total_rows = len(df_final)
    else:
        pd.DataFrame(columns=["intent","segment","url","title","listed_date","listed_time","scrape_datetime","agent_name","agent_id","listing_id","snapshot_path"]).to_csv(adlist_csv_path, index=False, encoding="utf-8-sig")
    print(f"📄 ADLIST CSV written: {adlist_csv_path} (rows: {total_rows})")

    # Upload ADLIST CSV (includes both-phase usage so far; ADVIEW likely 0 at this point)
    compress_and_upload(adlist_csv_path, csv_bot, label="ADLIST", adlist_bytes=adlist.net_bytes, adview_bytes=getattr(adview, "net_bytes", 0))

    # ====== Start ADVIEW ======
    current_phase["phase"] = "ADVIEW"
    
    # 🔁 Refresh retry/exhausted webhooks so they don't go passive between phases
    try:
        retry_bot.stop()
    except Exception:
        pass
    retry_bot = DiscordClient(RETRY_WEBHOOK)
    retry_bot.start()
    print(f"[DEBUG] retry_bot restarted; enabled={retry_bot.enabled}")
    
    try:
        exhausted_bot.stop()
    except Exception:
        pass
    exhausted_bot = DiscordClient(EXHAUSTED_WEBHOOK)
    exhausted_bot.start()
    print(f"[DEBUG] exhausted_bot restarted; enabled={exhausted_bot.enabled}")

    # Prefer fresh initial proxies for ADVIEW
    adview_initial_exclude = set(adlist.initial_proxy_indices) if adlist.initial_proxy_indices else set()
    adview.assign_initial_proxy = lambda thread_id, exclude=None: Stage.assign_initial_proxy(adview, thread_id, exclude=adview_initial_exclude)

    # Queue ADVIEW URLs from ADLIST CSV
    df_in = pd.read_csv(adlist_csv_path)
    adview_urls = 0
    for _, row in df_in.iterrows():
        url = str(row.get("url","")).strip()
        if not url or url == "nan": continue
        url = str(row.get("url","")).strip()
        if not url or url == "nan":
            continue
        # 🚫 Skip URLs with no digits
        if not any(ch.isdigit() for ch in url):
            continue

        task = {
            "url": url,
            "intent": row.get("intent","unknown"),
            "segment": row.get("segment","unknown"),
            "listing_id": row.get("listing_id") if "listing_id" in row else None,
            "attempt": 1
        }
        adview.ready_q.put(task); adview_urls += 1
    adview.metrics["total"] = adview_urls
    print(f"🧾 ADVIEW URLs queued: {adview_urls}")

    if tqdm is not None:
        adview.overall_bar = tqdm(total=adview.metrics["total"], desc="ADVIEW Overall", position=0, dynamic_ncols=True)
        fair = math.ceil(max(1, adview.metrics["total"]) / max(1, adview.threads))
        for t in range(adview.threads):
            adview.thread_bars[t] = tqdm(total=fair, desc=f"V{t}", position=t+1, dynamic_ncols=True)

    disp_view = threading.Thread(target=dispatcher_loop, args=(adview, stop_event), daemon=True, name="adview_dispatcher")
    disp_view.start()

    with ThreadPoolExecutor(max_workers=adview.threads) as ex:
        _ = [ex.submit(adview_worker, i, adview, retry_bot, exhausted_bot) for i in range(adview.threads)]
        while True:
            time.sleep(1)
            with adview.state_lock, adview.delayed_lock:
                main_done = adview.ready_q.qsize() == 0 and len(adview.delayed_heap) == 0 and len(adview.in_flight) == 0
                deferred_empty = adview.deferred_q.qsize() == 0
            if main_done and deferred_empty:
                break

    if tqdm is not None:
        if adview.overall_bar: adview.overall_bar.close()
        for t in list(adview.thread_bars.keys()):
            try: adview.thread_bars[t].close()
            except Exception: pass

    # Stop dashboard
    if dashboard_bot.enabled:
        dashboard_bot.set_dashboard(build_dashboard_text(adlist, adview, current_phase["phase"]))
    stop_event.set()
    try: dash_thr.join(timeout=3)
    except Exception: pass

    # ====== Build FINAL ADVIEW CSV (rich ADVIEW + ADLIST timing/agent) ======
    adview_csv_path = os.path.join(ADVIEW_DIR, f"iP_adview_{TS}.csv")
    total_rows_view = 0

    provided_order = [
        "activate_date",
        "ad_id",
        "agency",
        "build_up",
        "car_park",
        "created_time",
        "currency",
        "email",
        "furnishing",
        "id",
        "land_area",
        "lister",
        "listing_id",
        "location",
        "market",
        "phone",
        "phone_number",
        "phone_number2",
        "posted_date",
        "posted_time",
        "price",
        "property_type",
        "region",
        "ren",
        "rent_sale",
        "rooms",
        "scrape_date",
        "seller_name",
        "source",
        "state",
        "subregion",
        "title",
        "toilets",
        "type",
        "url",
        "updated_date",
    ]

    legacy_order = [
        "url",
        "type",
        "title",
        "currency",
        "price",
        "posted_date",
        "posted_time",
        "rooms",
        "toilets",
        "car_park",
        "phone",
        "lister",
        "agency",
        "furnishing",
        "location",
        "region",
        "subregion",
        "ren",
        "land_area",
    ]

    if hasattr(adview, "adview_rows") and adview.adview_rows:
        seen_urls = set()
        rows_for_csv = []
        for orig_row in adview.adview_rows:
            row = dict(orig_row)
            row.pop("snapshot_path", None)
            url_key = str(row.get("url", "") or "").strip()
            if url_key and url_key in seen_urls:
                continue
            if url_key:
                seen_urls.add(url_key)
            clean_row = {}
            for key, value in row.items():
                if value is None:
                    clean_row[key] = ""
                elif isinstance(value, float) and math.isnan(value):
                    clean_row[key] = ""
                elif isinstance(value, str) and value.strip().lower() == "nan":
                    clean_row[key] = ""
                else:
                    clean_row[key] = str(value)
            for default_key in provided_order:
                clean_row.setdefault(default_key, "")
            if not clean_row.get("ad_id"):
                clean_row["ad_id"] = clean_row.get("listing_id", "")
            rows_for_csv.append(clean_row)

        extra_fields = [field for field in legacy_order if field not in provided_order]
        for row in rows_for_csv:
            for key in row.keys():
                if key not in provided_order and key not in extra_fields:
                    extra_fields.append(key)

        fieldnames = provided_order + extra_fields
        with open(adview_csv_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows_for_csv)
        total_rows_view = len(rows_for_csv)
    else:
        extra_fields = [field for field in legacy_order if field not in provided_order]
        fieldnames = provided_order + extra_fields
        with open(adview_csv_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
        total_rows_view = 0

    print(f"📄 ADVIEW CSV written: {adview_csv_path} (rows: {total_rows_view})")

    try:
        if not os.path.isfile(adview_csv_path):
            error_logger.error(f"[ADVIEW] CSV not found at {adview_csv_path}", extra={'thread_id': 0})
            print(f"[CHECK] MISSING: {adview_csv_path}")
        else:
            size_bytes = os.path.getsize(adview_csv_path)
            print(f"📦 EXISTS: {adview_csv_path}  ({size_bytes:,} bytes)")
    except Exception as _e:
        error_logger.error(f"[ADVIEW] CSV path check failed: {type(_e).__name__}: {_e}", extra={'thread_id': 0})

    # Restart CSV webhook before ADVIEW upload (as in your PG code)
    try:
        csv_bot.stop()
    except Exception:
        pass
    csv_bot = DiscordClient(CSV_WEBHOOK)
    csv_bot.start()
    print(f"[DEBUG] csv_bot restarted before ADVIEW upload; enabled={csv_bot.enabled}")

    # Upload ADVIEW CSV (includes both-phase usage totals)
    compress_and_upload(adview_csv_path, csv_bot, label="ADVIEW", adlist_bytes=adlist.net_bytes, adview_bytes=adview.net_bytes)

    # Close bots
    if dashboard_bot.enabled:
        dashboard_bot.set_dashboard(build_dashboard_text(adlist, adview, current_phase["phase"]))
        time.sleep(0.5)
    dashboard_bot.stop(); retry_bot.stop(); exhausted_bot.stop(); csv_bot.stop()
