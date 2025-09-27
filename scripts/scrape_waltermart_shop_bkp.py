# /scripts/scrape_waltermart_shop.py
from __future__ import annotations

import csv, os, re, time, json, math, argparse, random
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

from playwright.sync_api import sync_playwright, Page, Response

BASE = "https://www.waltermartdelivery.com.ph"
STORE_LOCATOR_URL = f"{BASE}/my-store/store-locator#!/?distance=10&q=dasma"
TARGET_STORE_REGEX = r"Dasmari(?:ñas|nas)"  # robust to ñ/na
START_URL = f"{BASE}/shop#!/?limit=48&sort=name&page=1"
MAX_PAGES = int(os.getenv("WM_MAX_PAGES", "50"))  # cap navigation pages

# Debug / tunable environment overrides
DEFAULT_STAGNANT_LIMIT = int(os.getenv("WM_STAGNANT_LIMIT", "4"))  # higher than prev 2
DEFAULT_SCROLL_CYCLES = int(os.getenv("WM_SCROLL_CYCLES", "20"))
DEFAULT_SCROLL_WAIT_MS = int(os.getenv("WM_SCROLL_WAIT_MS", "750"))
DOM_NODE_CAP_DEFAULT = int(os.getenv("WM_DOM_NODE_CAP", "1000"))
RETRY_WAIT_BASE = float(os.getenv("WM_RETRY_WAIT_BASE", "1.0"))
MAX_RETRIES = int(os.getenv("WM_MAX_RETRIES", "3"))

# Optional fallbacks (kept)
FALLBACK_FILTERS = [
    f"{BASE}/shop#!/?filter=product_tag&tag_id=22641182&limit=48&sort=name&page=1",
    f"{BASE}/shop#!/?filter=department&id=1496148&limit=48&sort=name&page=1",
]

ROOT = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)
RAW_DIR = os.path.join(DATA_DIR, "raw")
os.makedirs(RAW_DIR, exist_ok=True)

USER_AGENT = os.getenv("PCHP_UA") or None
HEADLESS = os.getenv("PCHP_HEADLESS", "1") not in ("0", "false", "False")

# -------------- Logging helpers --------------

def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def dump_json(obj: Any, path: str):
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f"[dump-fail] {path}: {e}")

@dataclass
class Product:
    id: str
    name: str
    price: Optional[float]
    url: str
    source: str
    raw_source_page: Optional[int] = None
    raw_api_page: Optional[int] = None

def _norm_price(v: Any) -> Optional[float]:
    if v is None: return None
    try:
        if isinstance(v, (int, float)): return float(v)
        s = re.sub(r"[^\d.,]", "", str(v)).replace(",", "")
        return float(s) if s else None
    except Exception:
        return None

def _find_price_fields(obj: Dict[str, Any]) -> Optional[float]:
    if not isinstance(obj, dict): return None
    cands: List[float] = []
    # common keys + a few extras used by some Freshop tenants
    for k in ["price","current_price","sale_price","regular_price","price_in_cents",
              "store_price","unit_price","promo_price","retail","priceMin","priceMax"]:
        if k in obj and isinstance(obj[k], (int, float)):
            val = float(obj[k])
            if k.endswith("_in_cents"): val /= 100.0
            cands.append(val)
    pricing = obj.get("pricing")
    if isinstance(pricing, dict):
        for k in ["current","sale","regular","price"]:
            v = pricing.get(k)
            if isinstance(v, (int,float)): cands.append(float(v))
            elif isinstance(v, dict):
                for kk in ["amount","value"]:
                    vv = v.get(kk)
                    if isinstance(vv,(int,float)): cands.append(float(vv))
    return min(cands) if cands else None

def _extract_lists(data: Any) -> Iterable[List[Dict[str, Any]]]:
    if isinstance(data, list) and (not data or isinstance(data[0], dict)):
        yield data  # type: ignore
    if isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                yield v
            elif isinstance(v, dict):
                for vv in v.values():
                    if isinstance(vv, list) and vv and isinstance(vv[0], dict):
                        yield vv

def _set_query_param(url: str, key: str, value: str) -> str:
    p = urlparse(url); frag = p.fragment or ""
    if frag.startswith("!/"):
        before, after = (frag.split("?",1)+[""])[:2] if "?" in frag else (frag,"")
        fq = dict(parse_qsl(after, keep_blank_values=True)) if after else {}
        fq[key] = value
        new_frag = before + "?" + urlencode(fq, doseq=True)
        return urlunparse((p.scheme,p.netloc,p.path,p.params,p.query,new_frag))
    q = dict(parse_qsl(p.query, keep_blank_values=True)); q[key] = value
    return urlunparse((p.scheme,p.netloc,p.path,p.params,urlencode(q,doseq=True),p.fragment))

def _is_products_endpoint(url: str) -> bool:
    try:
        p = urlparse(url)
        path = p.path.lower()
        # Prefer concrete product listing endpoints
        return "/products" in path or "/catalog" in path or "/search" in path
    except Exception:
        return False

def _dom_collect(page: Page, max_nodes: int, allow_no_price: bool, source_page: int) -> List[Product]:
    collected: List[Product] = []
    container_sel = "#products > div.container-fluid.fp-container-results.fp-has-total > div.fp-result-list-wrapper > div.fp-result-list-content"
    name_sel = ".fp-item-name a, .fp-item-name, h3, .product-title, .product-name, [class*='product-name']"
    price_sel = ".fp-item-price, .price, .product-price, [class*='price']"
    nodes = []
    # Prefer querying items inside the known container
    try:
        container = page.query_selector(container_sel)
        if container:
            nodes = container.query_selector_all("ul > li, li, .fp-resultitem, [role='listitem']")
    except Exception:
        nodes = []
    if not nodes:
        for s in [
            f"{container_sel} > ul > li",
            "#products .fp-item",
            "#products [data-product-id]",
            "#products .product-list .product",
            ".product-card",
            ".product-item",
            "li.product",
            "div.product",
            "[data-testid*='product']",
        ]:
            try:
                nodes = page.query_selector_all(s)
                if nodes:
                    break
            except Exception:
                continue
    # Remove cap slice logic; we will slice after gathering if needed
    for idx, el in enumerate(nodes):
        if idx >= max_nodes:
            break
        try:
            name = (el.query_selector(name_sel).inner_text().strip()) if el.query_selector(name_sel) else el.inner_text().strip()[:160]
            price_txt = el.query_selector(price_sel).inner_text().strip() if el.query_selector(price_sel) else None
            price = _norm_price(price_txt)
            if price is None and not allow_no_price:
                continue
            href_el = el.query_selector("a[href]")
            href = href_el.get_attribute("href") if href_el else ""
            url = href if href and href.startswith("http") else (BASE + href if href else "")
            pid = None
            if url:
                m = re.search(r"/(\d{5,})(?:/|$)", url)
                if m: pid = m.group(1)
            if not pid: pid = re.sub(r"\s+"," ",name)[:64]
            collected.append(Product(id=str(pid), name=name, price=price, url=url, source="dom", raw_source_page=source_page))
        except Exception:
            continue
    return collected

def _walk_pages(page: Page, start_url: str, products: List[Product], seen: set[Tuple[str, str]], start_page: int, end_page: int, stagnant_limit: int, max_dom_nodes: int, allow_no_price: bool, debug: bool):
    total_before = len(products)
    stagnant_rounds = 0
    for page_idx in range(max(1, start_page), max(1, end_page) + 1):
        url = _set_query_param(start_url, "page", str(page_idx))
        log(f"[nav] Page {page_idx}: {url}")
        try:
            page.goto(url, timeout=90000, wait_until="domcontentloaded")
        except Exception as e:
            log(f"[nav-error] {page_idx}: {e}")
            stagnant_rounds += 1
            if stagnant_rounds >= stagnant_limit:
                log("[break] DOM walk stagnation limit reached (nav errors)")
                break
            continue
        try:
            page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            pass
        # Enhanced scrolling: keep scrolling until no growth for 3 cycles or cap cycles
        last_h = 0
        stable = 0
        for cycle in range(DEFAULT_SCROLL_CYCLES):
            try:
                h = page.evaluate("document.body.scrollHeight")
            except Exception:
                break
            if h <= last_h:
                stable += 1
            else:
                stable = 0
            last_h = h
            if stable >= 3:
                break
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(DEFAULT_SCROLL_WAIT_MS)
        # Give time for trailing XHR
        page.wait_for_timeout(1200)
        # DOM fallback per page
        try:
            dom_items = _dom_collect(page, max_dom_nodes, allow_no_price, page_idx)
            added_this_page = 0
            for it in dom_items:
                key = (it.id, it.url)
                if key not in seen and it.name:
                    products.append(it)
                    seen.add(key)
                    added_this_page += 1
            log(f"[dom] page={page_idx} added={added_this_page} total={len(products)}")
        except Exception as e:
            log(f"[dom-error] page={page_idx} {e}")
        if debug:
            try:
                open(os.path.join(RAW_DIR, f"page_{page_idx}.html"), "w", encoding="utf-8").write(page.content())
            except Exception:
                pass
        if len(products) == total_before:
            stagnant_rounds += 1
        else:
            stagnant_rounds = 0
        total_before = len(products)
        if stagnant_rounds >= stagnant_limit:
            log("[break] DOM walk stagnation limit reached (no new products)")
            break

# ----------------- NEW: force store via Store Locator -----------------
def _select_store_via_locator(page: Page) -> bool:
    """
    Go to the Store Locator with q=dasma, open the Dasmariñas store page,
    click 'Shop this store' / 'Make this my store', and verify selection.
    Returns True if we believe the store context is set.
    """
    print(f"[store] Opening locator: {STORE_LOCATOR_URL}")
    page.goto(STORE_LOCATOR_URL, wait_until="domcontentloaded", timeout=90000)
    try: page.wait_for_load_state("networkidle", timeout=15000)
    except Exception: pass

    # Find the Dasmariñas store link/heading and open its details page
    found = False
    for finder in [
        lambda: page.get_by_role("link", name=re.compile(TARGET_STORE_REGEX, re.I)).first,
        lambda: page.get_by_role("heading", name=re.compile(TARGET_STORE_REGEX, re.I)).first,
        lambda: page.locator(f"text=/{TARGET_STORE_REGEX}/i").first,
    ]:
        try:
            loc = finder()
            if loc and loc.is_visible():
                loc.click(timeout=4000)
                found = True
                break
        except Exception:
            continue
    if not found:
        print("[store] Could not locate Dasmariñas card/link on locator page.")
        return False

    # On the store page, click any of the known CTAs
    try: page.wait_for_load_state("networkidle", timeout=10000)
    except Exception: pass
    clicked = False
    for label in ["Shop this store","Make this my store","Select Store","Shop Now","Start Shopping"]:
        try:
            btn = page.get_by_role("button", name=re.compile(label, re.I)).first
            if btn and btn.is_visible():
                btn.click(timeout=4000)
                clicked = True
                break
        except Exception:
            continue
    # Sometimes CTAs are links
    if not clicked:
        for label in ["Shop this store","Make this my store","Select Store","Shop Now","Start Shopping","Shop"]:
            try:
                link = page.get_by_role("link", name=re.compile(label, re.I)).first
                if link and link.is_visible():
                    link.click(timeout=4000)
                    clicked = True
                    break
            except Exception:
                continue

    if not clicked:
        print("[store] No store CTA found on store page; selection may have auto-applied. Proceeding.")
    page.wait_for_timeout(1200)

    # Soft verification (cookies/localStorage + header text)
    try:
        ls_keys = page.evaluate("() => Object.keys(localStorage)")
        print(f"[store] localStorage keys: {ls_keys}")
    except Exception: pass
    try:
        header_text = page.locator("header, [role='banner']").inner_text()
        if re.search(TARGET_STORE_REGEX, header_text or "", re.I):
            print("[store] Header reflects Dasmariñas.")
            return True
    except Exception: pass
    # If we got pushed to /shop, assume success
    if "/shop" in page.url:
        print("[store] Navigated to Shop after selection.")
        return True
    return True  # most tenants set a cookie even if UI didn't change immediately

# ----------------------------------------------------------------------

# Main scraper logic
# input: optional start_page, end_page (1-indexed, inclusive) and output CSV file name
def scrape(start_page: Optional[int] = None, end_page: Optional[int] = None, out_csv: Optional[str] = None, debug: bool = False, stagnant_limit: int = DEFAULT_STAGNANT_LIMIT, max_dom_nodes: int = DOM_NODE_CAP_DEFAULT, allow_no_price: bool = False):
    if out_csv is None:
        out_csv = os.path.join(DATA_DIR, "waltermart_shop.csv")
    html_snap = os.path.join(DATA_DIR, "waltermart_shop_last.html")
    png_snap = os.path.join(DATA_DIR, "waltermart_shop_last.png")

    products: List[Product] = []
    seen: set[Tuple[str, str]] = set()
    first_api_url: Optional[str] = None
    api_pages_fetched = 0
    break_reason = None

    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=os.path.join(DATA_DIR, ".pw_state"),
            headless=HEADLESS,
            user_agent=USER_AGENT,
        ) if USER_AGENT else pw.chromium.launch_persistent_context(
            user_data_dir=os.path.join(DATA_DIR, ".pw_state"),
            headless=HEADLESS,
        )
        page = ctx.new_page()

        # Store selection (retry up to 2 times if verification fails)
        for attempt in range(1, 3):
            try:
                from_existing = attempt > 1
                log(f"[store] attempt={attempt}")
                _select_store_via_locator(page)
                if _verify_store(page, TARGET_STORE_REGEX):
                    break
                else:
                    page.wait_for_timeout(1500)
            except Exception as e:
                log(f"[store-error] {e}")
        else:
            log("[store-fail] proceeding without confirmed store")

        def on_response(resp: Response):
            nonlocal first_api_url
            try:
                u = resp.url or ""
                if not any(k in u for k in ("freshop","ncrcloud","wp-json","products","search","browse","catalog","items")):
                    return
                if first_api_url is None and _is_products_endpoint(u):
                    first_api_url = u
                    log(f"[api-seed] {first_api_url}")
                # Parse JSON
                try:
                    data = resp.json()
                except Exception:
                    if debug:
                        log(f"[json-skip] {u} ({resp.status})")
                    return
                batch_items = 0
                for lst in _extract_lists(data):
                    for item in lst:
                        if not isinstance(item, dict):
                            continue
                        name = item.get("name") or item.get("title") or item.get("product_name")
                        if not name:
                            continue
                        pid = item.get("id") or item.get("product_id") or item.get("sku") or name
                        price = _find_price_fields(item)
                        if price is None and not allow_no_price:
                            continue
                        link = item.get("link") or item.get("url") or ""
                        if link and link.startswith("/"):
                            link = BASE + link
                        key = (str(pid), link)
                        if key in seen:
                            continue
                        products.append(Product(id=str(pid), name=str(name).strip(), price=price, url=link, source=u))
                        seen.add(key)
                        batch_items += 1
                if batch_items:
                    log(f"[cap] {u} +{batch_items} total={len(products)})")
                if debug and batch_items:
                    # Save raw capture keyed by hash or incremental id
                    hname = re.sub(r"[^a-zA-Z0-9]+", "_", u)[:120]
                    dump_json(data, os.path.join(RAW_DIR, f"resp_{int(time.time()*1000)}_{hname}.json"))
            except Exception:
                pass
        page.on("response", on_response)

        log(f"[info] Visiting {START_URL}")
        page.goto(START_URL, wait_until="domcontentloaded", timeout=90000)
        try: page.wait_for_load_state("networkidle", timeout=20000)
        except Exception: pass

        # Initial dynamic scroll to seed responses
        last_total = len(products)
        for cycle in range(DEFAULT_SCROLL_CYCLES):
            try:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            except Exception:
                break
            page.wait_for_timeout(DEFAULT_SCROLL_WAIT_MS)
            if len(products) == last_total:
                # allow a grace idle cycle
                if cycle > 4:  # after some initial cycles
                    break
            else:
                last_total = len(products)
        log(f"[seed] initial products captured={len(products)} first_api_url={first_api_url}")

        eff_start = max(1, int(start_page) if start_page is not None else 1)
        eff_end = max(eff_start, int(end_page) if end_page is not None else MAX_PAGES)
        log(f"[cfg] Page window: {eff_start}..{eff_end} (MAX_PAGES={MAX_PAGES})")

        # Explicit API pagination if we have a seed url
        if first_api_url:
            def bump_page(u: str, n: int) -> str:
                v = _set_query_param(u, "page", str(n))
                if v == u and "offset=" in u:
                    return re.sub(r"(offset=)(\d+)", lambda m: f"{m.group(1)}{(n-1)*48}", u)
                return v
            stagnant = 0
            for i in range(eff_start, eff_end + 1):
                api_page = bump_page(first_api_url, i)
                retries = 0
                page_added = 0
                while retries <= MAX_RETRIES:
                    try:
                        r = page.request.get(api_page, timeout=45000)
                        if not r.ok:
                            raise RuntimeError(f"status {r.status}")
                        try:
                            data = r.json()
                        except Exception:
                            raise RuntimeError("json parse")
                        before = len(products)
                        batch_items = 0
                        for lst in _extract_lists(data):
                            for item in lst:
                                if not isinstance(item, dict):
                                    continue
                                name = item.get("name") or item.get("title") or item.get("product_name")
                                if not name:
                                    continue
                                pid = item.get("id") or item.get("product_id") or item.get("sku") or name
                                price = _find_price_fields(item)
                                if price is None and not allow_no_price:
                                    continue
                                link = item.get("link") or item.get("url") or ""
                                if link and link.startswith("/"):
                                    link = BASE + link
                                key = (str(pid), link)
                                if key in seen:
                                    continue
                                prod = Product(id=str(pid), name=str(name).strip(), price=price, url=link, source=api_page, raw_api_page=i)
                                products.append(prod)
                                seen.add(key)
                                batch_items += 1
                        if debug:
                            dump_json(data, os.path.join(RAW_DIR, f"api_page_{i}.json"))
                        page_added = len(products) - before
                        log(f"[api] page={i} added={page_added} total={len(products)} url={api_page}")
                        stagnant = stagnant + 1 if page_added == 0 else 0
                        api_pages_fetched += 1
                        break
                    except Exception as e:
                        if retries < MAX_RETRIES:
                            sleep_for = RETRY_WAIT_BASE * (2 ** retries) + random.uniform(0, 0.25)
                            log(f"[api-retry] page={i} attempt={retries+1} err={e} wait={sleep_for:.1f}s")
                            time.sleep(sleep_for)
                            retries += 1
                            continue
                        else:
                            log(f"[api-fail] page={i} giving up: {e}")
                            stagnant += 1
                            break
                if stagnant >= stagnant_limit:
                    break_reason = "api-stagnation"
                    log("[break] API stagnation limit reached")
                    break
        else:
            log("[warn] No API seed URL found; relying on DOM walk.")

        # Walk DOM pages
        pre_dom_total = len(products)
        _walk_pages(page, START_URL, products, seen, eff_start, eff_end, stagnant_limit, max_dom_nodes, allow_no_price, debug)
        if break_reason is None and len(products) == pre_dom_total:
            break_reason = "dom-no-growth"

        if not products:
            log("[warn] No products from API or DOM; final DOM attempt...")
            page.goto(START_URL, timeout=90000, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=20000)
            except Exception:
                pass
            products = _dom_collect(page, max_dom_nodes, allow_no_price, 1)
            if products:
                break_reason = "fallback-dom"

        try:
            open(html_snap, "w", encoding="utf-8").write(page.content())
        except Exception:
            pass
        try:
            page.screenshot(path=png_snap, full_page=True)
        except Exception:
            pass
        try:
            page.off("response", on_response)
        except Exception:
            pass
        ctx.close()

    # Deduplicate
    dedup: Dict[Tuple[str,str], Product] = {}
    for p in products:
        if not p.name:
            continue
        if p.price is None and not allow_no_price:
            continue
        dedup[(p.id, p.url)] = p
    rows = list(dedup.values())
    with open(out_csv, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["id","name","price","url","source","raw_source_page","raw_api_page"])
        w.writeheader()
        for p in rows: w.writerow(asdict(p))
    log(f"[done] Saved {len(rows)} products to {out_csv}. api_pages={api_pages_fetched} break_reason={break_reason}")
    if debug:
        meta = {
            "total_rows": len(rows),
            "api_pages": api_pages_fetched,
            "break_reason": break_reason,
            "stagnant_limit": stagnant_limit,
            "max_dom_nodes": max_dom_nodes,
            "allow_no_price": allow_no_price,
            "timestamp": time.time(),
        }
        dump_json(meta, os.path.join(RAW_DIR, "run_meta.json"))

def _verify_store(page: Page, expect_regex: str) -> bool:
    """Attempt to verify store context using localStorage keys or visible header text."""
    try:
        store_name = page.evaluate("() => (localStorage.getItem('storeName') || localStorage.getItem('store_name') || '')")
        if store_name and re.search(expect_regex, store_name, re.I):
            log(f"[store-ok] localStorage store_name={store_name!r}")
            return True
    except Exception:
        pass
    # Fallback: scan body text (avoid huge memory by limiting)
    try:
        header_text = page.locator("header, [role='banner']").inner_text(timeout=3000)
        if re.search(expect_regex, header_text or "", re.I):
            log("[store-ok] header text matches target store")
            return True
    except Exception:
        pass
    log("[store-warn] Could not confirm store selection.")
    return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WalterMart shop scraper with page range + debug support")
    parser.add_argument("-p", "--pages", help="Page range like '1-5' or single page '3'", default=None)
    parser.add_argument("--start", type=int, help="Start page (1-indexed)", default=None)
    parser.add_argument("--end", type=int, help="End page (inclusive)", default=None)
    parser.add_argument("-o", "--output", help="Output CSV filename (relative stored under data/)", default=None)
    parser.add_argument("--debug", action="store_true", help="Enable verbose debug logging & raw captures")
    parser.add_argument("--stagnant-limit", type=int, default=DEFAULT_STAGNANT_LIMIT, help="Consecutive stagnant pages before break (API / DOM)")
    parser.add_argument("--max-dom-nodes", type=int, default=DOM_NODE_CAP_DEFAULT, help="Max DOM product nodes to inspect per page")
    parser.add_argument("--include-no-price", action="store_true", help="Include products even if price missing")
    args = parser.parse_args()

    sp: Optional[int] = None
    ep: Optional[int] = None
    if args.pages:
        try:
            s = args.pages.strip()
            if "-" in s:
                a, b = s.split("-", 1)
                sp = int(a) if a.strip() else 1
                ep = int(b) if b.strip() else MAX_PAGES
            else:
                sp = int(s)
                ep = sp
        except Exception:
            pass
    if args.start is not None:
        sp = args.start
    if args.end is not None:
        ep = args.end

    out_path = None
    if args.output:
        out_path = args.output
        if not os.path.isabs(out_path):
            out_path = os.path.join(DATA_DIR, out_path)

    scrape(sp, ep, out_path, debug=args.debug, stagnant_limit=args.stagnant_limit, max_dom_nodes=args.max_dom_nodes, allow_no_price=args.include_no_price)