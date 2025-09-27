# WalterMart Dasmariñas POC scraper (public pages only).
#
# Usage:
#   python scrape_waltermart_dasma.py
#
# Setup:
#   pip install -r requirements.txt
#   python -m playwright install

import os, asyncio, re, time, pandas as pd, sys
from playwright.async_api import async_playwright
from urllib.parse import urljoin
from datetime import datetime, timezone
from typing import Any, Dict

BASE = "https://www.waltermartdelivery.com.ph"
STORE_URL = f"{BASE}/stores/waltermart-dasmarinas"  # primes the Dasmariñas context

# Public category URLs (no login). You can add a few more later.
CATEGORY_PATHS = [
    "/shop/shop_by_category/food_pantry/canned_goods/d/1496148",   # Canned Goods
    "/shop/shop_by_category/food_pantry/pasta_noodles/d/1496155",  # Pasta & Noodles
    "/shop/shop_by_category/food_pantry/pasta_noodles/instant_noodles/d/22459115",  # Instant Noodles
]

# Friendly user agent (can be overridden via PCHP_UA env var)
USER_AGENT = os.environ.get("PCHP_UA", "PriceCheckPH-POC/0.1 (+contact: you@example.com)")
HEADLESS = os.environ.get("HEADLESS", "1") not in ("0", "false", "False")

# Output directory (defaults to current working dir). docker-compose mounts ./data -> /app/data
OUT_DIR = os.environ.get("OUT_DIR", ".")

os.makedirs(OUT_DIR, exist_ok=True)

def parse_size(text: str):
    if not text:
        return None, None
    m = re.search(r'(\d+(?:\.\d+)?)\s*(g|kg|ml|l)\b', text, re.I)
    if not m:
        return None, None
    val, unit = float(m.group(1)), m.group(2).lower()
    return val, unit

def unit_price(price, size_val, size_unit):
    if price is None or size_val is None or size_unit is None:
        return None
    u = size_unit
    if u == 'kg':
        return price / size_val            # ₱/kg
    if u == 'g':
        return price / (size_val / 1000)   # ₱/kg
    if u == 'l':
        return price / size_val            # ₱/L
    if u == 'ml':
        return price / (size_val / 1000)   # ₱/L
    return None

async def scrape():
    rows = []
    seen = set()  # dedupe by (name, source) or product id

    def _extract_price(obj: Dict[str, Any]):
        # look for common price fields in various shapes
        candidates = []
        if isinstance(obj, dict):
            for k in [
                "price",
                "current_price",
                "sale_price",
                "regular_price",
                "price_in_cents",
            ]:
                if k in obj and isinstance(obj[k], (int, float)):
                    val = obj[k]
                    if k.endswith("_in_cents"):
                        val = val / 100.0
                    candidates.append(val)
            # nested structures
            if "pricing" in obj and isinstance(obj["pricing"], dict):
                for k in ["current", "sale", "regular", "price"]:
                    v = obj["pricing"].get(k)
                    if isinstance(v, (int, float)):
                        candidates.append(v)
                    elif isinstance(v, dict):
                        for kk in ["amount", "value"]:
                            vv = v.get(kk)
                            if isinstance(vv, (int, float)):
                                candidates.append(vv)
        return min(candidates) if candidates else None
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        ctx = await browser.new_context(user_agent=USER_AGENT)
        page = await ctx.new_page()

        # 1) Prime store cookie/context
        await page.goto(STORE_URL, wait_until="domcontentloaded")
        await asyncio.sleep(2)
        # Try basic cookie/consent banners
        try:
            for txt in ["Accept", "I agree", "Allow all", "OK", "Got it", "Continue"]:
                btn = page.get_by_role("button", name=re.compile(txt, re.I))
                if await btn.count():
                    await btn.first.click(timeout=1000)
        except Exception:
            pass

        # 2) Visit categories and collect product links (best-effort selectors)
        for path in CATEGORY_PATHS:
            # Try SPA hashbang filter routes using the numeric id, then other forms
            m_id = re.search(r"/d/(\d+)", path)
            cat_id = m_id.group(1) if m_id else None
            candidates = []
            if cat_id:
                candidates.extend([
                    f"{BASE}/shop#!/?filter=department&id={cat_id}",
                    f"{BASE}/shop#!/?filter=aisle&id={cat_id}",
                    f"{BASE}/shop#!/?filter=category&id={cat_id}",
                ])
            # Hashbang path and legacy path
            candidates.append(BASE + path.replace("/shop/", "/shop#!/", 1))
            candidates.append(urljoin(BASE, path))
            # Safe segment for debug artifacts and response handler closure
            safe_seg = re.sub(r"[^a-zA-Z0-9_-]+", "_", path.strip("/"))[:80]

            # Attach response listener BEFORE navigation so we catch XHRs made during load
            async def _capture_response(response):
                try:
                    url_r = response.url
                    ctype = (response.headers or {}).get("content-type", "")
                    if "application/json" in ctype and any(k in url_r for k in ["freshop", "products", "search", "department", "category", "aisle"]):
                        data = await response.json()
                        # persist raw
                        try:
                            import json
                            fname = os.path.join(OUT_DIR, f"debug_{safe_seg}_{int(time.time()*1000)}.json")
                            with open(fname, "w", encoding="utf-8") as f:
                                json.dump(data, f)
                        except Exception:
                            pass
                        # extract product items heuristically
                        lists = []
                        if isinstance(data, dict):
                            for k, v in data.items():
                                if isinstance(v, list) and v and isinstance(v[0], dict):
                                    lists.append(v)
                                if isinstance(v, dict):
                                    for kk, vv in v.items():
                                        if isinstance(vv, list) and vv and isinstance(vv[0], dict):
                                            lists.append(vv)
                        elif isinstance(data, list) and data and isinstance(data[0], dict):
                            lists.append(data)

                        for lst in lists:
                            for item in lst:
                                try:
                                    name = item.get("name") or item.get("title") or item.get("product_name")
                                    if not name:
                                        continue
                                    price_val = _extract_price(item)
                                    pid = item.get("id") or item.get("product_id") or item.get("sku") or name
                                    key = (str(pid), safe_seg)
                                    if key in seen:
                                        continue
                                    size_val, size_unit = parse_size(name)
                                    rows.append({
                                        "name": name,
                                        "price": price_val,
                                        "size_value": size_val,
                                        "size_unit": size_unit,
                                        "unit_price": unit_price(price_val, size_val, size_unit),
                                        "product_url": item.get("link") or item.get("url") or "",
                                        "branch": "Dasmariñas",
                                        "collected_at": datetime.now(timezone.utc).isoformat(),
                                        "source": url_r,
                                    })
                                    seen.add(key)
                                except Exception:
                                    continue
                except Exception:
                    pass

            page.on("response", _capture_response)

            # Navigate through candidates; give each some time to settle
            for url in candidates:
                await page.goto(url, wait_until="domcontentloaded")
                try:
                    await page.wait_for_load_state("networkidle", timeout=12000)
                except Exception:
                    pass
                await asyncio.sleep(2.5)

            # One more settle phase and an element-based wait for product areas
            try:
                await page.wait_for_load_state("networkidle", timeout=12000)
            except Exception:
                pass
            await asyncio.sleep(3)
            try:
                await page.wait_for_selector(
                    "#products .fp-item, #products [data-product-id], .product-card, .product-item",
                    timeout=10000,
                )
            except Exception:
                pass

            # Auto-scroll to load lazy content
            try:
                last_h = 0
                for _ in range(18):  # cap scroll attempts
                    h = await page.evaluate("document.body.scrollHeight")
                    if h <= last_h:
                        break
                    last_h = h
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await asyncio.sleep(0.9)
            except Exception:
                pass

            # Save debug artifacts per category after scroll/settle
            try:
                html_path = os.path.join(OUT_DIR, f"debug_{safe_seg}.html")
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(await page.content())
            except Exception:
                pass
            try:
                png_path = os.path.join(OUT_DIR, f"debug_{safe_seg}.png")
                await page.screenshot(path=png_path, full_page=True)
            except Exception:
                pass

            # Primary strategy: extract from category product cards directly
            card_selectors = [
                "#products .fp-item",
                "#products [data-product-id]",
                ".fp-product, .fp-item",
                ".product-item",
                ".product-card",
                "[class*='product-card']",
                "li[class*='product']",
                "[data-testid*='product']",
                "article.product, div.product",
            ]
            found_any = False
            for csel in card_selectors:
                cards = page.locator(csel)
                try:
                    # Try to wait for at least one card
                    await cards.first.wait_for(timeout=4000)
                    n = await cards.count()
                except Exception:
                    n = 0
                if n == 0:
                    continue
                print(f"[debug] selector {csel} -> {n} cards", file=sys.stderr)
                for i in range(min(n, 200)):
                    card = cards.nth(i)
                    try:
                        name = (await card.locator("h3, .product-title, [class*='product-name']").first.text_content()) or ""
                        name = name.strip()
                        price_txt = await card.locator(".price, .product-price, [class*='price']").first.text_content()
                        price_val = None
                        if price_txt:
                            digits = re.findall(r"\d+(?:\.\d+)?", price_txt.replace(",", ""))
                            if digits:
                                price_val = float(digits[0])
                        # Link if available
                        href = await card.locator("a[href*='/p/']").first.get_attribute("href")
                        prod_url = urljoin(BASE, href) if href else url

                        size_val, size_unit = parse_size(name)
                        uprice = unit_price(price_val, size_val, size_unit)
                        rows.append({
                            "name": name,
                            "price": price_val,
                            "size_value": size_val,
                            "size_unit": size_unit,
                            "unit_price": uprice,
                            "product_url": prod_url,
                            "branch": "Dasmariñas",
                            "collected_at": datetime.now(timezone.utc).isoformat(),
                            "source": url,
                        })
                        found_any = True
                    except Exception as e:
                        print(f"[warn] card extract: {e}", file=sys.stderr)
                if found_any:
                    break

            # Fallback strategy: visit product pages if we got links
            product_links = set()
            if not found_any:
                link_sel_candidates = [
                    "a:has(.product-item)",
                    ".product-card a",
                    "a[href*='/shop/']:has(h3), a[href*='/shop/']:has(.product-title)",
                    "a.product-item__link, a.product-link",
                    "a[href*='/p/']",
                ]
                for sel in link_sel_candidates:
                    for el in await page.locator(sel).element_handles():
                        href = await el.get_attribute("href")
                        if href and href.startswith("/") and "/p/" in href:
                            product_links.add(urljoin(BASE, href))
                print(f"[debug] product_links collected: {len(product_links)}", file=sys.stderr)

            # 3) Visit each product page (gently)
            if product_links:
                for prod_url in list(product_links)[:60]:  # keep it small for POC
                    prod = await ctx.new_page()
                    try:
                        await prod.goto(prod_url, wait_until="domcontentloaded")
                        try:
                            await prod.wait_for_load_state("networkidle", timeout=6000)
                        except Exception:
                            pass
                        await asyncio.sleep(1.5)

                        # Extract name
                        name = (await prod.locator("h1, .product-title").first.text_content()) or ""
                        name = name.strip()

                        # Extract price text
                        price_txt = await prod.locator(".price, .product-price, [class*='price']").first.text_content()
                        price_val = None
                        if price_txt:
                            digits = re.findall(r"\d+(?:\.\d+)?", price_txt.replace(",", ""))
                            if digits:
                                price_val = float(digits[0])

                        size_val, size_unit = parse_size(name)
                        uprice = unit_price(price_val, size_val, size_unit)

                        rows.append({
                            "name": name,
                            "price": price_val,
                            "size_value": size_val,
                            "size_unit": size_unit,
                            "unit_price": uprice,
                            "product_url": prod_url,
                            "branch": "Dasmariñas",
                            "collected_at": datetime.now(timezone.utc).isoformat(),
                            "source": url  # category page
                        })
                    except Exception as e:
                        print(f"[warn] {prod_url}: {e}", file=sys.stderr)
                    finally:
                        await prod.close()
                        time.sleep(1.0)  # per-product throttle

            time.sleep(2.0)  # per-category throttle
            try:
                page.off("response", _capture_response)
            except Exception:
                pass

        await browser.close()

    df = pd.DataFrame(rows)
    # Handle cases where scrape yields no rows or schema differs
    if df.empty:
        print("[info] No rows scraped; site structure may have changed or selectors are too strict.", file=sys.stderr)
        # Save page HTML snapshot for debugging if available (from last category)
        try:
            html = await page.content()  # type: ignore  # page closed? guard below
        except Exception:
            html = None
        csv_path = os.path.join(OUT_DIR, "waltermart_dasma_sample.csv")
        df.to_csv(csv_path, index=False)
        if html:
            with open(os.path.join(OUT_DIR, "waltermart_last_page.html"), "w", encoding="utf-8") as f:
                f.write(html)
        return

    # Keep only rows with a price if present; otherwise emit diagnostics and proceed
    if "price" in df.columns:
        df = df.dropna(subset=["price"])
    else:
        print("[warn] 'price' column missing in scraped data; writing raw rows for debugging.", file=sys.stderr)
        raw_path = os.path.join(OUT_DIR, "waltermart_dasma_raw.csv")
        df.to_csv(raw_path, index=False)

    # De-dup by product_url or name; keep the lowest price seen
    if "name" in df.columns:
        df = df.sort_values(["name", "price"]).drop_duplicates(subset=["name"], keep="first")

    csv_path = os.path.join(OUT_DIR, "waltermart_dasma_sample.csv")
    db_path = os.path.join(OUT_DIR, "pricecheck.duckdb")

    df.to_csv(csv_path, index=False)

    import duckdb
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE IF NOT EXISTS waltermart_dasma AS SELECT * FROM df LIMIT 0")
    con.execute("INSERT INTO waltermart_dasma SELECT * FROM df")
    print(f"Saved {len(df)} rows to {csv_path} and {db_path}")

if __name__ == "__main__":
    asyncio.run(scrape())