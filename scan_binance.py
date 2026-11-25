#!/usr/bin/env python3
"""Binance Spot candle relay to existing Woodies CCI Cloudflare Worker.

Uses ONLY public market data and sends OHLC candles to authenticated Worker.
No Binance API key, trade execution, or private account access.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BINANCE = "https://data-api.binance.vision/api/v3/klines"
FRAMES = {"5m": 300_000, "15m": 900_000}


def request_json(url, *, method="GET", data=None, key=None, timeout=45):
    headers = {"Accept": "application/json", "User-Agent": "WoodiesCCI-Monitor/2.0"}
    if key:
        headers["Authorization"] = f"Bearer {key}"
    if data is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(data, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            detail = e.read(300).decode("utf-8", errors="replace")
            if e.code in {429, 500, 502, 503, 504} and attempt < 2:
                if e.code == 429:
                    try:
                        pause = max(1, int(e.headers.get("Retry-After", "10")))
                    except ValueError:
                        pause = 10
                    if pause > 45:
                        raise RuntimeError(f"Rate limit: server asks to retry after {pause}s; abort this run") from e
                else:
                    pause = 4 * (attempt + 1)
                print(f"HTTP {e.code} at {urllib.parse.urlsplit(url).netloc}; retry in {pause}s")
                time.sleep(pause)
                continue
            raise RuntimeError(f"HTTP {e.code} from {urllib.parse.urlsplit(url).netloc}: {detail[:250]}") from e
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
                continue
            raise RuntimeError(f"Network error: {e}") from e


def get_candles(symbol, tf):
    url = BINANCE + "?" + urllib.parse.urlencode({
        "symbol": symbol, "interval": tf, "limit": 160
    })
    data = request_json(url)
    if not isinstance(data, list) or len(data) < 20:
        raise ValueError(f"{symbol}/{tf}: not enough Binance candles")
    now_ms = int(time.time() * 1000)
    interval = FRAMES[tf]
    result = []
    for row in data:
        start = int(row[0])
        # Ignore the still-forming candle, including its last-second edge.
        if start + interval + 1000 > now_ms:
            continue
        result.append([start, float(row[2]), float(row[3]), float(row[4])])
    if len(result) < 20:
        raise ValueError(f"{symbol}/{tf}: not enough CLOSED candles")
    return result


def main():
    base_url = os.environ.get("WORKER_URL", "").strip().rstrip("/")
    sync_key = os.environ.get("SYNC_KEY", "")
    if not base_url.startswith("https://") or not base_url.endswith(".workers.dev"):
        raise ValueError("WORKER_URL must be your https://...workers.dev URL")
    if len(sync_key) < 32:
        raise ValueError("SYNC_KEY missing: add the GitHub Actions repository secret")

    config = request_json(f"{base_url}/sync/config", key=sync_key)
    symbols = config.get("symbols", [])
    frames = config.get("frames", [])
    if not isinstance(symbols, list) or not isinstance(frames, list):
        raise ValueError("Worker returned bad config")
    if not symbols or not frames:
        print("No active symbols or timeframes. Nothing to scan.")
        return
    print(f"Watching {len(symbols)} symbols × {len(frames)} frames from Cloudflare /settings")
    errors = []
    for symbol in symbols:
        if not isinstance(symbol, str) or not (symbol.isalnum() and 5 <= len(symbol) <= 18):
            errors.append(f"Bad symbol in config: {symbol!r}")
            continue
        for tf in frames:
            if tf not in FRAMES:
                errors.append(f"Unsupported frame in config: {tf}")
                continue
            try:
                candles = get_candles(symbol, tf)
                result = request_json(
                    f"{base_url}/sync/candles", method="POST",
                    key=sync_key, data={"symbol": symbol, "tf": tf, "candles": candles},
                    timeout=90
                )
                if not result.get("ok"):
                    raise ValueError(f"Worker rejected candles: {result}")
                print(f"{symbol}/{tf}: closed={len(candles)}; result={json.dumps(result, ensure_ascii=False)}")
            except Exception as exc:
                errors.append(f"{symbol}/{tf}: {exc}")
    if errors:
        print("FAILURES (see below):", file=sys.stderr)
        for err in errors:
            print(" -", err, file=sys.stderr)
        raise SystemExit(1)
    print("SUCCESS: All enabled symbols/frames synced")


if __name__ == "__main__":
    main()
