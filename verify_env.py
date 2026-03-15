#!/usr/bin/env python3
"""Verify trading environment is correctly set up.
Run: conda run -n polybot python verify_env.py
"""

import sys

checks = []

def check(name, fn):
    try:
        result = fn()
        checks.append((name, "OK", result))
    except Exception as e:
        checks.append((name, "FAIL", str(e)))

# Python version
check("python>=3.11", lambda: f"{sys.version_info.major}.{sys.version_info.minor}")

# Core scientific
check("numpy", lambda: __import__("numpy").__version__)
check("pandas", lambda: __import__("pandas").__version__)
check("scipy", lambda: __import__("scipy").__version__)
check("scipy.stats.norm", lambda: str(__import__("scipy.stats").stats.norm.cdf(0)))
check("sklearn", lambda: __import__("sklearn").__version__)
check("statsmodels", lambda: __import__("statsmodels").__version__)

# Data handling
check("orjson", lambda: __import__("orjson").__version__ if hasattr(__import__("orjson"), "__version__") else "installed")
check("requests", lambda: __import__("requests").__version__)
check("websocket-client", lambda: __import__("websocket").__version__ if hasattr(__import__("websocket"), "__version__") else "installed")
check("aiohttp", lambda: __import__("aiohttp").__version__)

# Trading
check("py_clob_client", lambda: "installed" if __import__("py_clob_client") else "installed")
check("ccxt", lambda: __import__("ccxt").__version__)

# Visualization
check("matplotlib", lambda: __import__("matplotlib").__version__)
check("plotly", lambda: __import__("plotly").__version__)

# Extras
check("dotenv", lambda: "installed" if __import__("dotenv") else "installed")
check("ta", lambda: "installed" if __import__("ta") else "installed")

# Print results
print()
print("=" * 55)
print("  POLYBOT ENVIRONMENT VERIFICATION")
print("=" * 55)
for name, status, detail in checks:
    icon = "+" if status == "OK" else "!"
    detail_str = f" ({detail})" if status == "OK" and detail != "installed" else ""
    if status == "FAIL":
        detail_str = f" — {detail}"
    print(f"  [{icon}] {name}: {status}{detail_str}")
print("=" * 55)

ok = sum(1 for _, s, _ in checks if s == "OK")
total = len(checks)
print(f"  {ok}/{total} checks passed")

if ok < total:
    failed = [name for name, s, _ in checks if s == "FAIL"]
    print(f"  Missing: {', '.join(failed)}")
    print(f"  Fix: conda env create -f environment.yml")

print("=" * 55)
print()
