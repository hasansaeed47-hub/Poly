"""
Entry point: python -m weatherbot [--live]
"""

import argparse
from weatherbot.bot import WeatherBot


def main():
    parser = argparse.ArgumentParser(
        description="Weather Bot v6 -- Polymarket temperature trading",
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Run in live mode (requires POLY_API_KEY, POLY_API_SECRET, POLY_API_PASSPHRASE)",
    )
    args = parser.parse_args()
    WeatherBot(paper=not args.live).run()


if __name__ == "__main__":
    main()
