#!/usr/bin/env python3
import argparse
import sys

from scraper import DevForumScraper


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Roblox Developer Forum for SEO content"
    )
    parser.add_argument(
        "--categories",
        "-c",
        nargs="+",
        help="Specific categories to scrape (by name or slug)",
    )
    parser.add_argument(
        "--limit",
        "-l",
        type=int,
        help="Maximum number of topics to scrape",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start fresh, ignore previous scraping state",
    )
    parser.add_argument(
        "--list-categories",
        action="store_true",
        help="List available categories and exit",
    )
    parser.add_argument(
        "--pain-points",
        "-p",
        action="store_true",
        help="Scrape only pain point categories (scripting-support, bug-reports, etc.)",
    )
    parser.add_argument(
        "--analyze",
        "-a",
        action="store_true",
        help="Run pain point analysis after scraping",
    )

    args = parser.parse_args()

    scraper = DevForumScraper()

    try:
        if args.list_categories:
            categories = scraper.get_categories()
            print(f"Found {len(categories)} categories:\n")
            for cat in categories:
                print(f"  {cat.name} ({cat.slug}): {cat.topic_count} topics")
            return 0

        print("Starting Roblox DevForum scraper...")

        if args.pain_points:
            count = scraper.scrape_pain_points(limit=args.limit)
        else:
            count = scraper.scrape(
                categories=args.categories,
                limit=args.limit,
                resume=not args.no_resume,
            )

        if args.analyze and count > 0:
            print("\nRunning pain point analysis...")
            from analyzer import PainPointAnalyzer
            analyzer = PainPointAnalyzer()
            analyzer.analyze()
            analyzer.export_for_seo()
        print(f"\nDone! Scraped {count} topics.")
        return 0

    except KeyboardInterrupt:
        print("\nInterrupted. Progress has been saved.")
        return 1
    finally:
        scraper.close()


if __name__ == "__main__":
    sys.exit(main())
