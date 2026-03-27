import asyncio
from typing import Any

from apify import Actor

from contact_scraper import MAX_BATCH, scrape_site, split_social_links


def normalize_extract_only(value: Any):
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(v) for v in value]
    return None


def format_output(result: dict) -> dict:
    """Convert raw scrape result into a clean, flat Apify-friendly row."""
    social = split_social_links(result.get("social_links", []))
    return {
        "url": result.get("url", ""),
        "emails": result.get("emails", []),
        "phones": result.get("phones", []),
        "instagram": social.get("instagram", []),
        "facebook": social.get("facebook", []),
        "twitter": social.get("twitter", []),
        "linkedin": social.get("linkedin", []),
        "youtube": social.get("youtube", []),
        "tiktok": social.get("tiktok", []),
        "whatsapp": social.get("whatsapp", []),
        "telegram": social.get("telegram", []),
        "viber": social.get("viber", []),
        "pinterest": social.get("pinterest", []),
        "found_on_pages": result.get("found_on_pages", []),
    }


async def run_actor() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}

        single_url = actor_input.get("url")
        urls = actor_input.get("urls")
        extract_only = normalize_extract_only(actor_input.get("extract_only"))

        jobs = []
        if isinstance(single_url, str) and single_url.strip():
            jobs.append({"url": single_url, "extract_only": extract_only})
        elif isinstance(urls, list):
            for item in urls:
                if isinstance(item, str) and item.strip():
                    jobs.append({"url": item, "extract_only": extract_only})
        else:
            await Actor.fail(
                status_message=(
                    "Invalid input. Provide either 'url' (string) or 'urls' (array of strings)."
                )
            )
            return

        jobs = [job for i, job in enumerate(jobs) if i < MAX_BATCH]
        results = []
        for i, job in enumerate(jobs, 1):
            Actor.log.info(f"[{i}/{len(jobs)}] Scraping {job['url']} ...")
            result = await asyncio.to_thread(
                scrape_site,
                job["url"],
                job.get("extract_only"),
            )
            row = format_output(result)
            results.append(row)
            await Actor.push_data(row)
            Actor.log.info(
                f"  → emails={len(row['emails'])}, phones={len(row['phones'])}"
            )

        if len(results) == 1:
            await Actor.set_value("OUTPUT", results[0])
        else:
            await Actor.set_value("OUTPUT", results)

        await Actor.exit(status_message=f"Completed {len(results)} URL(s).")


if __name__ == "__main__":
    asyncio.run(run_actor())
