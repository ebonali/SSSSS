from __future__ import annotations
import argparse
import json
import re
import sys
import time
from typing import Any, List
from collections import deque
from html import unescape
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


USER_AGENT = "Mozilla/5.0 (compatible; ContactScraper/1.0; +https://example.local)"
MAX_BATCH = 100
MAX_PAGES_PER_SITE = 60
REQUEST_TIMEOUT_SECONDS = 12
RETRY_COUNT = 2

EMAIL_RE = re.compile(r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b", re.IGNORECASE)
HREF_RE = re.compile(r"""href\s*=\s*['"]([^'"]+)['"]""", re.IGNORECASE)
META_CONTENT_RE = re.compile(
    r"""<meta[^>]+(?:name|property)\s*=\s*['"][^'"]*['"][^>]*content\s*=\s*['"]([^'"]+)['"][^>]*>""",
    re.IGNORECASE,
)
SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b[^>]*>.*?</\1>", re.IGNORECASE | re.DOTALL)

ASSET_EXTENSIONS = {
    ".css", ".js", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico",
    ".woff", ".woff2", ".ttf", ".otf", ".map", ".zip", ".pdf",
    ".webp", ".avif", ".mp4", ".mp3", ".avi", ".mov",
}

DISPOSABLE_EMAIL_DOMAINS = {
    "example.com", "domain.com", "email.com",
    "wixpress.com", "sentry.io", "sentry-next.wixpress.com",
}

# Social profiles (actual pages/profiles we want to capture)
# NOTE: order matters — more-specific patterns first to avoid substring issues.
SOCIAL_PATTERNS = (
    "linkedin.com",
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "tiktok.com",
    "youtube.com",
    "youtu.be",
    "whatsapp.com",
    "telegram.me",
    "viber.com",
    "pinterest.com",
    # Short domains — checked with boundary awareness in _match_social()
    "//x.com",
    "//t.me",
    ".x.com",
    ".t.me",
    "//wa.me",
    ".wa.me",
    "//vb.me",
    ".vb.me",
)

# Share / intent URLs that are NOT actual social profiles
SOCIAL_SHARE_NOISE = (
    "facebook.com/sharer",
    "facebook.com/share",
    "twitter.com/intent",
    "twitter.com/share",
    "x.com/intent",
    "x.com/share",
    "pinterest.com/pin/create",
    "linkedin.com/shareArticle",
    "linkedin.com/share",
    "wa.me/send",
    "whatsapp.com/send",
    "telegram.me/share",
    "t.me/share",
    "reddit.com/submit",
    "plus.google.com/share",
)

# Patterns in social hrefs that indicate non-profile noise
SOCIAL_HREF_NOISE = (
    "#woo-cart-panel",
    "#account-modal",
    "wp-json",
    "xmlrpc.php",
    "/feed/",
)

PRIORITY_KEYWORDS = ("contact", "about", "impressum", "support", "help", "info")


def normalize_url(raw_url: str) -> str:
    raw_url = raw_url.strip()
    if not raw_url:
        return raw_url
    if not urlparse(raw_url).scheme:
        raw_url = "https://" + raw_url
    return raw_url


def fetch_html(url: str) -> str:
    last_error = None
    for _ in range(RETRY_COUNT + 1):
        try:
            req = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                content_type = (response.headers.get("Content-Type") or "").lower()
                if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
                    return ""
                charset = response.headers.get_content_charset() or "utf-8"
                return response.read().decode(charset, errors="ignore")
        except (HTTPError, URLError, TimeoutError, ValueError) as exc:
            last_error = exc
            time.sleep(0.35)
    raise last_error if last_error else RuntimeError("Unknown fetch error")


def canonical_phone(raw: str) -> str:
    candidate = raw.strip()
    if len(candidate) < 7:
        return ""
    candidate = re.sub(r"[^\d+]", "", candidate)
    if candidate.startswith("00"):
        candidate = "+" + candidate.removeprefix("00")
    if candidate.count("+") > 1:
        return ""
    if "+" in candidate and not candidate.startswith("+"):
        return ""
    digits = re.sub(r"\D", "", candidate)
    if len(digits) < 10 or len(digits) > 15:
        return ""
    if len(set(digits)) == 1:
        return ""
    if candidate.startswith("+"):
        return "+" + digits
    return digits


def is_valid_email(email: str) -> bool:
    email = email.strip().lower()
    if "@" not in email:
        return False
    local, domain = email.rsplit("@", 1)
    if not local or not domain:
        return False
    if domain in DISPOSABLE_EMAIL_DOMAINS:
        return False
    if local in {"john", "user", "test", "email", "noreply", "no-reply"}:
        return False
    # Skip image file-like emails (abc@2x.png etc.)
    if re.search(r"\.(png|jpg|jpeg|gif|svg|webp)$", domain):
        return False
    return True


def _is_social_noise(href: str) -> bool:
    """Return True if the href is a social share/intent link, not a profile."""
    low = href.lower()
    if any(noise in low for noise in SOCIAL_SHARE_NOISE):
        return True
    if any(noise in low for noise in SOCIAL_HREF_NOISE):
        return True
    return False


def extract_from_text(html: str):
    cleaned_html = SCRIPT_STYLE_RE.sub(" ", html)
    text = unescape(cleaned_html)
    emails = {e for e in EMAIL_RE.findall(text) if is_valid_email(e)}

    phones = set()
    for href in HREF_RE.findall(html):
        href_l = href.lower()
        if href_l.startswith("tel:"):
            # Use fixed slicing for safety or removeprefix if case matches
            normalized = canonical_phone(unescape(href.removeprefix("tel:")))
            if normalized:
                phones.add(normalized)
        if href_l.startswith("mailto:"):
            candidate = unescape(href.removeprefix("mailto:")).split("?")[0].strip()
            if is_valid_email(candidate):
                emails.add(candidate)
        if "wa.me/" in href_l or "whatsapp.com/" in href_l:
            last_part = href.rstrip("/").split("/")[-1]
            normalized = canonical_phone(last_part)
            if normalized:
                phones.add(normalized)

    socials = set()
    for href in HREF_RE.findall(html):
        href_lower = href.lower()
        if any(pat in href_lower for pat in SOCIAL_PATTERNS):
            if not _is_social_noise(href):
                socials.add(href.strip())

    for meta_content in META_CONTENT_RE.findall(html):
        for email in EMAIL_RE.findall(meta_content):
            if is_valid_email(email):
                emails.add(email)

    return emails, phones, socials


def same_domain(url: str, base_domain: str) -> bool:
    d1 = urlparse(url).netloc.lower()
    d2 = base_domain.lower()
    if d1 == d2:
        return True
    # Ignore www prefix differences
    d1 = d1.removeprefix("www.")
    d2 = d2.removeprefix("www.")
    return d1 == d2


def clean_link(link: str, current_url: str) -> str:
    absolute = urljoin(current_url, link.strip())
    parsed = urlparse(absolute)
    if parsed.scheme not in ("http", "https"):
        return ""
    return absolute.split("#")[0]


def _is_priority_link(link: str) -> bool:
    path = urlparse(link).path.lower()
    return any(kw in path for kw in PRIORITY_KEYWORDS)


def discover_links(html: str, current_url: str, base_domain: str):
    links = set()
    for href in HREF_RE.findall(html):
        normalized = clean_link(href, current_url)
        if not normalized:
            continue

        parsed = urlparse(normalized)
        path = parsed.path.lower()
        if any(path.endswith(ext) for ext in ASSET_EXTENSIONS):
            continue
        if "add-to-cart=" in normalized:
            continue
        # Skip query-heavy WP customizer links
        if "customize_changeset_uuid" in normalized:
            continue

        if same_domain(normalized, base_domain):
            links.add(normalized)
    return links


def scrape_site(url: str, extract_only=None):
    start_url = normalize_url(url)
    parsed = urlparse(start_url)
    domain = parsed.netloc
    if not domain:
        return {
            "url": start_url,
            "emails": [],
            "phones": [],
            "social_links": [],
            "found_on_pages": [],
        }

    queue = deque([start_url])
    visited = set()
    in_flight = set()
    # Add ThreadPoolExecutor
    import concurrent.futures

    found_emails = set()
    found_phones = set()
    found_socials = set()
    found_pages = set()

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        while (queue or in_flight) and len(visited) < MAX_PAGES_PER_SITE:
            while queue and len(in_flight) < 5 and len(visited) + len(in_flight) < MAX_PAGES_PER_SITE:
                page_url = queue.popleft()
                if page_url in visited or any(f[1] == page_url for f in in_flight):
                    continue
                future = executor.submit(fetch_html, page_url)
                in_flight.add((future, page_url))
                
            if not in_flight:
                break
                
            done, _ = concurrent.futures.wait(
                [f[0] for f in in_flight], return_when=concurrent.futures.FIRST_COMPLETED
            )
            
            for f in done:
                # Find the mapping tuple
                item = next((x for x in in_flight if x[0] == f), None)
                if not item: continue
                in_flight.remove(item)
                page_url = item[1]
                visited.add(page_url)
                
                try:
                    html = f.result()
                except Exception:
                    continue
                if not html:
                    continue

                emails, phones, socials = extract_from_text(html)
                if emails or phones or socials:
                    found_pages.add(page_url)
                found_emails.update(emails)
                found_phones.update(phones)
                found_socials.update(clean_link(s, page_url) or s for s in socials)

                new_links = discover_links(html, page_url, domain)
                priority = []
                normal = []
                for link in new_links:
                    if link in visited or any(fl[1] == link for fl in in_flight):
                        continue
                    if _is_priority_link(link):
                        priority.append(link)
                    else:
                        normal.append(link)

                queue.extendleft(reversed(priority))
                queue.extend(normal)

    result = {
        "url": start_url,
        "emails": sorted(found_emails),
        "phones": sorted(found_phones),
        "social_links": sorted(found_socials),
        "found_on_pages": sorted(found_pages),
    }

    if extract_only:
        extract_set = set(extract_only)
        if "email" not in extract_set and "emails" not in extract_set:
            result["emails"] = []
        if "phone" not in extract_set and "phones" not in extract_set:
            result["phones"] = []
        if (
            "social" not in extract_set
            and "socials" not in extract_set
            and "social_links" not in extract_set
        ):
            result["social_links"] = []

    return result


def split_social_links(social_links):
    grouped = {
        "instagram": set(),
        "facebook": set(),
        "twitter": set(),
        "whatsapp": set(),
        "telegram": set(),
        "linkedin": set(),
        "viber": set(),
        "youtube": set(),
        "tiktok": set(),
        "pinterest": set(),
    }
    for link in social_links or []:
        low = link.lower()
        if "instagram.com" in low:
            grouped["instagram"].add(link)
        elif "facebook.com" in low:
            grouped["facebook"].add(link)
        elif "twitter.com" in low or "//x.com" in low or ".x.com" in low:
            grouped["twitter"].add(link)
        elif "//wa.me" in low or ".wa.me" in low or "whatsapp.com" in low:
            grouped["whatsapp"].add(link)
        elif "telegram.me" in low or "//t.me" in low or ".t.me" in low:
            grouped["telegram"].add(link)
        elif "linkedin.com" in low:
            grouped["linkedin"].add(link)
        elif "viber.com" in low or "//vb.me" in low or ".vb.me" in low:
            grouped["viber"].add(link)
        elif "youtube.com" in low or "youtu.be" in low:
            grouped["youtube"].add(link)
        elif "tiktok.com" in low:
            grouped["tiktok"].add(link)
        elif "pinterest.com" in low:
            grouped["pinterest"].add(link)
    return {k: sorted(v) for k, v in grouped.items()}


def to_endpoint_row(result):
    def dedupe_phone_variants(values):
        unique = []
        normalized = [re.sub(r"\D", "", v) for v in values]
        for i, val in enumerate(values):
            digits = normalized[i]
            drop = False
            for j, other in enumerate(normalized):
                if i == j:
                    continue
                if len(other) > len(digits) and other.endswith(digits):
                    drop = True
                    break
            if not drop:
                unique.append(val)
        return unique

    social = split_social_links(result.get("social_links", []))
    phones = dedupe_phone_variants(result.get("phones", []))
    return {
        "url": result.get("url", ""),
        "phones": ", ".join(phones),
        "emails": ", ".join(result.get("emails", [])),
        "instagram": ", ".join(social["instagram"]),
        "facebook": ", ".join(social["facebook"]),
        "twitter": ", ".join(social["twitter"]),
        "whatsapp": ", ".join(social["whatsapp"]),
        "telegram": ", ".join(social["telegram"]),
        "linkedin": ", ".join(social["linkedin"]),
        "viber": ", ".join(social["viber"]),
        "youtube": ", ".join(social["youtube"]),
        "tiktok": ", ".join(social["tiktok"]),
        "pinterest": ", ".join(social["pinterest"]),
    }


def parse_input(payload: Any) -> List[dict[str, Any]]:
    if isinstance(payload, dict) and "url" in payload:
        return [payload]
    if isinstance(payload, dict) and "urls" in payload and isinstance(payload["urls"], list):
        return [{"url": u} if isinstance(u, str) else u for u in payload["urls"]]
    if isinstance(payload, list):
        return [{"url": p} if isinstance(p, str) else p for p in payload]
    raise ValueError("Input must contain 'url' or 'urls'.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="JSON string input. If omitted, reads stdin.")
    parser.add_argument("--url", help="Single URL (no JSON escaping needed).")
    parser.add_argument("--urls", nargs="+", help="Multiple URLs separated by spaces.")
    parser.add_argument(
        "--extract-only",
        nargs="+",
        help="Optional filter: email phone social_links",
    )
    args = parser.parse_args()

    if args.url or args.urls:
        if args.url:
            data = {"url": args.url}
            if args.extract_only:
                data["extract_only"] = args.extract_only
        else:
            data = {"urls": args.urls}
    else:
        raw = args.input if args.input is not None else sys.stdin.read()
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SystemExit(
                "Invalid JSON input. In PowerShell prefer: "
                "py .\\contact_scraper.py --url https://example.com"
            ) from exc

    jobs = parse_input(data)
    # Use a loop/comprehension rather than slicing if the type checker is confused
    jobs = [job for i, job in enumerate(jobs) if i < MAX_BATCH]

    output = []
    for job in jobs:
        url = job.get("url", "")
        extract_only = job.get("extract_only")
        if isinstance(extract_only, str):
            extract_only = [extract_only]
        if not isinstance(extract_only, list):
            extract_only = None
        output.append(scrape_site(url, extract_only=extract_only))

    if isinstance(data, dict) and "url" in data:
        print(json.dumps(output[0], ensure_ascii=False))
    else:
        print(json.dumps(output, ensure_ascii=False))


if __name__ == "__main__":
    main()
