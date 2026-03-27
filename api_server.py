from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from contact_scraper import MAX_BATCH, scrape_site, to_endpoint_row


app = FastAPI(title="Contact Extract API", version="1.0.0")


class ExtractRequest(BaseModel):
    url: Optional[str] = None
    urls: Optional[List[str]] = None
    extract_only: Optional[List[str]] = None


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/extract")
def extract_contacts(payload: ExtractRequest):
    jobs = []
    if payload.url:
        jobs.append(payload.url)
    elif payload.urls:
        jobs.extend(payload.urls)
    else:
        raise HTTPException(status_code=400, detail="Provide 'url' or 'urls'.")

    jobs = [u for u in jobs if isinstance(u, str) and u.strip()][:MAX_BATCH]
    if not jobs:
        raise HTTPException(status_code=400, detail="No valid URL found.")

    results = [scrape_site(url=u, extract_only=payload.extract_only) for u in jobs]
    rows = [to_endpoint_row(r) for r in results]

    if len(rows) == 1:
        return rows[0]
    return {"count": len(rows), "items": rows}
