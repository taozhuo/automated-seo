import time
import requests
from typing import Optional
import config


class DiscourseClient:
    def __init__(
        self,
        base_url: str = config.BASE_URL,
        rate_limit: float = config.RATE_LIMIT_SECONDS,
        timeout: int = config.REQUEST_TIMEOUT,
        max_retries: int = config.MAX_RETRIES,
    ):
        self.base_url = base_url.rstrip("/")
        self.rate_limit = rate_limit
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "DevForumScraper/1.0 (SEO Research Bot)",
            "Accept": "application/json",
        })
        self._last_request_time: float = 0

    def _rate_limit_wait(self) -> None:
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)

    def get(self, endpoint: str, params: Optional[dict] = None) -> Optional[dict]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        for attempt in range(self.max_retries):
            self._rate_limit_wait()
            self._last_request_time = time.time()

            try:
                response = self.session.get(url, params=params, timeout=self.timeout)

                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", 60))
                    print(f"Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                elif response.status_code == 404:
                    return None
                else:
                    print(f"HTTP {response.status_code} for {url}")

            except requests.RequestException as e:
                print(f"Request error (attempt {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)

        return None

    def get_categories(self) -> Optional[dict]:
        return self.get("/categories.json")

    def get_category_topics(
        self, category_slug: str, category_id: int, page: int = 0
    ) -> Optional[dict]:
        return self.get(f"/c/{category_slug}/{category_id}.json", {"page": page})

    def get_topic(self, topic_id: int) -> Optional[dict]:
        return self.get(f"/t/{topic_id}.json")

    def close(self) -> None:
        self.session.close()
