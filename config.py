BASE_URL = "https://devforum.roblox.com"
RATE_LIMIT_SECONDS = 1.0
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
DATA_DIR = "data/raw"
STATE_FILE = "data/scraper_state.json"

# Target categories for SEO pain point extraction
PAIN_POINT_CATEGORIES = [
    {"id": 55, "slug": "scripting-support", "name": "Scripting Support"},
    {"id": 75, "slug": "code-review", "name": "Code Review"},
    {"id": 56, "slug": "building-support", "name": "Building Support"},
    {"id": 10, "slug": "bug-reports", "name": "Bug Reports"},
    {"id": 170, "slug": "feature-requests", "name": "Feature Requests"},
]

# Keywords indicating pain points
PAIN_KEYWORDS = [
    "error", "bug", "issue", "problem", "broken", "not working", "help",
    "stuck", "confused", "can't", "cannot", "won't", "doesn't work",
    "crash", "lag", "slow", "freeze", "fail", "impossible", "difficult",
    "how do i", "how to", "why does", "why is", "what is wrong",
]
