"""
Truth Social API Client
Reverse-engineered API based on Mastodon
"""

from curl_cffi import requests
from dotenv import load_dotenv
import os
import re

load_dotenv()

class TruthSocialAPI:
    BASE_URL = "https://truthsocial.com"
    API_URL = "https://truthsocial.com/api"
    CLIENT_ID = "9X1Fdd-pxNsAgEDNi_SfhJWi8T-vLuV2WVzKIbkTCw4"
    CLIENT_SECRET = "ozF8jzI4968oTKFkEnsBC-UbLPCdrSv0MkXGQu2o_-M"
    IMPERSONATE = "safari15_5"

    def __init__(self, username: str = None, password: str = None):
        self.username = username or os.getenv("TRUTHSOCIAL_USERNAME")
        self.password = password or os.getenv("TRUTHSOCIAL_PASSWORD")
        self.token = os.getenv("TRUTHSOCIAL_TOKEN")
        self._headers = None

    def authenticate(self) -> str:
        """Authenticate and get access token"""
        if self.token:
            return self.token

        payload = {
            "client_id": self.CLIENT_ID,
            "client_secret": self.CLIENT_SECRET,
            "grant_type": "password",
            "username": self.username,
            "password": self.password,
            "redirect_uri": "urn:ietf:wg:oauth:2.0:oob",
            "scope": "read",
        }

        resp = requests.post(
            f"{self.BASE_URL}/oauth/token",
            json=payload,
            impersonate=self.IMPERSONATE
        )

        if resp.status_code != 200:
            raise Exception(f"Authentication failed: {resp.status_code}")

        self.token = resp.json()["access_token"]
        return self.token

    @property
    def headers(self) -> dict:
        if not self._headers:
            if not self.token:
                self.authenticate()
            self._headers = {
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
            }
        return self._headers

    def _get(self, endpoint: str, params: dict = None) -> dict:
        """Make authenticated GET request"""
        resp = requests.get(
            f"{self.API_URL}{endpoint}",
            headers=self.headers,
            params=params,
            impersonate=self.IMPERSONATE
        )
        if resp.status_code != 200:
            raise Exception(f"Request failed: {resp.status_code} - {resp.text[:200]}")
        return resp.json()

    def lookup_user(self, username: str) -> dict:
        """Lookup user by username"""
        return self._get(f"/v1/accounts/lookup", {"acct": username})

    def get_user(self, user_id: str) -> dict:
        """Get user by ID"""
        return self._get(f"/v1/accounts/{user_id}")

    def get_statuses(self, user_id: str, limit: int = 20, exclude_replies: bool = True) -> list:
        """Get user's statuses/posts"""
        params = {"limit": limit}
        if exclude_replies:
            params["exclude_replies"] = "true"
        return self._get(f"/v1/accounts/{user_id}/statuses", params)

    def get_user_statuses(self, username: str, limit: int = 20) -> list:
        """Get statuses by username (convenience method)"""
        user = self.lookup_user(username)
        return self.get_statuses(user["id"], limit)

    def search(self, query: str, search_type: str = "statuses", limit: int = 20) -> dict:
        """Search for users, statuses, or hashtags"""
        return self._get("/v2/search", {"q": query, "type": search_type, "limit": limit})

    def get_trending(self, limit: int = 20) -> list:
        """Get trending posts"""
        return self._get("/v1/trends/statuses", {"limit": limit})

    def get_trending_tags(self, limit: int = 20) -> list:
        """Get trending hashtags"""
        return self._get("/v1/trends/tags", {"limit": limit})

    @staticmethod
    def clean_html(content: str) -> str:
        """Remove HTML tags from content"""
        content = re.sub(r'<br\s*/?>', '\n', content)
        content = re.sub(r'<[^>]+>', '', content)
        content = content.replace('&amp;', '&')
        content = content.replace('&lt;', '<')
        content = content.replace('&gt;', '>')
        content = content.replace('&#39;', "'")
        content = content.replace('&quot;', '"')
        content = content.replace('&nbsp;', ' ')
        return content.strip()
