"""
Truth Social API Client
"""

import os
import json
import re
from curl_cffi import requests
from dotenv import load_dotenv
from datetime import datetime
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

    def get_statuses(self, user_id: str, limit: int = 20, exclude_replies: bool = True) -> list:
        """Get user's statuses/posts"""
        params = {"limit": limit}
        if exclude_replies:
            params["exclude_replies"] = "true"
        return self._get(f"/v1/accounts/{user_id}/statuses", params)

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


def main():
    """Ingestion autonome TruthSocial"""
    api = TruthSocialAPI()
    print("Authentification...")
    api.authenticate()
    print("OK\n")

    username = "realDonaldTrump" #on prend que Trump pour l'instant, mais on peut étendre apres

    print(f"Récupération de @{username}...")
    user = api.lookup_user(username)
    print(f"Utilisateur: {user['display_name']} (@{user['username']})")
    print(f"Followers: {user['followers_count']:,}")
    print(f"Posts: {user['statuses_count']:,}")

    print(f"\nRécupération des 10 derniers posts...")
    posts = api.get_statuses(user["id"], limit=100)

    base_path = "data/raw/truthsocial"
    now = datetime.now()
    output_dir = os.path.join(
        base_path,
        f"date={now:%Y-%m-%d}",
        f"hour={now:%H}"
    )
    os.makedirs(output_dir, exist_ok=True)

    filename = f"truthsocial_{username}_{now:%Y%m%d_%H%M%S}.jsonl"
    output_path = os.path.join(output_dir, filename)

    if not posts:
        print("[SAVE] Aucun post à sauvegarder")
        return

    with open(output_path, "a", encoding="utf-8") as f:
        for post in posts:
            record = {
                "ingestion_ts": now.isoformat(),
                "username": username,
                "user_id": user["id"],
                "post_id": post["id"],
                "created_at": post["created_at"],
                "raw": post
            }
            json.dump(record, f, ensure_ascii=False)
            f.write("\n")

    print(f"[SAVE] {len(posts)} posts sauvegardés → {output_path}")


if __name__ == "__main__":
    main()
