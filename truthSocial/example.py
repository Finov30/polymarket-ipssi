"""
Exemple d'utilisation de l'API Truth Social
"""

from api import TruthSocialAPI
import json

def main():
    # Initialiser l'API (utilise les credentials du .env)
    api = TruthSocialAPI()

    # Authentification
    print("Authentification...")
    api.authenticate()
    print("OK\n")

    # Récupérer un utilisateur
    username = "realDonaldTrump"
    print(f"Recherche de @{username}...")
    user = api.lookup_user(username)

    print(f"Utilisateur: {user['display_name']} (@{user['username']})")
    print(f"Followers: {user['followers_count']:,}")
    print(f"Posts: {user['statuses_count']:,}")

    # Récupérer les posts
    print(f"\nRécupération des 10 derniers posts...")
    posts = api.get_statuses(user["id"], limit=10)

    print(f"\n{'='*60}")
    for i, post in enumerate(posts, 1):
        content = api.clean_html(post.get("content", ""))
        print(f"\n--- Post {i} ({post['created_at']}) ---")
        print(content[:300])
        print(f"[Likes: {post.get('favourites_count', 0):,} | Reblogs: {post.get('reblogs_count', 0):,}]")

    # Sauvegarder en JSON
    data = {"user": user, "posts": posts}
    with open("output.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\n\nDonnées sauvegardées dans output.json")

if __name__ == "__main__":
    main()
