import os
import requests
import pandas as pd

# Loading token from environment variable
TOKEN = os.getenv("GITHUB_TOKEN")
headers = {"Authorization": f"token {TOKEN}"}


base_url = "https://api.github.com"

# Fetch users from Melbourne with over 100 followers
def fetch_users(location="Melbourne", min_followers=100):
    users = []
    url = f"{base_url}/search/users?q=location:{location}+followers:>{min_followers}&per_page=100"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    
    for user in data["items"]:
        user_data = fetch_user_details(user["login"])
        users.append(user_data)
    
    return users

# Fetch detailed user information
def fetch_user_details(username):
    url = f"{base_url}/users/{username}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    user_data = response.json()

    # Process company name (trim, remove leading @, uppercase)
    company = user_data.get("company", "")
    if company:
        company = company.strip().lstrip("@").upper()

    return {
        "login": user_data["login"],
        "name": user_data.get("name", ""),
        "company": company,
        "location": user_data.get("location", ""),
        "email": user_data.get("email", ""),
        "hireable": user_data.get("hireable", ""),
        "bio": user_data.get("bio", ""),
        "public_repos": user_data["public_repos"],
        "followers": user_data["followers"],
        "following": user_data["following"],
        "created_at": user_data["created_at"]
    }

# Fetch repositories for a specific user
def fetch_repositories(username, max_repos=500):
    repositories = []
    url = f"{base_url}/users/{username}/repos?per_page=100"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    repos = response.json()

    for repo in repos[:max_repos]:
        repo_data = {
            "login": username,
            "full_name": repo["full_name"],
            "created_at": repo["created_at"],
            "stargazers_count": repo["stargazers_count"],
            "watchers_count": repo["watchers_count"],
            "language": repo["language"] or "",
            "has_projects": repo["has_projects"],
            "has_wiki": repo["has_wiki"],
            "license_name": repo["license"]["name"] if repo["license"] else ""
        }
        repositories.append(repo_data)

    return repositories

# Save data to CSV
def save_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)

if __name__ == "__main__":
    # Fetch and save users
    users = fetch_users()
    save_to_csv(users, "users.csv")
    
    # Fetch and save repositories
    all_repos = []
    for user in users:
        repos = fetch_repositories(user["login"])
        all_repos.extend(repos)
    save_to_csv(all_repos, "repositories.csv")
