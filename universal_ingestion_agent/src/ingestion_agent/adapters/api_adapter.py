# adapters/api_adapter.py
import requests
from .base_adapter import DataSourceAdapter


class RESTAPIAdapter(DataSourceAdapter):
    def connect(self) -> bool:
        self.session = requests.Session()
        if self.config["auth"]["type"] == "oauth2":
            self.session.headers.update({
                "Authorization": f"Bearer {self._get_oauth_token()}"
            })
        return True

    def _get_oauth_token(self) -> str:
        # Implement OAuth2 flow (simplified)
        resp = requests.post(
            self.config["auth"]["token_url"],
            data={
                "client_id": self.config["auth"]["credentials"]["client_id"],
                "client_secret": self.config["auth"]["credentials"]["client_secret"],
                "grant_type": "client_credentials"
            }
        )
        return resp.json()["access_token"]

    def fetch(self) -> list[Dict]:
        url = f"{self.config['endpoint']['base_url']}{self.config['endpoint']['path']}"
        response = self.session.get(url)
        return response.json()["orders"]