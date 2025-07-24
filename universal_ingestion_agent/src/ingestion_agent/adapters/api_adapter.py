import requests
import json
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import time
from urllib.parse import urljoin
from requests.auth import AuthBase
from .base_adapter import DataSourceAdapter
from pandas import DataFrame, json_normalize
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ApiAdapter(DataSourceAdapter):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.session = requests.Session()
        self._configure_session()

    def _configure_session(self):
        """Configure session with auth, headers, and proxies"""
        # Set default headers
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': self.config['endpoint'].get('content_type', 'application/json')
        })

        # Configure authentication
        auth_type = self.config['auth']['type']
        if auth_type == 'oauth2':
            self._setup_oauth()
        elif auth_type == 'api_key':
            self._setup_api_key()
        elif auth_type == 'jwt':
            self._setup_jwt()
        elif auth_type == 'aws_sigv4':
            self._setup_aws_sigv4()

        # Configure proxies
        if 'proxy_url' in self.config.get('network', {}):
            self.session.proxies = {
                'http': self.config['network']['proxy_url'],
                'https': self.config['network']['proxy_url']
            }

        # SSL verification
        self.session.verify = not self.config.get('network', {}).get('disable_ssl_verify', False)

    def _save_token(self, token):
        """Persist tokens back to config"""
        self.config['auth']['config']['token'] = token
        # TODO Save to secure storage (DB, Vault, etc.) instead of plain sight

    def _setup_oauth(self):
        """Configure OAuth2 authentication with client credentials flow"""
        from requests_oauthlib import OAuth2Session
        auth_config = self.config['auth']['config']

        # Convert string token to proper dict format if needed
        token = auth_config.get('token')
        if token and isinstance(token, str):
            token = {'access_token': token, 'token_type': 'Bearer'}

        # Initialize OAuth2 session
        self.oauth_session = OAuth2Session(
            client_id=auth_config['credentials']['client_id'],
            token=token,  # Pre-existing token
            auto_refresh_url=auth_config.get('refresh_url'),
            auto_refresh_kwargs={
                'client_id': auth_config['credentials']['client_id'],
                'client_secret': auth_config['credentials']['client_secret']
            },
            token_updater=self._save_token
        )

        # If no token exists, fetch a new one
        if not auth_config.get('token'):
            token = self.oauth_session.fetch_token(
                token_url=auth_config['token_url'],
                client_secret=auth_config['credentials']['client_secret'],
                scope=auth_config.get('scopes', '').split(',')
            )
            self._save_token(token)

        # Replace the default session with our OAuth session
        self.session = self.oauth_session
        self.session.headers.update({
            'Accept': 'application/json'
        })

    def _setup_api_key(self):
        """Configure API key authentication"""
        auth_config = self.config['auth']['config']
        header_name = auth_config.get('key_header', 'X-API-Key')
        self.session.headers[header_name] = auth_config['key_value']

    def _setup_jwt(self):
        """Configure JWT authentication"""
        import jwt
        auth_config = self.config['auth']['config']
        payload = {
            'iss': auth_config['jwt_issuer'],
            'exp': datetime.utcnow() + timedelta(minutes=5)
        }
        token = jwt.encode(
            payload,
            auth_config['private_key'],
            algorithm='RS256'
        )
        self.session.headers['Authorization'] = f'Bearer {token}'

    def _setup_aws_sigv4(self):
        """Configure AWS Signature V4 authentication"""
        from requests_auth_aws_sigv4 import AWSSigV4
        auth_config = self.config['auth']['config']
        self.session.auth = AWSSigV4(
            auth_config['service'],
            region=auth_config.get('region', 'us-east-1'),
            access_key=auth_config['access_key'],
            secret_key=auth_config['secret_key']
        )

    def _make_request(self, url: str, method: str, **kwargs) -> requests.Response:
        """Execute API request with retry logic"""
        max_retries = self.config['error_handling']['retries']
        retry_delay = self._parse_duration(self.config['error_handling']['retry_delay'])

        for attempt in range(max_retries + 1):
            try:
                response = self.session.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if attempt == max_retries:
                    logger.error(f"Final attempt failed: {str(e)}")
                    raise
                logger.warning(f"Attempt {attempt + 1} failed, retrying: {str(e)}")
                time.sleep(retry_delay)

    def _parse_duration(self, duration_str: str) -> float:
        """Convert '5s', '10m' etc. to seconds"""
        units = {
            's': 1,
            'm': 60,
            'h': 3600
        }
        unit = duration_str[-1]
        value = int(duration_str[:-1])
        return value * units.get(unit, 1)

    def _handle_pagination(self, initial_response: requests.Response) -> list:
        """Handle paginated responses"""
        pagination_type = self.config['pagination']['type']
        data = []

        if pagination_type == 'link_header':
            response = initial_response
            while True:
                data.extend(response.json())
                if 'next' not in response.links:
                    break
                next_url = response.links['next']['url']
                response = self._make_request(next_url, 'GET')

        elif pagination_type == 'cursor':
            cursor = None
            while True:
                params = {'cursor': cursor} if cursor else {}
                response = self._make_request(
                    self._build_url(),
                    self.config['endpoint']['method'],
                    params=params
                )
                batch = response.json()
                data.extend(batch['items'])
                cursor = batch.get('next_cursor')
                if not cursor:
                    break

        return data

    def _build_url(self) -> str:
        """Construct full URL from base and path"""
        endpoint = self.config['endpoint']
        return urljoin(endpoint['base_url'], endpoint['path'])

    def _prepare_request(self) -> dict:
        """Prepare request kwargs based on config"""
        endpoint = self.config['endpoint']
        kwargs = {
            'method': endpoint['method'],
            'url': self._build_url()
        }

        # Handle query parameters
        if 'query_params' in endpoint:
            params = {}
            for param in endpoint['query_params']:
                params[param['name']] = param['value']
            kwargs['params'] = params

        # Handle request body
        if 'body' in self.config and self.config['body']['type'] != 'none':
            body_type = self.config['body']['type']
            if body_type == 'json':
                kwargs['json'] = json.loads(self.config['body']['template'])
            elif body_type == 'form-data':
                kwargs['data'] = self.config['body']['template']
            elif body_type == 'xml':
                kwargs['data'] = self.config['body']['template']
                kwargs['headers']['Content-Type'] = 'application/xml'

        return kwargs

    def connect(self) -> bool:
        """Test connection"""
        try:
            test_url = urljoin(self.config['endpoint']['base_url'], 'health')
            response = self.session.get(test_url, timeout=5)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False

    def fetch(self) -> Any:
        """Fetch data from API with pagination support"""
        request_kwargs = self._prepare_request()

        # Initial request
        response = self._make_request(**request_kwargs)

        # Handle pagination if configured
        if self.config['pagination']['type'] != 'none':
            return self._handle_pagination(response)

        return response.json()

    def normalize(self) -> DataFrame:
        """Convert API response to DataFrame with schema enforcement"""
        raw_data = self.fetch()
        df = json_normalize(raw_data)

        # Apply schema overrides
        if 'schema_inference' in self.config:
            for field in self.config['schema_inference'].get('field_overrides', []):
                if field['name'] in df.columns:
                    if field['type'] == 'datetime':
                        df[field['name']] = pd.to_datetime(
                            df[field['name']],
                            format=field.get('format')
                        )
                    elif field['type'] == 'decimal':
                        df[field['name']] = pd.to_numeric(
                            df[field['name']],
                            errors='coerce'
                        )

        return df