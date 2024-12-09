import pandas as pd
import boto3
import requests
from typing import Dict, Any
import time
import logging

from .models import (
    DataFrameClientError,
    AuthenticationError,
    APIError,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class EventsClient:

    def __init__(
            self,
            api_url: str,
            user: str = None,
            password: str = None,
            auth_token: str = None,
            region: str = 'eu-west-1'
    ):
        self.api_url = api_url.rstrip('/')
        self.region = region
        self.user = user
        self.password = password

        if not auth_token and user and password:
            self._auth_token = self.get_auth_token(api_url, user, password, region)
        else:
            self._auth_token = auth_token

        if not self._auth_token:
            raise ValueError("Either auth_token or user/password required")

        self.headers = {
            'Authorization': f"Bearer {self._auth_token}",
            'Content-Type': 'application/json'
        }

    @staticmethod
    def get_auth_token(api_url: str, user: str, password: str, region: str = 'eu-west-1') -> str:
        api_url = api_url.rstrip('/')
        try:
            response = requests.get(f"{api_url}/auth/config")
            response.raise_for_status()
            client_id = response.json()['userPoolClientId']

            cognito = boto3.client('cognito-idp', region_name=region)
            auth = cognito.initiate_auth(
                ClientId=client_id,
                AuthFlow='USER_PASSWORD_AUTH',
                AuthParameters={'USERNAME': user, 'PASSWORD': password}
            )
            return auth['AuthenticationResult']['IdToken']
        except Exception as e:
            raise AuthenticationError(f"Authentication failed: {str(e)}")

    def _refresh_token_if_needed(self) -> None:
        try:
            response = requests.get(f"{self.api_url}/auth/verify", headers=self.headers)
            if response.status_code == 401 and self.user and self.password:
                self._auth_token = self.get_auth_token(
                    self.api_url, self.user, self.password, self.region
                )
                self.headers['Authorization'] = f"Bearer {self._auth_token}"
            elif response.status_code == 401:
                raise AuthenticationError("Token expired and no refresh credentials")
        except requests.exceptions.RequestException as e:
            raise APIError(f"Token verification failed: {str(e)}")

    def _make_request(
            self,
            method: str,
            url: str,
            retries: int,
            retry_delay: int,
            timeout: int,
            **kwargs
    ) -> Dict[str, Any]:
        """Helper method to make HTTP requests with retries"""
        for attempt in range(retries):
            try:
                response = requests.request(
                    method,
                    url,
                    headers=self.headers,
                    timeout=timeout,
                    **kwargs
                )
                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                if attempt == retries - 1:
                    raise APIError(f"Request failed after {retries} attempts: {str(e)}")
                time.sleep(retry_delay * (2 ** attempt))

    def concat_events(
            self,
            df: pd.DataFrame,
            dataframe_name: str,
            retries: int = 3,
            retry_delay: int = 1,
            timeout: int = 300
    ) -> Dict[str, Any]:
        """
        Update (append) new events data to an existing DataFrame through the API

        Args:
            df: pandas DataFrame with new data to append
            dataframe_name: S3 key/path of the target file
            stream: If True, use SQS streaming, if False do direct update
            retries: Number of retries
            retry_delay: Seconds to wait between retries
            timeout: Request timeout in seconds

        Returns:
            Dict with update results
        """
        try:
            self._refresh_token_if_needed()

            if df.empty:
                raise DataFrameClientError("DataFrame is empty")

            if len(df) > 20:
                raise ValueError('This method is to concat real time events: '
                                 'small chunks of ideally 1 single row and up to 20 rows.'
                                 'For larger batch concats it is more reliable for you to do: '
                                 'get_dataframe(), pd.concat() on your own and post_dataframe()')

            # URL encode the key for path parameters
            encoded_key = requests.utils.quote(dataframe_name, safe='')

            url = f"{self.api_url}/dataframes/{encoded_key}/event-upload"

            # Send update request
            response = self._make_request(
                'POST',
                url,
                json={
                    'data': df.to_dict('records'),
                    'key': dataframe_name
                },
                timeout=timeout,
                retries=retries,
                retry_delay=retry_delay
            )

            logger.info('Your latest data stream can take up to 60 seconds to be available')

            return response

        except Exception as e:
            if isinstance(e, (DataFrameClientError, APIError, AuthenticationError)):
                raise
            raise DataFrameClientError(f"Error in update dataframe: {str(e)}")
