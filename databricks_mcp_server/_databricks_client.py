"""Base Databricks API client for making REST API calls.

This module provides a DatabricksClient class that wraps the BaseConfig
authentication and handles making REST API calls to Databricks endpoints.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

import requests

from .config import BaseConfig


class DatabricksClient:
	"""Base client for making authenticated requests to Databricks REST API.

	This client handles authorization and provides methods for making
	HTTP requests to Databricks API endpoints.

	Attributes:
		config: BaseConfig - The authentication configuration object.
	"""

	def __init__(self, config: BaseConfig) -> None:
		"""Initialize the Databricks client with a config.

		Args:
			config: A BaseConfig instance with authentication details.
		"""
		self.config = config

	@classmethod
	def authorize(cls) -> "DatabricksClient":
		"""Create and return a DatabricksClient with authorized config.

		Reads environment variables and creates a BaseConfig, then initializes
		the client with it.

		Returns:
			A DatabricksClient instance ready to make API calls.

		Raises:
			ValueError: if no valid authentication env vars are available.
		"""
		cfg = BaseConfig.authorize()
		return cls(cfg)

	def request(
		self,
		method: str,
		endpoint: str,
		json_data: Optional[Dict[str, Any]] = None,
		params: Optional[Dict[str, Any]] = None,
		headers: Optional[Dict[str, str]] = None,
		timeout: int = 30,
	) -> requests.Response:
		"""Make an authenticated HTTP request to a Databricks API endpoint.

		Args:
			method: HTTP method (GET, POST, PUT, DELETE, PATCH, etc.)
			endpoint: API endpoint path (e.g., "/api/2.1/jobs/list")
			json_data: JSON body data to send with the request
			params: Query parameters
			headers: Additional headers to include (auth headers added automatically)
			timeout: Request timeout in seconds (default: 30)

		Returns:
			The requests.Response object from the API call.

		Raises:
			ValueError: if host is not configured in the config.
			requests.RequestException: if the request fails.
		"""
		if not self.config.host:
			raise ValueError("DATABRICKS_HOST is not configured")

		url = f"{self.config.host}{endpoint}"

		# Get base headers from config (includes Authorization)
		req_headers = self.config.headers.copy()

		# Merge with any additional headers provided
		if headers:
			req_headers.update(headers)

		resp = requests.request(
			method=method,
			url=url,
			json=json_data,
			params=params,
			headers=req_headers,
			timeout=timeout,
		)

		return resp

	def do(
		self,
		method: str,
		endpoint: str,
		json_data: Optional[Dict[str, Any]] = None,
		params: Optional[Dict[str, Any]] = None,
		headers: Optional[Dict[str, str]] = None,
		timeout: int = 30,
		raise_for_status: bool = True,
	) -> Dict[str, Any]:
		"""Make an authenticated HTTP request and return the JSON response.

		Convenience method that wraps request() and automatically parses
		the JSON response.

		Args:
			method: HTTP method (GET, POST, PUT, DELETE, PATCH, etc.)
			endpoint: API endpoint path (e.g., "/api/2.1/jobs/list")
			json_data: JSON body data to send with the request
			params: Query parameters
			headers: Additional headers to include
			timeout: Request timeout in seconds (default: 30)
			raise_for_status: Whether to raise an exception for error status codes
				(default: True)

		Returns:
			The parsed JSON response as a dictionary.

		Raises:
			ValueError: if host is not configured.
			requests.RequestException: if the request fails and raise_for_status=True.
			ValueError: if the response cannot be parsed as JSON.
		"""
		resp = self.request(
			method=method,
			endpoint=endpoint,
			json_data=json_data,
			params=params,
			headers=headers,
			timeout=timeout,
		)

		if raise_for_status:
			resp.raise_for_status()

		try:
			return resp.json()
		except ValueError as e:
			raise ValueError(f"Failed to parse JSON response: {e}")

