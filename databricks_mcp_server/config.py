"""Configuration and authorization helper for Databricks MCP server.

This module provides:
  - `BaseConfig`: reads Databricks-related environment variables and prepares
    authentication headers for REST API calls.
  - `DatabricksSDKConfig`: creates a Databricks SDK Config object for use with
    the official Databricks Python SDK.

Behavior for BaseConfig:
- If `DATABRICKS_TOKEN` is present, it uses it as a PAT (personal access token).
- Otherwise, if `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` are
  present, and `DATABRICKS_OAUTH_TOKEN_URL` is provided, it performs an
  OAuth2 client-credentials request to obtain an access token.

If neither method has sufficient environment variables, `BaseConfig.authorize`
will raise `ValueError`.
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

import requests
from databricks.sdk.core import Config as DatabricksConfig


class BaseConfig:
	"""Holds Databricks configuration and provides authorization headers.

	Attributes:
		host: Optional[str] - Databricks workspace/host URL (e.g. https://adb-...)
		account_id: Optional[str] - Databricks account id (when applicable)
		pat_token: Optional[str] - Personal access token if provided
		client_id: Optional[str] - OAuth client id
		client_secret: Optional[str] - OAuth client secret
		oauth_token_url: Optional[str] - Token endpoint to request OAuth tokens
		oauth_scope: Optional[str] - Optional scope to request from the token endpoint
		access_token: Optional[str] - Cached access token (when using OAuth)
		token_expires_at: Optional[float] - Unix ts when access token expires
	"""

	def __init__(
		self,
		host: str = None,
		account_id: Optional[str] = None,
		pat_token: Optional[str] = None,
		client_id: Optional[str] = None,
		client_secret: Optional[str] = None,
		oauth_token_url: Optional[str] = None,
		oauth_scope: Optional[str] = None,
	) -> None:
		self.host = host
		self.account_id = account_id
		self.pat_token = pat_token
		self.client_id = client_id
		self.client_secret = client_secret
		self.oauth_token_url = oauth_token_url
		self.oauth_scope = oauth_scope

		# runtime-only fields
		self.access_token: Optional[str] = None
		self.token_expires_at: Optional[float] = None

	def __repr__(self) -> str:  # pragma: no cover - trivial
		return (
			f"Config(host={self.host!r}, account_id={self.account_id!r}, "
			f"pat_token={'***' if self.pat_token else None}, "
			f"client_id={'***' if self.client_id else None})"
		)

	@classmethod
	def authorize(cls) -> "BaseConfig":
		"""Create and return a BaseConfig based on environment variables.

		Order:
		  1. If `DATABRICKS_TOKEN` is present, create config with PAT.
		  2. Else if `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`
			 (and `DATABRICKS_OAUTH_TOKEN_URL`) are present, perform OAuth2
			 client-credentials request and populate `access_token`.

		Raises:
			ValueError: if no valid authentication env vars are available or
						if an OAuth token request fails.
		"""
		env = os.environ
		host = env.get("DATABRICKS_HOST")
		account_id = env.get("DATABRICKS_ACCOUNT_ID")

		# PAT flow
		pat = env.get("DATABRICKS_TOKEN") or env.get("DATABRICKS_PAT")
		if pat:
			cfg = cls(host=host, account_id=account_id, pat_token=pat)
			return cfg

		# OAuth/client credentials flow (requires token URL)
		client_id = env.get("DATABRICKS_CLIENT_ID")
		client_secret = env.get("DATABRICKS_CLIENT_SECRET")
		token_url = env.get("DATABRICKS_OAUTH_TOKEN_URL")
		scope = env.get("DATABRICKS_OAUTH_SCOPE")

		if client_id and client_secret:
			if not token_url:
				raise ValueError(
					"DATABRICKS_OAUTH_TOKEN_URL must be set for client-credentials auth"
				)

			cfg = cls(
				host=host,
				account_id=account_id,
				client_id=client_id,
				client_secret=client_secret,
				oauth_token_url=token_url,
				oauth_scope=scope,
			)

			# perform token request
			data = {"grant_type": "client_credentials"}
			if scope:
				data["scope"] = scope

			# Try standard client credentials form first
			data["client_id"] = client_id
			data["client_secret"] = client_secret

			headers = {"Content-Type": "application/x-www-form-urlencoded"}

			resp = requests.post(token_url, data=data, headers=headers, timeout=10)
			try:
				resp.raise_for_status()
			except Exception as exc:  # pragma: no cover - network error
				raise ValueError(
					f"failed to obtain OAuth token from {token_url}: {exc} - {resp.text}"
				)

			body = resp.json()
			access_token = body.get("access_token")
			if not access_token:
				raise ValueError(f"token endpoint did not return access_token: {body}")

			cfg.access_token = access_token
			expires_in = body.get("expires_in")
			if expires_in:
				try:
					self_expires = float(expires_in)
				except Exception:
					self_expires = None
				if self_expires:
					cfg.token_expires_at = time.time() + float(self_expires) - 10

			return cfg

		# If we reach here, no supported auth variables were present
		raise ValueError(
			"No Databricks authentication environment variables found. "
			"Provide DATABRICKS_TOKEN (PAT) or DATABRICKS_CLIENT_ID and "
			"DATABRICKS_CLIENT_SECRET along with DATABRICKS_OAUTH_TOKEN_URL."
		)

	def _ensure_oauth_token(self) -> None:
		"""Ensure a valid OAuth access token is available (refresh if needed)."""
		if self.pat_token:
			return
		if not (self.client_id and self.client_secret and self.oauth_token_url):
			raise ValueError("OAuth token config missing client_id/secret or token URL")

		if self.access_token and self.token_expires_at and time.time() < self.token_expires_at:
			return

		# request a new token
		data = {"grant_type": "client_credentials"}
		if self.oauth_scope:
			data["scope"] = self.oauth_scope
		data["client_id"] = self.client_id
		data["client_secret"] = self.client_secret

		headers = {"Content-Type": "application/x-www-form-urlencoded"}
		resp = requests.post(self.oauth_token_url, data=data, headers=headers, timeout=10)
		resp.raise_for_status()
		body = resp.json()
		access_token = body.get("access_token")
		if not access_token:
			raise ValueError(f"token endpoint did not return access_token: {body}")
		self.access_token = access_token
		expires_in = body.get("expires_in")
		if expires_in:
			try:
				expires_val = float(expires_in)
			except Exception:
				expires_val = None
			if expires_val:
				self.token_expires_at = time.time() + expires_val - 10

	@property
	def headers(self) -> Dict[str, str]:
		"""Return headers ready to use for Databricks REST API calls.

		Uses the PAT when available, otherwise uses an OAuth access token.
		"""
		if self.pat_token:
			return {"Authorization": f"Bearer {self.pat_token}", "Content-Type": "application/json"}

		# ensure we have an oauth token
		self._ensure_oauth_token()
		if self.access_token:
			return {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}

		raise ValueError("No authentication token available to build headers")


class DatabricksSDKConfig:
	"""Creates and manages Databricks SDK Config for use with the official SDK.

	Use this class to obtain a Databricks SDK Config object that can be used
	with databricks.sdk.WorkspaceClient or similar clients.
	"""

	@staticmethod
	def authorize(
		http_timeout_seconds: int = 30,
		retry_timeout_seconds: int = 60,
	) -> Any:
		"""Create and return a Databricks SDK Config based on environment variables.

		Args:
			http_timeout_seconds: Timeout for HTTP requests (default: 30)
			retry_timeout_seconds: Timeout for retries (default: 60)

		Returns:
			A databricks.sdk.core.Config object ready for use with SDK clients.

		Raises:
			ValueError: if databricks-sdk is not installed or auth fails.
					if no valid authentication env vars are available.
		"""

		env = os.environ
		host = env.get("DATABRICKS_HOST")
		if not host:
			raise ValueError("DATABRICKS_HOST environment variable is required")

		# Check for PAT first
		token = env.get("DATABRICKS_TOKEN") or env.get("DATABRICKS_PAT")
		if token:
			return DatabricksConfig(
				host=host,
				token=token,
				http_timeout_seconds=http_timeout_seconds,
				retry_timeout_seconds=retry_timeout_seconds,
			)

		# Check for OAuth credentials
		client_id = env.get("DATABRICKS_CLIENT_ID")
		client_secret = env.get("DATABRICKS_CLIENT_SECRET")

		if client_id and client_secret:
			return DatabricksConfig(
				host=host,
				client_id=client_id,
				client_secret=client_secret,
				http_timeout_seconds=http_timeout_seconds,
				retry_timeout_seconds=retry_timeout_seconds,
			)

		raise ValueError(
			"No Databricks authentication environment variables found. "
			"Provide DATABRICKS_TOKEN (PAT) or DATABRICKS_CLIENT_ID and "
			"DATABRICKS_CLIENT_SECRET."
		)

