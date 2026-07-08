"""Bearer-token authorization for the billing ingest endpoint (spec Amendment A1).

Wraps ``quixportal.auth.Auth`` (portal-native permission check) behind a small
``Authorizer`` so the HTTP layer depends on an interface rather than the SDK, and
so smoke tests can inject a stub. ``Auth`` keeps its own validation cache keyed by
sha256(token) (default 300 s) -- we add no cache of our own.
"""

from __future__ import annotations

import logging
from enum import Enum

logger = logging.getLogger(__name__)


class AuthDecision(Enum):
    """Outcome of an authorization check, mapped to HTTP status by the handler."""

    ALLOW = "allow"  # -> continue (then 202)
    UNAUTHENTICATED = "unauthenticated"  # missing/malformed header -> 401
    FORBIDDEN = "forbidden"  # validated False -> 403
    UNAVAILABLE = "unavailable"  # portal/httpx error -> 503 (retryable)


def extract_bearer(header: str | None) -> str | None:
    """Return the token from an ``Authorization: Bearer <token>`` header, else None."""
    if not header:
        return None
    parts = header.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        return None
    return parts[1].strip()


class Authorizer:
    """Validate an Authorization header into an :class:`AuthDecision`.

    The underlying ``quixportal.auth.Auth`` is constructed lazily on first use so
    importing the module (and local dev with ``AUTH_ENABLED=false``) never needs
    ``Quix__Portal__Api`` to be set. Any error raised by ``Auth`` (it has no
    retry) is treated as UNAVAILABLE -> 503: never accept unvalidated, never 403
    an outage.
    """

    def __init__(
        self,
        *,
        workspace_id: str,
        required_permission: str,
        cache_seconds: float,
        enabled: bool = True,
        auth=None,
    ):
        self._workspace_id = workspace_id
        self._required_permission = required_permission
        self._cache_seconds = cache_seconds
        self._enabled = enabled
        self._auth = auth  # allows tests to inject a stub Auth

    def _get_auth(self):
        if self._auth is None:
            from quixportal.auth import Auth

            self._auth = Auth(cache_validity=self._cache_seconds)
        return self._auth

    def authorize(self, authorization_header: str | None) -> AuthDecision:
        if not self._enabled:
            return AuthDecision.ALLOW
        token = extract_bearer(authorization_header)
        if token is None:
            return AuthDecision.UNAUTHENTICATED
        try:
            allowed = self._get_auth().validate_permissions(
                token, "Workspace", self._workspace_id, self._required_permission
            )
        except Exception as exc:  # httpx/transport error, missing portal, etc.
            logger.info("[BILLING-SINK] auth backend unavailable: %s", exc)
            return AuthDecision.UNAVAILABLE
        return AuthDecision.ALLOW if allowed else AuthDecision.FORBIDDEN
