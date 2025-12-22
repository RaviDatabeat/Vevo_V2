import logging
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logger = logging.getLogger(__name__)


def slack_notification(webhook_url: str, json_data):
    """Send alert with all users in one Slack message."""
    if not json_data:
        logger.info("No message to send; skipping Slack notification.")
        return False
    try:
        response = requests.post(webhook_url, json=json_data, timeout=50)
        response.raise_for_status()
        logger.info("Slack alert sent successfully")
        return True

    except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
        logger.error("Failed to send alert to Slack: %s", e, exc_info=True)
        return False


def simple_slack_notification(webhook_url: str, msg: str):
    """Send alert with all users in one Slack message."""

    json_data = {
        "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": msg}}]
    }

    return slack_notification(webhook_url, json_data)


class SlackAPI:
    def __init__(
        self,
        bot_token: str,
        max_retries: int = 3,
        session: Optional[requests.Session] = None,
    ):
        """
        Initialize Slack notifier with webhook URL and retry configuration.
        Args:
            bot_token: Bot User OAuth Token.
            max_retries: Max retry attempts for transient failures.
            session: Optional pre-configured requests.Session (useful for testing).
        """
        self.bot_token = bot_token
        self.session = session or self._create_session(max_retries)

    def _create_session(self, max_retries: int) -> requests.Session:
        """Create requests session with retry strategy for transient errors."""
        session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)

        # Add custom headers
        session.headers.update(
            {
                "Authorization": f"Bearer {self.bot_token}",
                "Content-Type": "application/json",
                "User-Agent": "SlackNotifierApp/1.0",
            }
        )
        return session

    def lookup_by_email(self, user_email: str):
        url = "https://slack.com/api/users.lookupByEmail"

        params = {"email": user_email}

        try:
            res = self.session.get(url, params=params)

            response = res.json()

            if response.get("ok"):
                user = response.get("user")
                return user["id"]
            else:
                logger.info(response)
                return None
        except BaseException as e:
            logger.error(str(e))
            return None
