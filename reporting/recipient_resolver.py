"""
Email Recipient Resolver Module

This module resolves email recipients for the inventory reporting pipeline.
It supports fetching recipients dynamically from an ERP GraphQL API with
fallback to the static EMAIL_RECIPIENTS list from config.

Resolution Logic:
1. Check if USE_API_EMAIL_RECIPIENTS is enabled
   - If False or not set → use EMAIL_RECIPIENTS from config (default behavior)
2. If enabled, try to fetch from ERP GraphQL API
   - If successful AND non-empty → use ERP emails (cleaned + deduplicated)
   - If any error / timeout / empty response → fallback to EMAIL_RECIPIENTS

This module is designed to be NON-BREAKING:
- Existing EMAIL_RECIPIENTS logic remains unchanged
- ERP API errors never stop the job
- No exceptions propagate from this module
"""

import requests
from typing import List, Tuple

from reporting.config import (
    EMAIL_RECIPIENTS,
    USE_API_EMAIL_RECIPIENTS,
    ERP_GRAPHQL_ENDPOINT,
    ERP_API_TIMEOUT,
    ERP_API_TOKEN
)
from reporting.logger import get_logger

logger = get_logger(__name__)


def _fetch_recipients_from_erp_api() -> Tuple[bool, List[str], str]:
    """
    Fetch email recipients from ERP GraphQL API.
    
    This function:
    1. Sends a GraphQL query to the ERP endpoint
    2. Parses the response structure: data["inventoryDashboardEmailRecipients"]["data"]
    3. Extracts and cleans email addresses
    
    Returns:
        Tuple of (success: bool, emails: List[str], error_msg: str)
        - success: True if API call succeeded and returned valid data
        - emails: List of cleaned email addresses (empty if failed)
        - error_msg: Error message if failed, empty string if successful
    
    Note:
        This function catches ALL exceptions and returns (False, [], error_msg).
        It will NEVER raise an exception.
    """
    try:
        # Validate configuration
        if not ERP_GRAPHQL_ENDPOINT:
            return False, [], "ERP_GRAPHQL_ENDPOINT is not configured"
        
        # Prepare GraphQL query
        graphql_query = """
        query {
            inventoryDashboardEmailRecipients {
                status
                data {
                    email
                }
                message
            }
        }
        """
        
        # Prepare headers with static internal token
        headers = {
            "Content-Type": "application/json"
        }
        
        # Add X-Internal-Token header if token is configured
        if ERP_API_TOKEN:
            headers["X-Internal-Token"] = ERP_API_TOKEN
        else:
            logger.warning("ERP_API_TOKEN is not configured. Proceeding without authentication.")
        
        # Prepare request payload
        payload = {
            "query": graphql_query
        }
        
        logger.info(f"Fetching email recipients from ERP API: {ERP_GRAPHQL_ENDPOINT}")
        logger.debug(f"Request timeout: {ERP_API_TIMEOUT} seconds")
        
        # Make HTTP POST request
        response = requests.post(
            ERP_GRAPHQL_ENDPOINT,
            json=payload,
            headers=headers,
            timeout=ERP_API_TIMEOUT
        )
        
        # Check HTTP status
        if response.status_code != 200:
            error_msg = f"ERP API returned HTTP {response.status_code}"
            logger.warning(error_msg)
            return False, [], error_msg
        
        # Parse JSON response
        try:
            response_data = response.json()
        except ValueError as e:
            error_msg = f"Failed to parse ERP API response as JSON: {str(e)}"
            logger.warning(error_msg)
            return False, [], error_msg
        
        # Navigate to the expected response structure
        # Expected: data["inventoryDashboardEmailRecipients"]["data"]
        if "data" not in response_data:
            error_msg = "ERP API response missing 'data' field"
            logger.warning(error_msg)
            return False, [], error_msg
        
        query_result = response_data.get("data", {}).get("inventoryDashboardEmailRecipients")
        
        if query_result is None:
            error_msg = "ERP API response missing 'inventoryDashboardEmailRecipients' field"
            logger.warning(error_msg)
            return False, [], error_msg
        
        # Check status field if present
        api_status = query_result.get("status", "")
        if api_status and api_status.lower() != "success":
            api_message = query_result.get("message", "Unknown error")
            error_msg = f"ERP API returned non-success status: {api_status} - {api_message}"
            logger.warning(error_msg)
            return False, [], error_msg
        
        # Extract email list
        email_data_list = query_result.get("data")
        
        if email_data_list is None:
            error_msg = "ERP API response missing 'data' array in inventoryDashboardEmailRecipients"
            logger.warning(error_msg)
            return False, [], error_msg
        
        if not isinstance(email_data_list, list):
            error_msg = f"ERP API 'data' field is not a list (got {type(email_data_list).__name__})"
            logger.warning(error_msg)
            return False, [], error_msg
        
        # Extract and clean emails
        emails = []
        for item in email_data_list:
            if isinstance(item, dict) and "email" in item:
                email = item.get("email")
                if email and isinstance(email, str):
                    # Clean: strip whitespace and convert to lowercase
                    cleaned_email = email.strip().lower()
                    if cleaned_email and "@" in cleaned_email:
                        emails.append(cleaned_email)
        
        # Check if we got any valid emails
        if not emails:
            error_msg = "ERP API returned empty or invalid email list"
            logger.warning(error_msg)
            return False, [], error_msg
        
        # Deduplicate while preserving order
        seen = set()
        unique_emails = []
        for email in emails:
            if email not in seen:
                seen.add(email)
                unique_emails.append(email)
        
        logger.info(f"Successfully fetched {len(unique_emails)} email recipient(s) from ERP API")
        return True, unique_emails, ""
        
    except requests.exceptions.Timeout:
        error_msg = f"ERP API request timed out after {ERP_API_TIMEOUT} seconds"
        logger.warning(error_msg)
        return False, [], error_msg
        
    except requests.exceptions.ConnectionError as e:
        error_msg = f"Failed to connect to ERP API: {str(e)}"
        logger.warning(error_msg)
        return False, [], error_msg
        
    except requests.exceptions.RequestException as e:
        error_msg = f"ERP API request failed: {str(e)}"
        logger.warning(error_msg)
        return False, [], error_msg
        
    except Exception as e:
        # Catch-all for any unexpected errors
        error_msg = f"Unexpected error fetching from ERP API: {str(e)}"
        logger.warning(error_msg, exc_info=True)
        return False, [], error_msg


def resolve_email_recipients() -> List[str]:
    """
    Resolve email recipients for the inventory reporting pipeline.
    
    Resolution Logic:
    1. If USE_API_EMAIL_RECIPIENTS is False or not set:
       → Return EMAIL_RECIPIENTS from config (default behavior, no API call)
    
    2. If USE_API_EMAIL_RECIPIENTS is True:
       → Try to fetch from ERP GraphQL API
       → If successful AND non-empty → return ERP emails
       → If any error / timeout / empty → fallback to EMAIL_RECIPIENTS
    
    Returns:
        List of email addresses to send the report to.
        This function NEVER returns an empty list if EMAIL_RECIPIENTS has values.
        This function NEVER raises an exception.
    
    Note:
        This function is designed to be NON-BREAKING. The job will always have
        recipients to send to (as long as EMAIL_RECIPIENTS is configured).
    """
    try:
        # Check feature flag
        if not USE_API_EMAIL_RECIPIENTS:
            logger.info("USE_API_EMAIL_RECIPIENTS is disabled. Using static EMAIL_RECIPIENTS from config.")
            logger.info(f"Resolved {len(EMAIL_RECIPIENTS)} email recipient(s) from config")
            return EMAIL_RECIPIENTS.copy()
        
        # Feature flag is enabled - try ERP API
        logger.info("USE_API_EMAIL_RECIPIENTS is enabled. Attempting to fetch from ERP API...")
        
        success, erp_emails, error_msg = _fetch_recipients_from_erp_api()
        
        if success and erp_emails:
            logger.info(f"Using {len(erp_emails)} email recipient(s) from ERP API")
            return erp_emails
        
        # Fallback to static EMAIL_RECIPIENTS
        if error_msg:
            logger.warning(f"ERP API fetch failed: {error_msg}")
        logger.info("Falling back to static EMAIL_RECIPIENTS from config")
        logger.info(f"Resolved {len(EMAIL_RECIPIENTS)} email recipient(s) from config (fallback)")
        return EMAIL_RECIPIENTS.copy()
        
    except Exception as e:
        # Ultimate safety net - should never reach here, but just in case
        logger.error(f"Unexpected error in resolve_email_recipients: {str(e)}", exc_info=True)
        logger.info("Falling back to static EMAIL_RECIPIENTS from config (safety fallback)")
        return EMAIL_RECIPIENTS.copy()
