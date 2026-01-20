"""
Email Sender Module

This module provides a reusable utility for sending HTML emails with optional PDF attachments.
This is a pure infrastructure module - no email content generation logic.

Uses SMTP for email delivery with support for:
- HTML email body
- Optional file attachments (PDF)
- Multiple recipients with delay between sends
- Environment variable-based configuration
"""

import os
import smtplib
import time
from typing import List, Optional, Tuple
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from reporting.config import EMAIL_DELAY_SECONDS, DEFAULT_SMTP_PORT
from reporting.logger import get_logger

logger = get_logger(__name__)


def send_email(
    to_emails: List[str],
    subject: str,
    html_body: str,
    attachments: Optional[List[str]] = None
) -> Tuple[bool, Optional[str]]:
    """
    Send HTML email with optional PDF attachments to multiple recipients.
    
    This function:
    1. Validates email addresses and attachment paths
    2. Reads SMTP credentials from environment variables
    3. Sends email to each recipient with a delay between sends
    4. Returns success status and error message if any
    
    Args:
        to_emails: List of recipient email addresses
        subject: Email subject line
        html_body: HTML content for email body
        attachments: Optional list of file paths to attach (typically PDF files)
    
    Returns:
        Tuple of (success: bool, error_msg: Optional[str])
        - success: True if at least one email was sent successfully, False otherwise
        - error_msg: Error message if sending failed, None if successful
    
    Environment Variables Required:
        - SMTP_SERVER: SMTP server address (e.g., 'smtp.gmail.com')
        - SMTP_USER: SMTP username/email address
        - SMTP_PASSWORD: SMTP password or app-specific password
        - SMTP_PORT: SMTP port (optional, defaults to 587)
    
    Example:
        success, error = send_email(
            to_emails=["user@example.com"],
            subject="Test Email",
            html_body="<h1>Hello</h1><p>This is a test.</p>",
            attachments=["report.pdf"]
        )
    """
    try:
        # Step 1: Validate inputs
        logger.info(f"Preparing to send email to {len(to_emails)} recipient(s)")
        logger.info(f"Subject: {subject}")
        if attachments:
            attachment_names = [os.path.basename(att) for att in attachments]
            logger.info(f"Attachments: {', '.join(attachment_names)}")
        else:
            logger.info("Attachments: None")
        
        if not to_emails or len(to_emails) == 0:
            error_msg = "Email recipient list is empty"
            logger.warning(error_msg)
            return False, error_msg
        
        # Validate email addresses (basic check)
        for email in to_emails:
            if not email or '@' not in email:
                error_msg = f"Invalid email address: {email}"
                logger.error(error_msg)
                return False, error_msg
        
        # Validate attachment paths if provided
        if attachments:
            for attachment_path in attachments:
                if not os.path.exists(attachment_path):
                    error_msg = f"Attachment file not found: {attachment_path}"
                    logger.error(error_msg)
                    return False, error_msg
                if not os.path.isfile(attachment_path):
                    error_msg = f"Attachment path is not a file: {attachment_path}"
                    logger.error(error_msg)
                    return False, error_msg
        
        # Step 2: Read SMTP configuration from environment variables
        smtp_server = os.getenv('SMTP_SERVER')
        smtp_user = os.getenv('SMTP_USER')
        smtp_password = os.getenv('SMTP_PASSWORD')
        smtp_port = int(os.getenv('SMTP_PORT', DEFAULT_SMTP_PORT))
        
        # Validate SMTP configuration
        if not smtp_server:
            error_msg = "SMTP_SERVER environment variable is not set"
            logger.error(error_msg)
            return False, error_msg
        
        if not smtp_user:
            error_msg = "SMTP_USER environment variable is not set"
            logger.error(error_msg)
            return False, error_msg
        
        if not smtp_password:
            error_msg = "SMTP_PASSWORD environment variable is not set"
            logger.error(error_msg)
            return False, error_msg
        
        logger.info(f"SMTP Configuration: {smtp_server}:{smtp_port}")
        logger.debug(f"SMTP User: {smtp_user}")
        
        # Step 3: Send email to each recipient
        success_count = 0
        failed_recipients = []
        
        for i, recipient in enumerate(to_emails):
            try:
                logger.info(f"Sending email to {recipient} ({i+1}/{len(to_emails)})")
                
                # Create RFC-compliant MIME structure:
                # multipart/mixed (root)
                # ├── multipart/alternative (body container)
                # │   └── text/html
                # └── application/pdf (attachment)

                # Root container for entire message (supports attachments)
                mixed_msg = MIMEMultipart('mixed')
                mixed_msg['From'] = smtp_user
                mixed_msg['To'] = recipient
                mixed_msg['Subject'] = subject

                # Inner container for body alternatives (currently only HTML)
                alternative_part = MIMEMultipart('alternative')

                # Add HTML body (unchanged content)
                html_part = MIMEText(html_body, 'html')
                alternative_part.attach(html_part)

                # Attach the body container to the root
                mixed_msg.attach(alternative_part)
                
                # Add attachments if provided (attach to multipart/mixed root)
                if attachments:
                    for attachment_path in attachments:
                        try:
                            with open(attachment_path, 'rb') as f:
                                attachment = MIMEBase('application', 'octet-stream')
                                attachment.set_payload(f.read())
                            
                            encoders.encode_base64(attachment)
                            
                            # Get filename from path
                            filename = os.path.basename(attachment_path)
                            # Properly formatted Content-Disposition header
                            # No leading spaces; filename safely quoted
                            attachment.add_header(
                                'Content-Disposition',
                                f'attachment; filename="{filename}"'
                            )
                            
                            mixed_msg.attach(attachment)
                            logger.debug(f"Attached file: {filename}")
                        except Exception as e:
                            logger.warning(f"Failed to attach {attachment_path}: {str(e)}")
                            # Continue even if attachment fails
                
                # Connect to SMTP server and send
                server = smtplib.SMTP(smtp_server, smtp_port)
                server.starttls()  # Enable TLS encryption
                server.login(smtp_user, smtp_password)
                server.send_message(mixed_msg)
                server.quit()
                
                success_count += 1
                logger.info(f"Email sent successfully to {recipient}")
                
                # Add delay between emails (except for last one)
                if i < len(to_emails) - 1:
                    time.sleep(EMAIL_DELAY_SECONDS)
                    logger.debug(f"Waiting {EMAIL_DELAY_SECONDS} seconds before next email")
                
            except Exception as e:
                error_msg = f"Failed to send email to {recipient}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                failed_recipients.append(recipient)
                # Continue to next recipient even if one fails
        
        # Step 4: Return result
        if success_count == 0:
            error_msg = f"Failed to send email to all {len(to_emails)} recipient(s)"
            if failed_recipients:
                error_msg += f". Failed recipients: {', '.join(failed_recipients)}"
            logger.error(error_msg)
            return False, error_msg
        
        if failed_recipients:
            logger.warning(f"Email sending partially successful: {success_count} sent, "
                         f"{len(failed_recipients)} failed")
            logger.warning(f"Failed recipients: {', '.join(failed_recipients)}")
        
        logger.info(f"Email sending completed: {success_count} successful, "
                   f"{len(failed_recipients)} failed")
        return True, None
        
    except Exception as e:
        error_msg = f"Unexpected error in send_email: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, error_msg

