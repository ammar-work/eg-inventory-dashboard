"""
Email Body Generator Module

This module generates HTML email body content for the inventory reporting pipeline.
The email body includes:
- Greeting
- Intro text
- Priority items table (top 15 specifications)
- Notes section
- ERP link
- Attachment explanation
- Footer

All HTML is email-client safe (Gmail-compatible).
"""

from datetime import datetime
from typing import Optional
import pandas as pd

from reporting.config import PDF_SPECIFICATIONS, ERP_SYSTEM_LINK, DATE_FORMAT_DISPLAY
from reporting.logger import get_logger

logger = get_logger(__name__)


def format_date_for_email(date_value: datetime) -> str:
    """
    Format date value for email display (DD MMM YYYY).
    
    Example: "15 Jan 2024"
    
    Args:
        date_value: datetime object
    
    Returns:
        Formatted date string (e.g., "15 Jan 2024")
    """
    if isinstance(date_value, datetime):
        # Format as "DD MMM YYYY" (e.g., "15 Jan 2024")
        return date_value.strftime("%d %b %Y")
    return str(date_value)


def format_number_for_email(value: float) -> str:
    """
    Format number with thousand separators and 2 decimal places.
    
    Example: 1234.56 -> "1,234.56"
    
    Args:
        value: Numeric value
    
    Returns:
        Formatted string with thousand separators
    """
    try:
        num_value = float(value)
        # Format with thousand separators and 2 decimal places
        return f"{num_value:,.2f}"
    except (ValueError, TypeError):
        return "0.00"


def generate_email_subject(report_date: datetime) -> str:
    """
    Generate email subject line.
    
    Format: "Inventory Report – Priority Items (Data as of <DD MMM YYYY>)"
    
    Args:
        report_date: Report date (typically S3 file LastModified)
    
    Returns:
        Formatted subject line string
    """
    date_str = format_date_for_email(report_date)
    return f"Inventory Report – Priority Items (Data as of {date_str})"


def generate_inventory_email_body(
    priority_items_df: pd.DataFrame,
    report_date: datetime,
    erp_url: str,
    recipient_name: Optional[str] = None
) -> str:
    """
    Generate HTML email body for inventory report.
    
    Args:
        priority_items_df: DataFrame with columns ['Specification', 'Total_Free_For_Sale_MT']
                          Should contain top 15 items, already sorted descending
        report_date: Report date (typically S3 file LastModified)
        erp_url: URL to ERP system
        recipient_name: Optional recipient name for personalized greeting
    
    Returns:
        HTML string ready to send via email
    """
    logger.info("Generating HTML email body")
    
    # Validate inputs
    if priority_items_df is None or priority_items_df.empty:
        logger.warning("Priority items DataFrame is empty. Generating email with empty table.")
        priority_items_df = pd.DataFrame(columns=['Specification', 'Total_Free_For_Sale_MT'])
    
    # Ensure we have required columns
    required_columns = ['Specification', 'Total_Free_For_Sale_MT']
    missing_columns = [col for col in required_columns if col not in priority_items_df.columns]
    if missing_columns:
        error_msg = f"Priority items DataFrame missing required columns: {missing_columns}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Limit to top 15 items (should already be limited, but ensure)
    display_df = priority_items_df.head(15).copy()
    
    # Format report date
    date_str = format_date_for_email(report_date)
    
    # Generate greeting
    if recipient_name:
        greeting = f"Hi {recipient_name},"
    else:
        greeting = "Hi there,"
    
    # Generate priority items table rows
    table_rows = []
    for _, row in display_df.iterrows():
        spec = str(row['Specification']).strip()
        mt_value = format_number_for_email(row['Total_Free_For_Sale_MT'])
        
        table_rows.append(f"""
        <tr>
            <td style="padding: 8px; border: 1px solid #ddd;">{spec}</td>
            <td style="padding: 8px; border: 1px solid #ddd; text-align: right;">{mt_value}</td>
        </tr>""")
    
    # If no items, add a placeholder row
    if not table_rows:
        table_rows.append("""
        <tr>
            <td colspan="2" style="padding: 8px; border: 1px solid #ddd; text-align: center; color: #666;">
                No priority items found above threshold.
            </td>
        </tr>""")
    
    table_rows_html = "".join(table_rows)
    
    # Generate PDF specifications list (bullet points)
    pdf_specs_list = "".join([f"<li>{spec}</li>" for spec in PDF_SPECIFICATIONS])
    
    # Build HTML email body
    html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6; color: #333; margin: 0; padding: 20px;">
    
    <p>{greeting}</p>
    
    <h2 style="color: #2c3e50; margin-top: 20px; margin-bottom: 10px;">Inventory Report - Priority Items for Sales Focus</h2>
    <p style="color: #666; margin-bottom: 20px;"><strong>{date_str}</strong></p>
    
    <p>This report highlights inventory items that currently require priority sales focus based on available Free-For-Sale quantities.</p>
    
    <p>The information shared below represents a point-in-time snapshot of inventory as of the date mentioned above.</p>
    
    <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px; border: 1px solid #ddd;">
        <thead>
            <tr style="background-color: #333; color: white;">
                <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Specification</th>
                <th style="padding: 10px; border: 1px solid #ddd; text-align: right;">Free-For-Sale (MT)</th>
            </tr>
        </thead>
        <tbody>
            {table_rows_html}
        </tbody>
    </table>
    
    <p style="margin-top: 20px; margin-bottom: 20px;">
        <strong>Note:</strong><br>
        Items are ordered from highest to lowest available inventory, indicating increasing urgency for sales focus.<br>
        This snapshot of inventory data is as of the date mentioned above.
    </p>
    
    <p style="margin-top: 20px; margin-bottom: 20px;">
        For real-time inventory levels, reservations, and incoming stock updates, please refer to the ERP system using the link below:
    </p>
    
    <p style="margin-top: 10px; margin-bottom: 20px;">
        <a href="{erp_url}" style="color: #0066cc; text-decoration: none; font-weight: bold;">View Live Inventory in ERP →</a>
    </p>
    
    <p style="margin-top: 30px; margin-bottom: 10px;">
        Attached to this email are detailed inventory reports providing a breakdown of Free-For-Sale stock across schedules and categories for the following grades:
    </p>
    
    <ul style="margin-top: 10px; margin-bottom: 20px; padding-left: 20px;">
        {pdf_specs_list}
    </ul>
    
    <p style="margin-top: 30px; margin-bottom: 5px;">
        Regards,<br>
        <strong>Evergreen Analytics</strong>
    </p>
    
    <p style="margin-top: 20px; padding-top: 20px; border-top: 1px solid #eee; color: #666; font-size: 12px;">
        <em>This is a system-generated email. For the latest and most accurate data, please rely on the ERP as the source of truth.</em>
    </p>
    
</body>
</html>
"""
    
    logger.info(f"Generated HTML email body ({len(html_body)} characters)")
    logger.debug(f"Email body includes {len(display_df)} priority items")
    
    return html_body

