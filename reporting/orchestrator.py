"""
Main Orchestrator Module

This module orchestrates the complete inventory reporting pipeline:
1. Load and preprocess inventory data
2. Generate priority items table
3. Generate heatmaps and images for PDF specifications
4. Generate PDF report
5. Send email with PDF attachment

This is pure orchestration/glue code - no business logic.
All business logic lives in the individual modules.
"""

import os
from datetime import datetime
from typing import Tuple, Optional
import pandas as pd

from reporting.config import (
    PDF_SPECIFICATIONS,
    EMAIL_RECIPIENTS,
    EMAIL_SUBJECT_TEMPLATE,
    DATE_FORMAT_DISPLAY,
    LOGS_DIR,
    LOG_FILENAME,
    ERP_SYSTEM_LINK
)
from reporting.data_preprocessor import preprocess_inventory_data
from reporting.priority_items_generator import generate_priority_items
from reporting.heatmap_generator import generate_heatmap_dataframe, generate_heatmap_image
from reporting.pdf_generator import generate_inventory_pdf
from reporting.email_sender import send_email
from reporting.email_body_generator import generate_inventory_email_body, generate_email_subject
from reporting.s3_file_fetcher import fetch_latest_inventory_file
from reporting.logger import get_logger

logger = get_logger(__name__)


def format_date(date_value: datetime) -> str:
    """
    Format date value to display format (DD-MMM-YYYY).
    
    Args:
        date_value: datetime object
    
    Returns:
        Formatted date string (e.g., "15-Jan-2024")
    """
    if isinstance(date_value, datetime):
        return date_value.strftime(DATE_FORMAT_DISPLAY)
    return str(date_value)


def run_inventory_reporting_pipeline(
    excel_file_path: Optional[str] = None,
    report_date: Optional[datetime] = None,
    dry_run_email: bool = True
) -> Tuple[bool, Optional[str]]:
    """
    Run the complete inventory reporting pipeline end-to-end.
    
    This function orchestrates all reporting steps:
    1. Load and preprocess inventory data
    2. Generate priority items table
    3. Generate heatmaps and images for each PDF specification
    4. Generate PDF report
    5. Send email with PDF attachment (if dry_run_email=False)
    
    Args:
        excel_file_path: Path to inventory Excel file. If None, fetches latest file from S3.
        report_date: File upload date (NOT execution date). If None, uses current date.
        dry_run_email: If True, skip email sending (log only). If False, send email.
    
    Returns:
        Tuple of (success: bool, result: Optional[str])
        - success: True if pipeline completed successfully, False otherwise
        - result: PDF file path on success, error message on failure
    
    Example:
        success, result = run_inventory_reporting_pipeline(
            excel_file_path="data/inventory.xlsx",
            report_date=datetime(2024, 1, 15),
            dry_run_email=True
        )
    """
    try:
        # Log pipeline start with configuration
        logger.info("=" * 70)
        logger.info("Starting Inventory Reporting Pipeline")
        logger.info("=" * 70)
        logger.info(f"Report date: {format_date(report_date) if report_date else 'Not provided (will use current date)'}")
        logger.info(f"Dry run email: {dry_run_email}")
        logger.info(f"Log file: {os.path.join(LOGS_DIR, LOG_FILENAME)}")
        logger.info("=" * 70)
        logger.info("")
        
        # Step 0: Resolve Excel file path (local override or S3 fetch)
        logger.info("STEP 0: Resolving inventory Excel file...")
        
        resolved_excel_path = None
        
        if excel_file_path:
            # Local file override provided - use it directly (for testing/development)
            logger.info(f"Using local Excel file override: {excel_file_path}")
            
            # Input validation: Check if local override file exists
            if not os.path.exists(excel_file_path):
                error_msg = f"Local Excel file not found: {excel_file_path}"
                logger.error(error_msg)
                return False, error_msg
            
            resolved_excel_path = excel_file_path
            logger.info(f"Inventory file resolved to: {resolved_excel_path}")
            
        else:
            # No local override - fetch latest file from S3
            logger.info("No local file path provided. Fetching latest inventory file from S3...")
            
            try:
                success, file_path, s3_last_modified, error = fetch_latest_inventory_file()
                
                if not success:
                    error_msg = f"Failed to fetch latest inventory file from S3: {error}"
                    logger.error(error_msg)
                    return False, error_msg
                
                resolved_excel_path = file_path
                logger.info(f"Inventory file resolved to: {resolved_excel_path}")
                
                # Use S3 LastModified timestamp as report date when in S3 mode
                if s3_last_modified is not None:
                    # Extract date from S3 LastModified timestamp
                    s3_report_date = s3_last_modified.date()
                    # Set report_date from S3 LastModified (as per requirements)
                    report_date = datetime.combine(s3_report_date, datetime.min.time())
                    logger.info(f"S3 file last modified at: {s3_last_modified}")
                    logger.info(f"Report date resolved from S3 metadata: {format_date(report_date)}")
                else:
                    logger.warning("S3 LastModified timestamp not available. Will use provided or default report date.")
                
            except Exception as e:
                error_msg = f"Exception while fetching file from S3: {str(e)}"
                logger.error(error_msg, exc_info=True)
                return False, error_msg
        
        logger.info("✓ Step 0 completed: Excel file resolved")
        logger.info("")
        
        # Step 1: Load and preprocess inventory data
        logger.info("STEP 1: Loading and preprocessing inventory data...")
        
        # Input validation: Check file extension
        
        # Input validation: Check file extension
        file_ext = os.path.splitext(resolved_excel_path)[1].lower()
        valid_extensions = ['.xlsx', '.xls']
        if file_ext not in valid_extensions:
            error_msg = f"Invalid file extension: {file_ext}. Expected one of: {valid_extensions}"
            logger.error(error_msg)
            return False, error_msg
        
        logger.info(f"Loading Excel file: {resolved_excel_path}")
        
        try:
            sheets = preprocess_inventory_data(resolved_excel_path)
        except ValueError as e:
            # This catches missing sheets or file opening errors
            error_msg = f"Failed to preprocess inventory data: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
        except Exception as e:
            error_msg = f"Unexpected error during preprocessing: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
        
        stock_df = sheets.get("Stock")
        reservations_df = sheets.get("Reservations")
        incoming_df = sheets.get("Incoming")
        
        # Input validation: Check if required sheets are present
        if stock_df is None or reservations_df is None or incoming_df is None:
            missing = []
            if stock_df is None:
                missing.append("Stock")
            if reservations_df is None:
                missing.append("Reservations")
            if incoming_df is None:
                missing.append("Incoming")
            error_msg = f"Missing required sheets in Excel file: {', '.join(missing)}"
            logger.error(error_msg)
            return False, error_msg
        
        # Input validation: Check if sheets are empty
        if len(stock_df) == 0:
            error_msg = "Stock sheet is empty (0 rows). Cannot generate report."
            logger.error(error_msg)
            return False, error_msg
        
        if len(reservations_df) == 0:
            logger.warning("Reservations sheet is empty (0 rows). Continuing with empty reservations.")
        
        if len(incoming_df) == 0:
            logger.warning("Incoming sheet is empty (0 rows). Continuing with empty incoming stock.")
        
        logger.info(f"Loaded data: Stock={len(stock_df)} rows, "
                   f"Reservations={len(reservations_df)} rows, "
                   f"Incoming={len(incoming_df)} rows")
        logger.info("✓ Step 1 completed: Data loaded and preprocessed")
        logger.info("")
        
        # Step 2: Generate priority items table
        logger.info("STEP 2: Generating priority items table...")
        
        try:
            success, priority_df, error = generate_priority_items(
                stock_df=stock_df,
                reservations_df=reservations_df,
                incoming_df=incoming_df
            )
            
            if not success:
                logger.warning(f"Priority items generation failed: {error}")
                logger.warning("Continuing with empty priority items list")
                priority_df = None
            else:
                logger.info(f"Generated {len(priority_df)} priority items")
                
                # Data sanity check: No specs found above threshold
                if len(priority_df) == 0:
                    logger.warning("No specifications found above priority threshold. This may indicate low inventory levels.")
        except Exception as e:
            logger.warning(f"Priority items generation raised exception: {str(e)}", exc_info=True)
            logger.warning("Continuing with empty priority items list")
            priority_df = None
        
        logger.info("✓ Step 2 completed: Priority items table generated")
        logger.info("")
        
        # Step 3: Generate heatmaps and images for each specification
        logger.info("STEP 3: Generating heatmaps and images...")
        logger.info(f"Processing {len(PDF_SPECIFICATIONS)} specifications: {PDF_SPECIFICATIONS}")
        
        heatmap_images_by_spec = {}
        metrics_by_spec = {}
        failed_specs = []
        
        for spec in PDF_SPECIFICATIONS:
            logger.info(f"  Processing specification: {spec}")
            
            try:
                # Generate heatmap DataFrame
                styled_df, metrics, error = generate_heatmap_dataframe(
                    stock_df=stock_df,
                    reservations_df=reservations_df,
                    incoming_df=incoming_df,
                    specification=spec
                )
                
                if error or styled_df is None:
                    error_msg = f"Failed to generate heatmap DataFrame for {spec}: {error}"
                    logger.error(error_msg)
                    failed_specs.append(spec)
                    continue
                
                # Store metrics
                if metrics:
                    metrics_by_spec[spec] = metrics
                    stock_val = metrics.get('stock', 0)
                    reservation_val = metrics.get('reservation', 0)
                    incoming_val = metrics.get('incoming', 0)
                    free_for_sale_val = metrics.get('free_for_sale', 0)
                    
                    logger.debug(f"  Metrics for {spec}: Stock={stock_val:.2f}, "
                               f"Reservation={reservation_val:.2f}, "
                               f"Incoming={incoming_val:.2f}, "
                               f"Free For Sale={free_for_sale_val:.2f}")
                    
                    # Data sanity checks (non-blocking warnings)
                    if incoming_val == 0:
                        logger.debug(f"  Note: Incoming MT is 0 for {spec}")
                    
                    if reservation_val > stock_val:
                        logger.warning(f"  Data check: Reservation MT ({reservation_val:.2f}) > Stock MT ({stock_val:.2f}) for {spec}")
                    
                    if free_for_sale_val < 0:
                        logger.warning(f"  Data check: Free For Sale MT is negative ({free_for_sale_val:.2f}) for {spec}")
                
                # Generate heatmap image
                success, image_path, image_error = generate_heatmap_image(
                    styled_dataframe=styled_df,
                    specification=spec
                )
                
                if not success or image_error:
                    error_msg = f"Failed to generate heatmap image for {spec}: {image_error}"
                    logger.error(error_msg)
                    failed_specs.append(spec)
                    continue
                
                heatmap_images_by_spec[spec] = image_path
                logger.info(f"  ✓ {spec}: Image generated at {image_path}")
                
            except Exception as e:
                error_msg = f"Exception while processing {spec}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                failed_specs.append(spec)
                continue
        
        # Validate we have at least some successful specifications
        if len(heatmap_images_by_spec) == 0:
            error_msg = f"Failed to generate heatmaps for all {len(PDF_SPECIFICATIONS)} specifications"
            logger.error(error_msg)
            if failed_specs:
                logger.error(f"Failed specifications: {failed_specs}")
            return False, error_msg
        
        if failed_specs:
            logger.warning(f"Failed to generate heatmaps for {len(failed_specs)} specifications: {failed_specs}")
            logger.warning(f"Continuing with {len(heatmap_images_by_spec)} successful specifications")
        
        # Data sanity check: Check if incoming MT is 0 for all specs
        all_incoming_zero = True
        for spec, metrics in metrics_by_spec.items():
            if metrics.get('incoming', 0) != 0:
                all_incoming_zero = False
                break
        if all_incoming_zero and len(metrics_by_spec) > 0:
            logger.warning("Data check: Incoming MT is 0 for all specifications. This may indicate missing incoming stock data.")
        
        logger.info(f"✓ Step 3 completed: Generated {len(heatmap_images_by_spec)} heatmap images")
        logger.info("")
        
        # Step 4: Generate PDF report
        logger.info("STEP 4: Generating PDF report...")
        
        # Use provided report_date or current date (fallback only)
        # Note: If S3 fetch was used, report_date should already be set from S3 LastModified
        if report_date is None:
            report_date = datetime.now()
            logger.warning("report_date not provided and not available from S3, using current date")
        
        logger.info(f"Final report date used: {format_date(report_date)}")
        
        try:
            success, pdf_path, pdf_error = generate_inventory_pdf(
                heatmap_images_by_spec=heatmap_images_by_spec,
                metrics_by_spec=metrics_by_spec,
                specifications=list(heatmap_images_by_spec.keys()),
                report_date=report_date
            )
            
            if not success or pdf_error:
                error_msg = f"Failed to generate PDF: {pdf_error}"
                logger.error(error_msg)
                return False, error_msg
            
            # PDF validation: Verify PDF file exists and is not empty
            if not os.path.exists(pdf_path):
                error_msg = f"PDF file was not created at expected path: {pdf_path}"
                logger.error(error_msg)
                return False, error_msg
            
            pdf_size = os.path.getsize(pdf_path)
            if pdf_size == 0:
                error_msg = f"Generated PDF file is empty (0 bytes): {pdf_path}"
                logger.error(error_msg)
                # Clean up empty file
                try:
                    os.remove(pdf_path)
                except Exception:
                    pass
                return False, error_msg
            
            # Count pages (approximate: 1 cover + 1 per spec)
            expected_pages = 1 + len(heatmap_images_by_spec)
            logger.info(f"PDF generated successfully: {pdf_path} ({pdf_size} bytes, ~{expected_pages} pages)")
        except Exception as e:
            error_msg = f"Exception while generating PDF: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
        
        logger.info("✓ Step 4 completed: PDF report generated")
        logger.info("")
        
        # Step 5: Send email (if not dry run)
        logger.info("STEP 5: Sending email...")
        
        if dry_run_email:
            logger.info("DRY RUN MODE: Email sending skipped (dry_run_email=True)")
            logger.info(f"Would send email to {len(EMAIL_RECIPIENTS)} recipients")
            email_subject = generate_email_subject(report_date)
            logger.info(f"Subject: {email_subject}")
            logger.info(f"Attachment: {pdf_path}")
            
            # Generate email body for dry-run preview (optional logging)
            try:
                if priority_df is None or priority_df.empty:
                    priority_df = pd.DataFrame(columns=['Specification', 'Total_Free_For_Sale_MT'])
                
                html_body_preview = generate_inventory_email_body(
                    priority_items_df=priority_df,
                    report_date=report_date,
                    erp_url=ERP_SYSTEM_LINK,
                    recipient_name=None
                )
                logger.debug(f"Email body preview generated ({len(html_body_preview)} characters)")
            except Exception as e:
                logger.debug(f"Could not generate email body preview: {str(e)}")
        else:
            logger.info(f"Sending email to {len(EMAIL_RECIPIENTS)} recipients")
            
            # Generate email subject using new format
            email_subject = generate_email_subject(report_date)
            logger.debug(f"Email subject: {email_subject}")
            
            # Generate HTML email body using email body generator
            try:
                # Ensure priority_df is a valid DataFrame (handle None case)
                if priority_df is None or priority_df.empty:
                    logger.warning("Priority items DataFrame is None or empty. Using empty DataFrame for email body.")
                    priority_df = pd.DataFrame(columns=['Specification', 'Total_Free_For_Sale_MT'])
                
                html_body = generate_inventory_email_body(
                    priority_items_df=priority_df,
                    report_date=report_date,
                    erp_url=ERP_SYSTEM_LINK,
                    recipient_name=None  # No personalization for now
                )
                logger.info("Generated HTML email body successfully")
            except Exception as e:
                error_msg = f"Failed to generate email body: {str(e)}"
                logger.error(error_msg, exc_info=True)
                # Fallback to simple HTML body
                html_body = f"""
                <html>
                <body>
                    <h1>Inventory Report</h1>
                    <p>Please find the weekly inventory report attached.</p>
                    <p>Report Date: {format_date(report_date)}</p>
                    <p><em>Note: Email body generation failed. Please refer to the attached PDF for details.</em></p>
                </body>
                </html>
                """
                logger.warning("Using fallback HTML email body")
            
            # Email validation: Check recipient list and attachment
            if not EMAIL_RECIPIENTS or len(EMAIL_RECIPIENTS) == 0:
                logger.warning("Email recipient list is empty. Skipping email send.")
                logger.warning("Pipeline completed successfully but no email was sent.")
                return True, pdf_path
            
            if pdf_path and not os.path.exists(pdf_path):
                logger.warning(f"PDF attachment not found: {pdf_path}. Skipping email send.")
                logger.warning("Pipeline completed successfully but no email was sent.")
                return True, pdf_path
            
            try:
                success, error = send_email(
                    to_emails=EMAIL_RECIPIENTS,
                    subject=email_subject,
                    html_body=html_body,
                    attachments=[pdf_path] if pdf_path else None
                )
                
                if not success:
                    error_msg = f"Failed to send email: {error}"
                    logger.warning(error_msg)
                    # Don't fail the entire pipeline if email fails - PDF was generated successfully
                    logger.warning("Pipeline completed but email sending failed")
                    return True, pdf_path  # Return success with PDF path
                
                logger.info("Email sent successfully")
            except Exception as e:
                error_msg = f"Exception while sending email: {str(e)}"
                logger.error(error_msg, exc_info=True)
                # Don't fail the entire pipeline if email fails - PDF was generated successfully
                logger.warning("Pipeline completed but email sending raised exception")
                return True, pdf_path  # Return success with PDF path
        
        logger.info("✓ Step 5 completed: Email sent (or skipped in dry run)")
        logger.info("")
        
        # Final summary
        logger.info("")
        logger.info("=" * 70)
        logger.info("Pipeline completed successfully")
        logger.info("=" * 70)
        logger.info(f"Final PDF Report: {pdf_path}")
        logger.info(f"Specifications processed: {len(heatmap_images_by_spec)}/{len(PDF_SPECIFICATIONS)}")
        if failed_specs:
            logger.warning(f"Failed specifications: {failed_specs}")
        logger.info(f"Priority items generated: {len(priority_df) if priority_df is not None else 0}")
        logger.info(f"Email sent: {'No (dry run)' if dry_run_email else 'Yes'}")
        logger.info("=" * 70)
        logger.info("")
        
        return True, pdf_path
        
    except Exception as e:
        error_msg = f"Unexpected error in reporting pipeline: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, error_msg

