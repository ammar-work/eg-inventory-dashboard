"""
PDF Generator for Automated Inventory Reporting

This module generates multi-page PDF reports following the exact wireframe structure.
It embeds heatmap images and displays summary metrics for each specification.

IMPORTANT: 
- This module does NOT modify heatmap logic or styling - it only assembles PDF pages.
- Generated PDFs are stored persistently in reports/ directory and are NOT deleted after email sending.
- PDFs serve as permanent artifacts for manual review, debugging, and potential re-sending.
- Filenames are deterministic based on file upload date (format: inventory_report_YYYYMMDD.pdf).
"""

import os
from typing import Dict, List, Tuple, Optional
from datetime import datetime

try:
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.units import mm
    from reportlab.pdfgen import canvas
    from reportlab.lib.utils import ImageReader
    from PIL import Image
except ImportError:
    # ReportLab or Pillow not installed
    A4 = None
    landscape = None
    mm = None
    canvas = None
    ImageReader = None
    Image = None

# Import config values
try:
    from reporting.config import (
        REPORT_TITLE,
        PDF_SPECIFICATIONS,
        REPORTS_DIR,
        PDF_FILENAME_PREFIX,
        DATE_FORMAT_DISPLAY,
        DATE_FORMAT_FILENAME,
        PDF_PAGE_SIZE
    )
except ImportError:
    # Fallback if config not available
    REPORT_TITLE = "INVENTORY REPORT"
    PDF_SPECIFICATIONS = ["CSSMP106B", "ASSMPP11", "ASSMPP22", "ASSMPP9", "ASSMPP5", "ASSMPP91"]
    REPORTS_DIR = "reports"
    PDF_FILENAME_PREFIX = "inventory_report_"
    DATE_FORMAT_DISPLAY = "%d-%b-%Y"
    DATE_FORMAT_FILENAME = "%Y%m%d"
    PDF_PAGE_SIZE = "A4"

from reporting.logger import get_logger

logger = get_logger(__name__)

# ============================================================================
# PDF Generation Constants
# ============================================================================

# Page dimensions (A4 landscape)
PAGE_WIDTH = landscape(A4)[0] if landscape else 842  # points
PAGE_HEIGHT = landscape(A4)[1] if landscape else 595  # points

# Margins (20mm = ~56.7 points)
MARGIN = 20 * mm if mm else 56.7
LEFT_MARGIN = MARGIN
RIGHT_MARGIN = PAGE_WIDTH - MARGIN
TOP_MARGIN = PAGE_HEIGHT - MARGIN
BOTTOM_MARGIN = MARGIN

# Content area
CONTENT_WIDTH = PAGE_WIDTH - (2 * MARGIN)
CONTENT_HEIGHT = PAGE_HEIGHT - (2 * MARGIN)

# Font sizes
TITLE_FONT_SIZE = 24
HEADING_FONT_SIZE = 18
SECTION_FONT_SIZE = 14
BODY_FONT_SIZE = 12
METRIC_FONT_SIZE = 11

# Spacing
LINE_SPACING = 20
SECTION_SPACING = 30
METRIC_SPACING = 15


# ============================================================================
# Helper Functions
# ============================================================================

def format_date(date_value) -> str:
    """
    Format date value to display format (DD-MMM-YYYY).
    
    Args:
        date_value: datetime object or string
    
    Returns:
        Formatted date string
    """
    if isinstance(date_value, datetime):
        return date_value.strftime(DATE_FORMAT_DISPLAY)
    elif isinstance(date_value, str):
        try:
            # Try to parse if it's a string
            dt = datetime.strptime(date_value, DATE_FORMAT_DISPLAY)
            return dt.strftime(DATE_FORMAT_DISPLAY)
        except ValueError:
            return str(date_value)
    else:
        return str(date_value)


def validate_image_file(image_path: str) -> Tuple[bool, Optional[str]]:
    """
    Validate that image file exists and is readable.
    
    Args:
        image_path: Path to image file
    
    Returns:
        Tuple of (is_valid: bool, error_message: str or None)
    """
    if not image_path:
        return False, "Image path is empty"
    
    if not os.path.exists(image_path):
        return False, f"Image file does not exist: {image_path}"
    
    if not os.path.isfile(image_path):
        return False, f"Image path is not a file: {image_path}"
    
    # Check file size
    try:
        file_size = os.path.getsize(image_path)
        if file_size == 0:
            return False, f"Image file is empty: {image_path}"
    except OSError as e:
        return False, f"Cannot read image file: {str(e)}"
    
    # Try to open with PIL to verify it's a valid image
    if Image:
        try:
            img = Image.open(image_path)
            img.verify()
        except Exception as e:
            return False, f"Invalid image file: {str(e)}"
    
    return True, None


# ============================================================================
# Main PDF Generation Function
# ============================================================================

def generate_inventory_pdf(
    heatmap_images_by_spec: Dict[str, str],
    metrics_by_spec: Dict[str, Dict],
    specifications: List[str],
    report_date: datetime,
    output_dir: Optional[str] = None
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Generate multi-page PDF report following the exact wireframe structure.
    
    Structure:
    - Page 1: Cover + Table of Contents
    - Pages 2-7: One specification per page (heatmap + summary metrics)
    
    Args:
        heatmap_images_by_spec: Dictionary mapping specification to image path
            Example: {"CSSMP106B": "reports/temp_heatmap_CSSMP106B.png", ...}
        metrics_by_spec: Dictionary mapping specification to metrics dict
            Example: {"CSSMP106B": {"stock": 1500.0, "reservation": 200.0, ...}, ...}
        specifications: List of 6 specifications in order (must match PDF_SPECIFICATIONS)
        report_date: File upload date (NOT execution date) - used for PDF header
        output_dir: Output directory for PDF (default: from config)
    
    Returns:
        Tuple of (success: bool, pdf_path: str or None, error_message: str or None)
        - success: True if PDF was generated successfully
        - pdf_path: Full path to generated PDF file (or None if failed)
        - error_message: Error message string (or None if successful)
    
    Raises:
        ValueError: If required inputs are missing or invalid
        ImportError: If reportlab or Pillow libraries are not installed
        IOError: If output directory cannot be created or written to
    """
    try:
        # Validate inputs
        if not specifications or len(specifications) != 6:
            raise ValueError(f"Must provide exactly 6 specifications, got {len(specifications) if specifications else 0}")
        
        if not heatmap_images_by_spec:
            raise ValueError("heatmap_images_by_spec cannot be empty")
        
        if not metrics_by_spec:
            raise ValueError("metrics_by_spec cannot be empty")
        
        if report_date is None:
            raise ValueError("report_date cannot be None")
        
        # Check if reportlab is available
        if canvas is None:
            raise ImportError(
                "reportlab library is not installed. "
                "Please install it with: pip install reportlab"
            )
        
        if Image is None:
            raise ImportError(
                "Pillow library is not installed. "
                "Please install it with: pip install Pillow"
            )
        
        # Validate all required images exist
        logger.info("Validating heatmap images...")
        for spec in specifications:
            if spec not in heatmap_images_by_spec:
                raise ValueError(f"Heatmap image missing for specification: {spec}")
            
            image_path = heatmap_images_by_spec[spec]
            is_valid, error_msg = validate_image_file(image_path)
            if not is_valid:
                raise ValueError(f"Invalid heatmap image for {spec}: {error_msg}")
        
        # Validate all required metrics exist
        logger.info("Validating metrics...")
        for spec in specifications:
            if spec not in metrics_by_spec:
                raise ValueError(f"Metrics missing for specification: {spec}")
            
            metrics = metrics_by_spec[spec]
            required_keys = ['stock', 'reservation', 'incoming', 'free_for_sale']
            missing_keys = [key for key in required_keys if key not in metrics]
            if missing_keys:
                raise ValueError(f"Missing metric keys for {spec}: {missing_keys}")
        
        # Determine output directory
        if output_dir is None:
            output_dir = REPORTS_DIR
        
        # Ensure output directory exists
        try:
            os.makedirs(output_dir, exist_ok=True)
            logger.debug(f"Output directory ensured: {output_dir}")
        except OSError as e:
            error_msg = f"Failed to create output directory '{output_dir}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            return False, None, error_msg
        
        # Generate PDF filename
        # IMPORTANT: Filename is deterministic based on file upload date
        # Format: inventory_report_YYYYMMDD.pdf (e.g., inventory_report_20240115.pdf)
        # This ensures same date = same filename, allowing for manual review and re-sending
        date_str = format_date(report_date)
        filename_date = report_date.strftime(DATE_FORMAT_FILENAME) if isinstance(report_date, datetime) else datetime.now().strftime(DATE_FORMAT_FILENAME)
        pdf_filename = f"{PDF_FILENAME_PREFIX}{filename_date}.pdf"
        pdf_path = os.path.join(output_dir, pdf_filename)
        
        logger.info(f"Generating PDF report: {pdf_path}")
        logger.info(f"Report date: {format_date(report_date)}")
        logger.info(f"PDF will be stored persistently in {output_dir} for manual review and potential re-sending")
        
        # Create PDF canvas
        c = canvas.Canvas(pdf_path, pagesize=landscape(A4))
        
        try:
            # ====================================================================
            # PAGE 1: Cover + Table of Contents
            # ====================================================================
            logger.info("Creating Page 1: Cover + Table of Contents")
            
            # Title
            c.setFont("Helvetica-Bold", TITLE_FONT_SIZE)
            title_width = c.stringWidth(REPORT_TITLE, "Helvetica-Bold", TITLE_FONT_SIZE)
            title_x = (PAGE_WIDTH - title_width) / 2
            c.drawString(title_x, TOP_MARGIN - 30, REPORT_TITLE)
            
            # Date (below title)
            date_str = format_date(report_date)
            c.setFont("Helvetica", BODY_FONT_SIZE)
            date_width = c.stringWidth(date_str, "Helvetica", BODY_FONT_SIZE)
            date_x = (PAGE_WIDTH - date_width) / 2
            c.drawString(date_x, TOP_MARGIN - 60, date_str)
            
            # Table of Contents heading
            toc_y = TOP_MARGIN - 120
            c.setFont("Helvetica-Bold", HEADING_FONT_SIZE)
            toc_heading = "TABLE OF CONTENTS"
            toc_heading_width = c.stringWidth(toc_heading, "Helvetica-Bold", HEADING_FONT_SIZE)
            toc_heading_x = (PAGE_WIDTH - toc_heading_width) / 2
            c.drawString(toc_heading_x, toc_y, toc_heading)
            
            # Table of Contents entries
            toc_y -= 60
            # Increased font size for TOC entries for better readability
            TOC_ENTRY_FONT_SIZE = 14
            c.setFont("Helvetica", TOC_ENTRY_FONT_SIZE)
            TOC_LINE_SPACING = 28  # Increased spacing between entries
            for i, spec in enumerate(specifications, 1):
                page_num = i + 1  # Pages 2-7
                entry_text = f"{i}. {spec}"
                dots = "." * (50 - len(entry_text) - len(str(page_num)))
                full_entry = f"{entry_text} {dots} {page_num}"
                
                c.drawString(LEFT_MARGIN, toc_y, full_entry)
                toc_y -= TOC_LINE_SPACING
            
            # Page number (bottom-right corner)
            c.setFont("Helvetica", 10)
            page_text = "Page 1"
            page_width = c.stringWidth(page_text, "Helvetica", 10)
            c.drawString(PAGE_WIDTH - 60, 20, page_text)
            
            c.showPage()
            
            # ====================================================================
            # PAGES 2-7: Specification Detail Pages
            # ====================================================================
            for page_num, spec in enumerate(specifications, 2):
                logger.info(f"Creating Page {page_num}: {spec}")
                
                # Header: Specification name (left only - page number removed from header)
                c.setFont("Helvetica-Bold", HEADING_FONT_SIZE)
                c.drawString(LEFT_MARGIN, TOP_MARGIN - 20, spec)
                
                # Section: HEATMAP TABLE
                y_pos = TOP_MARGIN - 60
                c.setFont("Helvetica-Bold", SECTION_FONT_SIZE)
                section_title = "HEATMAP TABLE (Free For Sale MT)"
                c.drawString(LEFT_MARGIN, y_pos, section_title)
                
                # Load and embed heatmap image
                image_path = heatmap_images_by_spec[spec]
                y_pos -= 20
                
                try:
                    # Open image to get dimensions
                    img = Image.open(image_path)
                    img_width, img_height = img.size
                    
                    # Calculate scaling to fit in available space
                    # Leave space for summary metrics below (reduced from 150 to 120 for larger image)
                    available_width = CONTENT_WIDTH
                    available_height = y_pos - BOTTOM_MARGIN - 120
                    
                    # Calculate scale to fit both dimensions
                    # Allow upscaling for better readability (removed 1.0 cap)
                    scale_x = available_width / img_width
                    scale_y = available_height / img_height
                    # Use minimum scale but allow upscaling up to 1.5x for better readability
                    base_scale = min(scale_x, scale_y)
                    scale = min(base_scale, 1.5)  # Cap at 1.5x to avoid excessive upscaling
                    
                    # Calculate final dimensions
                    final_width = img_width * scale
                    final_height = img_height * scale
                    
                    # Center horizontally
                    image_x = LEFT_MARGIN + (CONTENT_WIDTH - final_width) / 2
                    image_y = y_pos - final_height
                    
                    # Draw image
                    c.drawImage(
                        ImageReader(image_path),
                        image_x,
                        image_y,
                        width=final_width,
                        height=final_height,
                        preserveAspectRatio=True
                    )
                    
                    logger.debug(f"Embedded image for {spec}: {final_width:.1f}x{final_height:.1f} points")
                    
                except Exception as e:
                    error_msg = f"Failed to embed image for {spec}: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    raise ValueError(error_msg)
                
                # Summary Metrics section
                metrics_y = image_y - 40
                c.setFont("Helvetica-Bold", SECTION_FONT_SIZE)
                metrics_title = "SUMMARY METRICS (Total)"
                c.drawString(LEFT_MARGIN, metrics_y, metrics_title)
                
                # Metrics box
                metrics_y -= 10
                box_height = 80
                box_width = 400
                box_x = LEFT_MARGIN
                box_y = metrics_y - box_height
                
                # Draw box border
                c.setStrokeColorRGB(0, 0, 0)
                c.setLineWidth(1)
                c.rect(box_x, box_y, box_width, box_height)
                
                # Metrics content
                metrics = metrics_by_spec[spec]
                metric_labels = [
                    ("Stock", metrics.get('stock', 0.0)),
                    ("Reservation", metrics.get('reservation', 0.0)),
                    ("Incoming", metrics.get('incoming', 0.0)),
                    ("Free For Sale", metrics.get('free_for_sale', 0.0))
                ]
                
                c.setFont("Helvetica", METRIC_FONT_SIZE)
                metric_y = box_y + box_height - 20
                # Fixed label position for proper alignment
                label_x = box_x + 15
                value_x = box_x + 180  # Fixed position for values to ensure alignment
                for label, value in metric_labels:
                    # Format value to 2 decimal places
                    value_str = f"{float(value):.2f} MT"
                    # Draw label and value separately for proper alignment
                    c.drawString(label_x, metric_y, f"{label}:")
                    c.drawString(value_x, metric_y, value_str)
                    metric_y -= METRIC_SPACING
                
                # Page number (bottom-right corner)
                c.setFont("Helvetica", 10)
                page_text = f"Page {page_num}"
                page_width = c.stringWidth(page_text, "Helvetica", 10)
                c.drawString(PAGE_WIDTH - 60, 20, page_text)
                
                c.showPage()
            
            # Save PDF
            c.save()
            logger.info(f"PDF generated successfully: {pdf_path}")
            
            # Verify file was created
            if not os.path.exists(pdf_path):
                error_msg = f"PDF file was not created at expected path: {pdf_path}"
                logger.error(error_msg)
                return False, None, error_msg
            
            # Check file size
            file_size = os.path.getsize(pdf_path)
            if file_size == 0:
                error_msg = f"Generated PDF file is empty: {pdf_path}"
                logger.error(error_msg)
                # Clean up empty file
                try:
                    os.remove(pdf_path)
                except Exception:
                    pass
                return False, None, error_msg
            
            logger.info(f"PDF file created successfully: {pdf_path} ({file_size} bytes)")
            logger.info(f"PDF stored persistently - will NOT be deleted after email sending")
            return True, pdf_path, None
            
        except Exception as e:
            # Clean up partial PDF on error
            if os.path.exists(pdf_path):
                try:
                    os.remove(pdf_path)
                    logger.debug(f"Cleaned up partial PDF file: {pdf_path}")
                except Exception:
                    pass
            
            raise  # Re-raise to be caught by outer exception handler
        
    except ValueError as e:
        error_msg = f"ValueError in PDF generation: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, None, error_msg
    
    except ImportError as e:
        error_msg = f"ImportError in PDF generation: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, None, error_msg
    
    except OSError as e:
        error_msg = f"OSError in PDF generation: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, None, error_msg
    
    except Exception as e:
        error_msg = f"Unexpected error in PDF generation: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, None, error_msg

