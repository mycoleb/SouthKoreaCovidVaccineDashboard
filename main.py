"""
Main script for the South Korea COVID-19 vaccination analysis project.
"""

import os
import argparse
import webbrowser
import sys
import traceback
from pathlib import Path

from data_fetcher import VaccinationDataFetcher
from data_processor import VaccinationDataProcessor
from visualizer import VaccinationVisualizer
from utils.logging_utils import setup_logger, log_function_call, log_error

# Set up logger
logger = setup_logger('main')

@log_function_call(logger)
def run_vaccination_analysis(refresh_data=False, open_dashboard=True, retry_count=3):
    """
    Run the complete vaccination analysis workflow.
    
    Args:
        refresh_data (bool): Whether to force refresh data from sources
        open_dashboard (bool): Whether to open the dashboard in browser after completion
        retry_count (int): Number of retries for data fetching
        
    Returns:
        str: Path to the generated dashboard
    """
    logger.info("Starting South Korea COVID-19 vaccination analysis")
    
    # Initialize components
    fetcher = VaccinationDataFetcher()
    processor = VaccinationDataProcessor()
    visualizer = VaccinationVisualizer()
    
    # Fetch data with retries
    logger.info("Fetching data...")
    
    # Track overall fetch status and consolidated errors
    fetch_success = False
    error_messages = []
    
    for attempt in range(1, retry_count + 1):
        try:
            logger.info(f"Data fetch attempt {attempt}/{retry_count}")
            
            # Fetch each data type, with more detailed error handling
            try:
                vaccination_data = fetcher.fetch_vaccination_data(use_cache=not refresh_data)
                logger.info("✓ Successfully fetched vaccination data")
            except Exception as e:
                error_detail = f"Vaccination data fetch failed: {str(e)}\n{traceback.format_exc()}"
                logger.error(error_detail)
                error_messages.append(error_detail)
                raise RuntimeError("Failed to fetch vaccination data")
                
            try:
                daily_stats = fetcher.fetch_daily_stats(use_cache=not refresh_data)
                logger.info("✓ Successfully fetched daily statistics")
            except Exception as e:
                error_detail = f"Daily stats fetch failed: {str(e)}\n{traceback.format_exc()}"
                logger.error(error_detail)
                error_messages.append(error_detail)
                raise RuntimeError("Failed to fetch daily statistics")
                
            try:
                regional_data = fetcher.fetch_regional_data(use_cache=not refresh_data)
                logger.info("✓ Successfully fetched regional data")
            except Exception as e:
                error_detail = f"Regional data fetch failed: {str(e)}\n{traceback.format_exc()}"
                logger.error(error_detail)
                error_messages.append(error_detail)
                raise RuntimeError("Failed to fetch regional data")
            
            # If we get here, all fetches succeeded
            fetch_success = True
            break
            
        except Exception as e:
            logger.error(f"Data fetch attempt {attempt} failed: {str(e)}")
            
            # If this is not the last attempt, try again
            if attempt < retry_count:
                logger.info(f"Retrying in 5 seconds...")
                import time
                time.sleep(5)
            else:
                # This was the last attempt
                logger.critical(f"All {retry_count} data fetch attempts failed. Cannot proceed.")
    
    # Check if we were able to fetch the data
    if not fetch_success:
        error_details = "\n".join(error_messages)
        error_msg = (
            f"Failed to fetch required data after {retry_count} attempts.\n"
            f"Please check your internet connection and try again later.\n"
            f"Detailed errors:\n{error_details}"
        )
        logger.critical(error_msg)
        raise SystemExit(error_msg)
    
    # Process data
    logger.info("Processing data...")
    try:
        processed_vaccination = processor.process_vaccination_data(vaccination_data)
        logger.info("✓ Successfully processed vaccination data")
    except Exception as e:
        error_msg = f"Failed to process vaccination data: {str(e)}"
        logger.critical(error_msg)
        raise SystemExit(error_msg)
    
    try:
        processed_daily_stats = processor.process_daily_stats(daily_stats, processed_vaccination)
        logger.info("✓ Successfully processed daily statistics")
    except Exception as e:
        error_msg = f"Failed to process daily statistics: {str(e)}"
        logger.critical(error_msg)
        raise SystemExit(error_msg)
    
    try:
        processed_regional = processor.process_regional_data(regional_data)
        logger.info("✓ Successfully processed regional data")
    except Exception as e:
        error_msg = f"Failed to process regional data: {str(e)}"
        logger.critical(error_msg)
        raise SystemExit(error_msg)
    
    # Generate summary
    try:
        summary = processor.generate_vaccination_summary(processed_vaccination, processed_regional)
        logger.info("✓ Successfully generated summary")
    except Exception as e:
        error_msg = f"Failed to generate summary: {str(e)}"
        logger.critical(error_msg)
        raise SystemExit(error_msg)
    
    # Create visualizations
    logger.info("Creating visualizations...")
    try:
        vaccination_viz = visualizer.visualize_vaccination_progress(processed_vaccination)
        regional_viz = visualizer.visualize_regional_comparison(processed_regional)
        distribution_viz = visualizer.visualize_vaccine_distribution(processed_vaccination)
        impact_viz = visualizer.visualize_vaccination_impact(processed_daily_stats)
        regional_heatmap = visualizer.create_regional_heatmap(processed_regional)
        logger.info("✓ Successfully created all visualizations")
    except Exception as e:
        error_msg = f"Failed to create visualizations: {str(e)}\n{traceback.format_exc()}"
        logger.critical(error_msg)
        raise SystemExit(error_msg)
    
    # Create dashboard
    logger.info("Creating dashboard...")
    try:
        dashboard = visualizer.create_dashboard(
            summary, 
            vaccination_viz, 
            regional_viz, 
            distribution_viz, 
            impact_viz
        )
        logger.info(f"✓ Analysis complete. Dashboard saved to {dashboard}")
    except Exception as e:
        error_msg = f"Failed to create dashboard: {str(e)}\n{traceback.format_exc()}"
        logger.critical(error_msg)
        raise SystemExit(error_msg)
    
    # Open the dashboard in the default web browser
    if open_dashboard:
        logger.info("Opening dashboard in web browser")
        try:
            dashboard_url = Path(dashboard).as_uri()
            webbrowser.open(dashboard_url)
        except Exception as e:
            logger.error(f"Failed to open dashboard in browser: {e}")
            logger.info(f"Dashboard is available at: {dashboard}")
    
    return dashboard

def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="South Korea COVID-19 Vaccination Analysis")
    parser.add_argument('--refresh', action='store_true', help='Force refresh data from sources')
    parser.add_argument('--no-browser', action='store_true', help='Do not open the dashboard in a browser')
    parser.add_argument('--retry', type=int, default=3, help='Number of retry attempts for data fetching')
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()
    
    # Run the analysis
    try:
        run_vaccination_analysis(
            refresh_data=args.refresh,
            open_dashboard=not args.no_browser,
            retry_count=args.retry
        )
    except SystemExit as e:
        print(f"ERROR: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"Unhandled exception: {str(e)}")
        traceback.print_exc()
        sys.exit(1)