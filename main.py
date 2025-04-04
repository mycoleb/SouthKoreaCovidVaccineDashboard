"""
Main script for the South Korea COVID-19 vaccination analysis project.
"""

import os
import argparse
import webbrowser
from pathlib import Path

from data_fetcher import VaccinationDataFetcher
from data_processor import VaccinationDataProcessor
from visualizer import VaccinationVisualizer
from utils.logging_utils import setup_logger, log_function_call, log_error

# Set up logger
logger = setup_logger('main')

@log_function_call(logger)
def run_vaccination_analysis(refresh_data=False, open_dashboard=True):
    """
    Run the complete vaccination analysis workflow.
    
    Args:
        refresh_data (bool): Whether to force refresh data from sources
        open_dashboard (bool): Whether to open the dashboard in browser after completion
        
    Returns:
        str: Path to the generated dashboard
    """
    logger.info("Starting South Korea COVID-19 vaccination analysis")
    
    # Initialize components
    fetcher = VaccinationDataFetcher()
    processor = VaccinationDataProcessor()
    visualizer = VaccinationVisualizer()
    
    # Fetch data
    logger.info("Fetching data...")
    vaccination_data = fetcher.fetch_vaccination_data(use_cache=not refresh_data)
    daily_stats = fetcher.fetch_daily_stats(use_cache=not refresh_data)
    regional_data = fetcher.fetch_regional_data(use_cache=not refresh_data)
    
    # Process data
    logger.info("Processing data...")
    processed_vaccination = processor.process_vaccination_data(vaccination_data)
    processed_daily_stats = processor.process_daily_stats(daily_stats, processed_vaccination)
    processed_regional = processor.process_regional_data(regional_data)
    
    # Generate summary
    summary = processor.generate_vaccination_summary(processed_vaccination, processed_regional)
    
    # Create visualizations
    logger.info("Creating visualizations...")
    vaccination_viz = visualizer.visualize_vaccination_progress(processed_vaccination)
    regional_viz = visualizer.visualize_regional_comparison(processed_regional)
    distribution_viz = visualizer.visualize_vaccine_distribution(processed_vaccination)
    impact_viz = visualizer.visualize_vaccination_impact(processed_daily_stats)
    regional_heatmap = visualizer.create_regional_heatmap(processed_regional)
    
    # Create dashboard
    logger.info("Creating dashboard...")
    dashboard = visualizer.create_dashboard(
        summary, 
        vaccination_viz, 
        regional_viz, 
        distribution_viz, 
        impact_viz
    )
    
    logger.info(f"Analysis complete. Dashboard saved to {dashboard}")
    
    # Open the dashboard in the default web browser
    if open_dashboard:
        logger.info("Opening dashboard in web browser")
        try:
            dashboard_url = Path(dashboard).as_uri()
            webbrowser.open(dashboard_url)
        except Exception as e:
            logger.error(f"Failed to open dashboard in browser: {e}")
    
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
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()
    
    # Run the analysis
    run_vaccination_analysis(
        refresh_data=args.refresh,
        open_dashboard=not args.no_browser
    )