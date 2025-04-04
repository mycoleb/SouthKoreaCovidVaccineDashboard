"""
Data visualization module for the South Korea COVID-19 vaccination analysis project.
"""

import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from matplotlib.ticker import FuncFormatter
from datetime import datetime, timedelta

from config import (
    OUTPUT_DIR, REGIONS, VACCINE_TYPES, COLOR_PALETTE, 
    VISUALIZATION_DPI, FIGURE_SIZE
)
from utils.logging_utils import setup_logger, log_function_call, log_error

# Set up logger
logger = setup_logger(__name__)

# Set plot style
plt.style.use('ggplot')
sns.set_style("whitegrid")

class VaccinationVisualizer:
    """Class to create visualizations for COVID-19 vaccination data in South Korea."""
    
    def __init__(self):
        """
        
        # Save the dashboard to a file
        output_file = os.path.join(self.output_dir, 'dashboard.html')
        with open(output_file, 'w') as f:
            f.write(html_content)
        
        logger.info(f"Saved dashboard to {output_file}")
        return output_fileInitialize the visualizer."""
        self.output_dir = OUTPUT_DIR
        os.makedirs(self.output_dir, exist_ok=True)
    
    @log_function_call(logger)
    def visualize_vaccination_progress(self, vaccination_df):
        """
        Create visualizations for vaccination progress over time.
        
        Args:
            vaccination_df (pandas.DataFrame): Processed vaccination data
            
        Returns:
            str: Path to saved visualization
        """
        logger.info("Creating vaccination progress visualization")
        
        # Convert date to datetime if needed
        if isinstance(vaccination_df['date'].iloc[0], str):
            vaccination_df['date'] = pd.to_datetime(vaccination_df['date'])
        
        # Set up the figure
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=FIGURE_SIZE, sharex=True)
        fig.suptitle('COVID-19 Vaccination Progress in South Korea', fontsize=16)
        
        # Plot daily vaccinations (7-day average)
        ax1.plot(vaccination_df['date'], vaccination_df['daily_first_dose_7d_avg'], 
                label='First Dose', color=COLOR_PALETTE['First Dose'])
        ax1.plot(vaccination_df['date'], vaccination_df['daily_second_dose_7d_avg'], 
                label='Second Dose', color=COLOR_PALETTE['Second Dose'])
        ax1.plot(vaccination_df['date'], vaccination_df['daily_booster_7d_avg'], 
                label='Booster', color=COLOR_PALETTE['Booster'])
        
        # Format the first axis
        ax1.set_ylabel('Daily Vaccinations (7-day avg)')
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)
        
        # Add thousands separator to y-axis
        ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{int(x):,}'))
        
        # Plot cumulative percentage
        ax2.plot(vaccination_df['date'], vaccination_df['first_dose_percentage'], 
                label='First Dose', color=COLOR_PALETTE['First Dose'])
        ax2.plot(vaccination_df['date'], vaccination_df['second_dose_percentage'], 
                label='Second Dose', color=COLOR_PALETTE['Second Dose'])
        ax2.plot(vaccination_df['date'], vaccination_df['booster_percentage'], 
                label='Booster', color=COLOR_PALETTE['Booster'])
        
        # Format the second axis
        ax2.set_xlabel('Date')
        ax2.set_ylabel('Percentage of Population')
        ax2.legend(loc='upper left')
        ax2.grid(True, alpha=0.3)
        
        # Add percentage to y-axis
        ax2.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:.0f}%'))
        
        # Format x-axis dates
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        plt.xticks(rotation=45)
        
        # Add a horizontal line at 70% for herd immunity threshold
        ax2.axhline(y=70, color='r', linestyle='--', alpha=0.7)
        ax2.text(vaccination_df['date'].iloc[10], 71, 'Herd Immunity Threshold (70%)', 
                color='r', alpha=0.7)
        
        plt.tight_layout()
        
        # Save the figure
        output_file = os.path.join(self.output_dir, 'vaccination_progress.png')
        plt.savefig(output_file, dpi=VISUALIZATION_DPI)
        plt.close()
        
        logger.info(f"Saved vaccination progress visualization to {output_file}")
        return output_file
    
    @log_function_call(logger)
    def visualize_regional_comparison(self, regional_df):
        """
        Create visualizations comparing vaccination rates across regions.
        
        Args:
            regional_df (pandas.DataFrame): Processed regional data
            
        Returns:
            str: Path to saved visualization
        """
        logger.info("Creating regional comparison visualization")
        
        # Set up the figure
        fig, ax = plt.subplots(figsize=FIGURE_SIZE)
        fig.suptitle('COVID-19 Vaccination Rates by Region in South Korea', fontsize=16)
        
        # Sort regions by first dose percentage
        sorted_df = regional_df.sort_values('first_dose_percentage', ascending=False)
        
        # Create a horizontal bar chart
        x = np.arange(len(sorted_df))
        width = 0.25
        
        # Plot each dose type
        ax.barh(x - width, sorted_df['first_dose_percentage'], width, 
               label='First Dose', color=COLOR_PALETTE['First Dose'])
        ax.barh(x, sorted_df['second_dose_percentage'], width, 
               label='Second Dose', color=COLOR_PALETTE['Second Dose'])
        ax.barh(x + width, sorted_df['booster_percentage'], width, 
               label='Booster', color=COLOR_PALETTE['Booster'])
        
        # Set the y-axis tick labels to region names
        ax.set_yticks(x)
        ax.set_yticklabels(sorted_df['region'])
        
        # Add percentage to x-axis
        ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:.0f}%'))
        
        # Set axis labels and legend
        ax.set_xlabel('Percentage of Population')
        ax.set_title('Vaccination Rates by Region')
        ax.legend(loc='lower right')
        
        # Add a grid for better readability
        ax.grid(True, axis='x', alpha=0.3)
        
        plt.tight_layout()
        
        # Save the figure
        output_file = os.path.join(self.output_dir, 'regional_comparison.png')
        plt.savefig(output_file, dpi=VISUALIZATION_DPI)
        plt.close()
        
        logger.info(f"Saved regional comparison visualization to {output_file}")
        return output_file
    
    @log_function_call(logger)
    def visualize_vaccine_distribution(self, vaccination_df):
        """
        Create visualizations showing the distribution of different vaccine types.
        
        Args:
            vaccination_df (pandas.DataFrame): Processed vaccination data
            
        Returns:
            str: Path to saved visualization
        """
        logger.info("Creating vaccine distribution visualization")
        
        # Get the latest date in the data
        latest_date = vaccination_df['date'].max()
        
        # Filter for the latest week of data
        week_ago = latest_date - timedelta(days=7)
        recent_data = vaccination_df[vaccination_df['date'] >= week_ago]
        
        # Calculate total doses by vaccine type for the recent period
        vaccine_totals = {}
        for vaccine in VACCINE_TYPES:
            vaccine_totals[vaccine] = recent_data[f'{vaccine}_daily'].sum()
        
        # Create a pie chart
        fig, ax = plt.subplots(figsize=(10, 10))
        fig.suptitle('COVID-19 Vaccine Distribution in South Korea (Last 7 Days)', fontsize=16)
        
        # Plot pie chart
        wedges, texts, autotexts = ax.pie(
            vaccine_totals.values(),
            labels=vaccine_totals.keys(),
            autopct='%1.1f%%',
            startangle=90,
            colors=[COLOR_PALETTE.get(vaccine, f'C{i}') for i, vaccine in enumerate(vaccine_totals.keys())]
        )
        
        # Make the text more readable
        plt.setp(autotexts, size=10, weight="bold")
        plt.setp(texts, size=12)
        
        # Add a title
        ax.set_title(f'Vaccine Distribution ({week_ago.strftime("%Y-%m-%d")} to {latest_date.strftime("%Y-%m-%d")})')
        
        # Equal aspect ratio ensures that pie is drawn as a circle
        ax.axis('equal')
        
        plt.tight_layout()
        
        # Save the figure
        output_file = os.path.join(self.output_dir, 'vaccine_distribution.png')
        plt.savefig(output_file, dpi=VISUALIZATION_DPI)
        plt.close()
        
        logger.info(f"Saved vaccine distribution visualization to {output_file}")
        return output_file
    
    @log_function_call(logger)
    def visualize_vaccination_impact(self, daily_stats_df):
        """
        Create visualizations showing the impact of vaccination on COVID-19 cases.
        
        Args:
            daily_stats_df (pandas.DataFrame): Processed daily statistics with vaccination data
            
        Returns:
            str: Path to saved visualization
        """
        logger.info("Creating vaccination impact visualization")
        
        # Convert date to datetime if needed
        if isinstance(daily_stats_df['date'].iloc[0], str):
            daily_stats_df['date'] = pd.to_datetime(daily_stats_df['date'])
        
        # Set up the figure
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=FIGURE_SIZE, sharex=True)
        fig.suptitle('Impact of Vaccination on COVID-19 Cases in South Korea', fontsize=16)
        
        # Plot vaccination percentages
        ax1.plot(daily_stats_df['date'], daily_stats_df['first_dose_percentage'], 
                label='First Dose', color=COLOR_PALETTE['First Dose'])
        ax1.plot(daily_stats_df['date'], daily_stats_df['second_dose_percentage'], 
                label='Second Dose', color=COLOR_PALETTE['Second Dose'])
        ax1.plot(daily_stats_df['date'], daily_stats_df['booster_percentage'], 
                label='Booster', color=COLOR_PALETTE['Booster'])
        
        # Format the first axis
        ax1.set_ylabel('Percentage of Population Vaccinated')
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)
        ax1.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:.0f}%'))
        
        # Plot daily cases (7-day average)
        ax2.plot(daily_stats_df['date'], daily_stats_df['daily_cases_7d_avg'], 
                label='Daily Cases (7-day avg)', color='red')
        
        # Format the second axis
        ax2.set_xlabel('Date')
        ax2.set_ylabel('Daily Cases (7-day avg)')
        ax2.legend(loc='upper right')
        ax2.grid(True, alpha=0.3)
        
        # Add thousands separator to y-axis
        ax2.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{int(x):,}'))
        
        # Format x-axis dates
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        plt.xticks(rotation=45)
        
        # Add vertical lines for significant vaccination milestones
        for threshold, label in [(10, '10% First Dose'), (50, '50% First Dose'), (70, '70% First Dose')]:
            try:
                milestone_date = daily_stats_df[daily_stats_df['first_dose_percentage'] >= threshold]['date'].min()
                if not pd.isna(milestone_date):
                    ax2.axvline(x=milestone_date, color='green', linestyle='--', alpha=0.7)
                    ax2.text(milestone_date, ax2.get_ylim()[1] * 0.9, label, 
                            rotation=90, verticalalignment='top', color='green', alpha=0.7)
            except:
                pass  # Skip if the milestone hasn't been reached yet
        
        plt.tight_layout()
        
        # Save the figure
        output_file = os.path.join(self.output_dir, 'vaccination_impact.png')
        plt.savefig(output_file, dpi=VISUALIZATION_DPI)
        plt.close()
        
        logger.info(f"Saved vaccination impact visualization to {output_file}")
        return output_file
    
    @log_function_call(logger)
    def create_regional_heatmap(self, regional_df):
        """
        Create a heatmap showing vaccination rates across regions.
        
        Args:
            regional_df (pandas.DataFrame): Processed regional data
            
        Returns:
            str: Path to saved visualization
        """
        logger.info("Creating regional heatmap visualization")
        
        # Prepare data for heatmap
        heatmap_data = regional_df[['region', 'first_dose_percentage', 'second_dose_percentage', 'booster_percentage']]
        heatmap_data = heatmap_data.set_index('region')
        
        # Set up the figure
        plt.figure(figsize=FIGURE_SIZE)
        
        # Create the heatmap
        ax = sns.heatmap(
            heatmap_data, 
            annot=True, 
            fmt='.1f', 
            cmap='Blues', 
            cbar_kws={'label': 'Percentage of Population (%)'}
        )
        
        # Set title and labels
        plt.title('COVID-19 Vaccination Rates by Region and Dose Type', fontsize=16)
        plt.xlabel('Dose Type')
        plt.ylabel('Region')
        
        # Set the column labels
        ax.set_xticklabels(['First Dose', 'Second Dose', 'Booster'])
        
        plt.tight_layout()
        
        # Save the figure
        output_file = os.path.join(self.output_dir, 'regional_heatmap.png')
        plt.savefig(output_file, dpi=VISUALIZATION_DPI)
        plt.close()
        
        logger.info(f"Saved regional heatmap to {output_file}")
        return output_file
    
    @log_function_call(logger)
    def create_dashboard(self, summary, vaccination_file, regional_file, distribution_file, impact_file):
        """
        Create an HTML dashboard with all visualizations.
        
        Args:
            summary (dict): Vaccination summary statistics
            vaccination_file (str): Path to vaccination progress visualization
            regional_file (str): Path to regional comparison visualization
            distribution_file (str): Path to vaccine distribution visualization
            impact_file (str): Path to vaccination impact visualization
            
        Returns:
            str: Path to saved dashboard
        """
        logger.info("Creating dashboard")
        
        # Create an HTML dashboard with paths relative to the output directory
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>South Korea COVID-19 Vaccination Dashboard</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 0;
                    padding: 20px;
                    background-color: #f5f5f5;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                }}
                .header {{
                    background-color: #003e74;
                    color: white;
                    padding: 20px;
                    border-radius: 5px;
                    margin-bottom: 20px;
                }}
                .summary {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 20px;
                    margin-bottom: 20px;
                }}
                .summary-card {{
                    background-color: white;
                    border-radius: 5px;
                    padding: 20px;
                    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                }}
                .summary-card h3 {{
                    margin-top: 0;
                    color: #003e74;
                }}
                .summary-card p {{
                    font-size: 24px;
                    font-weight: bold;
                    margin-bottom: 0;
                }}
                .grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
                    gap: 20px;
                }}
                .card {{
                    background-color: white;
                    border-radius: 5px;
                    padding: 20px;
                    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                }}
                .card h2 {{
                    margin-top: 0;
                    color: #003e74;
                }}
                .card img {{
                    max-width: 100%;
                    height: auto;
                    display: block;
                    margin: 0 auto;
                }}
                .footer {{
                    margin-top: 20px;
                    text-align: center;
                    color: #666;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>South Korea COVID-19 Vaccination Dashboard</h1>
                    <p>Data as of {summary['report_date']}</p>
                </div>
                
                <div class="summary">
                    <div class="summary-card">
                        <h3>First Dose</h3>
                        <p>{summary['first_dose_percentage']}%</p>
                    </div>
                    <div class="summary-card">
                        <h3>Second Dose</h3>
                        <p>{summary['second_dose_percentage']}%</p>
                    </div>
                    <div class="summary-card">
                        <h3>Booster</h3>
                        <p>{summary['booster_percentage']}%</p>
                    </div>
                    <div class="summary-card">
                        <h3>Daily Vaccinations (7-day avg)</h3>
                        <p>{summary['daily_vaccinations_last_week']:,}</p>
                    </div>
                </div>
                
                <div class="grid">
                    <div class="card">
                        <h2>Vaccination Progress Over Time</h2>
                        <img src="{os.path.basename(vaccination_file)}" alt="Vaccination Progress">
                    </div>
                    <div class="card">
                        <h2>Regional Comparison</h2>
                        <img src="{os.path.basename(regional_file)}" alt="Regional Comparison">
                    </div>
                    <div class="card">
                        <h2>Vaccine Distribution</h2>
                        <img src="{os.path.basename(distribution_file)}" alt="Vaccine Distribution">
                    </div>
                    <div class="card">
                        <h2>Impact on COVID-19 Cases</h2>
                        <img src="{os.path.basename(impact_file)}" alt="Vaccination Impact">
                    </div>
                </div>
                
                <div class="footer">
                    <p>South Korea COVID-19 Vaccination Analysis &copy; {datetime.now().year}</p>
                </div>
            </div>
        </body>
        </html>
        """
                # Save the dashboard to a file
        output_file = os.path.join(self.output_dir, 'dashboard.html')
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Saved dashboard to {output_file}")
        return output_file