"""
Data processing module for the South Korea COVID-19 vaccination analysis project.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from config import PROCESSED_DATA_DIR, REGIONS, VACCINE_TYPES, DATE_FORMAT
from utils.logging_utils import setup_logger, log_function_call, log_error

# Set up logger
logger = setup_logger(__name__)

class VaccinationDataProcessor:
    """Class to process COVID-19 vaccination data for South Korea."""
    
    def __init__(self):
        """Initialize the data processor."""
        self.processed_data_dir = PROCESSED_DATA_DIR
        os.makedirs(self.processed_data_dir, exist_ok=True)
    
    @log_function_call(logger)
    def process_vaccination_data(self, vaccination_df):
        """
        Process vaccination data.
        
        Args:
            vaccination_df (pandas.DataFrame): Raw vaccination data
            
        Returns:
            pandas.DataFrame: Processed vaccination data
        """
        logger.info("Processing vaccination data")
        
        # Make a copy to avoid modifying the original
        df = vaccination_df.copy()
        
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Sort by date
        df = df.sort_values('date')
        
        # Calculate 7-day moving averages
        df['daily_first_dose_7d_avg'] = df['daily_first_dose'].rolling(window=7).mean().round()
        df['daily_second_dose_7d_avg'] = df['daily_second_dose'].rolling(window=7).mean().round()
        df['daily_booster_7d_avg'] = df['daily_booster'].rolling(window=7).mean().round()
        
        # Calculate daily total
        df['daily_total'] = df['daily_first_dose'] + df['daily_second_dose'] + df['daily_booster']
        df['daily_total_7d_avg'] = df['daily_total'].rolling(window=7).mean().round()
        
        # Calculate daily change rates
        for column in ['daily_first_dose', 'daily_second_dose', 'daily_booster', 'daily_total']:
            df[f'{column}_change'] = df[column].pct_change() * 100
        
        # Save processed data
        output_file = os.path.join(self.processed_data_dir, 'processed_vaccination_data.csv')
        df.to_csv(output_file, index=False)
        logger.info(f"Saved processed vaccination data to {output_file}")
        
        return df
    
    @log_function_call(logger)
    def process_daily_stats(self, daily_stats_df, vaccination_df):
        """
        Process daily COVID-19 statistics and combine with vaccination data.
        
        Args:
            daily_stats_df (pandas.DataFrame): Raw daily statistics
            vaccination_df (pandas.DataFrame): Processed vaccination data
            
        Returns:
            pandas.DataFrame: Processed daily statistics with vaccination correlation
        """
        logger.info("Processing daily stats")
        
        # Make copies to avoid modifying the originals
        stats_df = daily_stats_df.copy()
        vax_df = vaccination_df.copy()
        
        # Convert date columns to datetime
        stats_df['date'] = pd.to_datetime(stats_df['date'])
        if isinstance(vax_df['date'].iloc[0], str):
            vax_df['date'] = pd.to_datetime(vax_df['date'])
        
        # Sort by date
        stats_df = stats_df.sort_values('date')
        
        # Calculate 7-day moving averages
        stats_df['daily_cases_7d_avg'] = stats_df['daily_cases'].rolling(window=7).mean().round()
        stats_df['daily_deaths_7d_avg'] = stats_df['daily_deaths'].rolling(window=7).mean().round()
        stats_df['positivity_rate_7d_avg'] = stats_df['positivity_rate'].rolling(window=7).mean().round(2)
        
        # Merge with vaccination data
        merged_df = pd.merge(stats_df, vax_df[['date', 'first_dose_percentage', 'second_dose_percentage', 'booster_percentage']], 
                             on='date', how='left')
        
        # Fill NaN values for vaccination data (for dates before vaccination started)
        for col in ['first_dose_percentage', 'second_dose_percentage', 'booster_percentage']:
            merged_df[col] = merged_df[col].fillna(0)
        
        # Calculate correlation between vaccination rates and cases (using 14-day lag to account for immunity buildup)
        # We'll calculate this over the whole dataset
        df_corr = merged_df.copy()
        df_corr['cases_14d_later'] = df_corr['daily_cases'].shift(-14)
        
        # Only calculate correlation where we have both vaccination and future case data
        mask = (~df_corr['first_dose_percentage'].isna()) & (~df_corr['cases_14d_later'].isna()) & (df_corr['first_dose_percentage'] > 0)
        if mask.sum() > 10:  # Only calculate if we have enough data points
            first_dose_correlation = df_corr.loc[mask, 'first_dose_percentage'].corr(df_corr.loc[mask, 'cases_14d_later'])
            merged_df['first_dose_case_correlation'] = first_dose_correlation
        
        mask = (~df_corr['second_dose_percentage'].isna()) & (~df_corr['cases_14d_later'].isna()) & (df_corr['second_dose_percentage'] > 0)
        if mask.sum() > 10:
            second_dose_correlation = df_corr.loc[mask, 'second_dose_percentage'].corr(df_corr.loc[mask, 'cases_14d_later'])
            merged_df['second_dose_case_correlation'] = second_dose_correlation
        
        # Save processed data
        output_file = os.path.join(self.processed_data_dir, 'processed_daily_stats.csv')
        merged_df.to_csv(output_file, index=False)
        logger.info(f"Saved processed daily stats to {output_file}")
        
        return merged_df
    
    @log_function_call(logger)
    def process_regional_data(self, regional_df):
        """
        Process regional vaccination data.
        
        Args:
            regional_df (pandas.DataFrame): Raw regional data
            
        Returns:
            pandas.DataFrame: Processed regional data
        """
        logger.info("Processing regional data")
        
        # Make a copy to avoid modifying the original
        df = regional_df.copy()
        
        # Ensure regions are in the same order as in config
        df['region'] = pd.Categorical(df['region'], categories=REGIONS, ordered=True)
        df = df.sort_values('region')
        
        # Calculate national averages
        national_avg = {
            'first_dose_percentage': df['first_dose'].sum() / df['population'].sum() * 100,
            'second_dose_percentage': df['second_dose'].sum() / df['population'].sum() * 100,
            'booster_percentage': df['booster'].sum() / df['population'].sum() * 100
        }
        
        # Calculate difference from national average
        for metric in ['first_dose_percentage', 'second_dose_percentage', 'booster_percentage']:
            df[f'{metric}_diff_from_avg'] = df[metric] - national_avg[metric]
        
        # Calculate vaccination efficiency (doses per population)
        df['vaccination_efficiency'] = (df['first_dose'] + df['second_dose'] + df['booster']) / df['population']
        
        # Calculate ranking for each metric
        for metric in ['first_dose_percentage', 'second_dose_percentage', 'booster_percentage']:
            df[f'{metric}_rank'] = df[metric].rank(ascending=False).astype(int)
        
        # Save processed data
        output_file = os.path.join(self.processed_data_dir, 'processed_regional_data.csv')
        df.to_csv(output_file, index=False)
        logger.info(f"Saved processed regional data to {output_file}")
        
        return df
    
    @log_function_call(logger)
    def generate_vaccination_summary(self, vaccination_df, regional_df):
        """
        Generate a summary of vaccination progress.
        
        Args:
            vaccination_df (pandas.DataFrame): Processed vaccination data
            regional_df (pandas.DataFrame): Processed regional data
            
        Returns:
            dict: Summary statistics
        """
        logger.info("Generating vaccination summary")
        
        # Get the latest date in the vaccination data
        latest_date = vaccination_df['date'].max()
        latest_data = vaccination_df[vaccination_df['date'] == latest_date].iloc[0]
        
        # Calculate national percentages
        total_population = regional_df['population'].sum()
        total_first_dose = regional_df['first_dose'].sum()
        total_second_dose = regional_df['second_dose'].sum()
        total_booster = regional_df['booster'].sum()
        
        # Find top and bottom regions
        top_region_first_dose = regional_df.loc[regional_df['first_dose_percentage'].idxmax(), 'region']
        bottom_region_first_dose = regional_df.loc[regional_df['first_dose_percentage'].idxmin(), 'region']
        
        # Create summary dictionary
        summary = {
            'report_date': latest_date.strftime(DATE_FORMAT),
            'first_dose_percentage': round(total_first_dose / total_population * 100, 2),
            'second_dose_percentage': round(total_second_dose / total_population * 100, 2),
            'booster_percentage': round(total_booster / total_population * 100, 2),
            'daily_vaccinations_last_week': int(latest_data['daily_total_7d_avg']),
            'weekly_change_percentage': round(latest_data['daily_total_change'], 2),
            'top_region': top_region_first_dose,
            'bottom_region': bottom_region_first_dose,
            'regional_variation': round(regional_df['first_dose_percentage'].max() - regional_df['first_dose_percentage'].min(), 2)
        }
        
        # Save summary to file
        output_file = os.path.join(self.processed_data_dir, 'vaccination_summary.json')
        pd.Series(summary).to_json(output_file)
        logger.info(f"Saved vaccination summary to {output_file}")
        
        return summary