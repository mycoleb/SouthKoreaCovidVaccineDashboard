"""
Data fetching module for the South Korea COVID-19 vaccination analysis project.
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import json
from bs4 import BeautifulSoup

from config import (
    RAW_DATA_DIR, 
    VACCINATION_DATA_URL, 
    DAILY_STATS_URL, 
    DATE_FORMAT
)
from utils.logging_utils import setup_logger, log_function_call, log_error

# Set up logger
logger = setup_logger(__name__)

class VaccinationDataFetcher:
    """Class to fetch COVID-19 vaccination data for South Korea."""
    
    def __init__(self):
        """Initialize the data fetcher."""
        self.raw_data_dir = RAW_DATA_DIR
        
    @log_function_call(logger)
    def fetch_vaccination_data(self, use_cache=True, cache_days=1):
        """
        Fetch vaccination data from the KDCA website or use cached data if available.
        
        Args:
            use_cache (bool): Whether to use cached data if available
            cache_days (int): Number of days the cache is considered valid
            
        Returns:
            pandas.DataFrame: DataFrame containing vaccination data
        """
        cache_file = os.path.join(self.raw_data_dir, 'vaccination_data.csv')
        
        # Check if cached data is available and not too old
        if use_cache and os.path.exists(cache_file):
            file_modified = datetime.fromtimestamp(os.path.getmtime(cache_file))
            if datetime.now() - file_modified < timedelta(days=cache_days):
                logger.info(f"Using cached vaccination data from {file_modified}")
                return pd.read_csv(cache_file)
        
        logger.info("Fetching vaccination data from KDCA website")
        try:
            # In a real implementation, we would use web scraping or API calls
            # For this example, we'll simulate the data
            
            # Simulate scraping delay
            time.sleep(1)
            
            # Create simulated vaccination data
            simulated_data = self._generate_simulated_vaccination_data()
            
            # Save to cache
            os.makedirs(self.raw_data_dir, exist_ok=True)
            simulated_data.to_csv(cache_file, index=False)
            logger.info(f"Saved vaccination data to {cache_file}")
            
            return simulated_data
            
        except Exception as e:
            logger.error(f"Error fetching vaccination data: {e}", exc_info=True)
            
            # If cache exists but is old, use it as fallback
            if os.path.exists(cache_file):
                logger.warning("Using outdated cache as fallback")
                return pd.read_csv(cache_file)
            
            raise
    
    @log_function_call(logger)
    def fetch_daily_stats(self, use_cache=True, cache_days=1):
        """
        Fetch daily COVID-19 statistics from the KDCA website.
        
        Args:
            use_cache (bool): Whether to use cached data if available
            cache_days (int): Number of days the cache is considered valid
            
        Returns:
            pandas.DataFrame: DataFrame containing daily statistics
        """
        cache_file = os.path.join(self.raw_data_dir, 'daily_stats.csv')
        
        # Check if cached data is available and not too old
        if use_cache and os.path.exists(cache_file):
            file_modified = datetime.fromtimestamp(os.path.getmtime(cache_file))
            if datetime.now() - file_modified < timedelta(days=cache_days):
                logger.info(f"Using cached daily stats from {file_modified}")
                return pd.read_csv(cache_file)
        
        logger.info("Fetching daily stats from KDCA website")
        try:
            # In a real implementation, we would use web scraping or API calls
            # For this example, we'll simulate the data
            
            # Simulate scraping delay
            time.sleep(1)
            
            # Create simulated daily stats
            simulated_data = self._generate_simulated_daily_stats()
            
            # Save to cache
            os.makedirs(self.raw_data_dir, exist_ok=True)
            simulated_data.to_csv(cache_file, index=False)
            logger.info(f"Saved daily stats to {cache_file}")
            
            return simulated_data
            
        except Exception as e:
            logger.error(f"Error fetching daily stats: {e}", exc_info=True)
            
            # If cache exists but is old, use it as fallback
            if os.path.exists(cache_file):
                logger.warning("Using outdated cache as fallback")
                return pd.read_csv(cache_file)
            
            raise
    
    @log_function_call(logger)
    def fetch_regional_data(self, use_cache=True, cache_days=1):
        """
        Fetch regional vaccination data.
        
        Args:
            use_cache (bool): Whether to use cached data if available
            cache_days (int): Number of days the cache is considered valid
            
        Returns:
            pandas.DataFrame: DataFrame containing regional vaccination data
        """
        cache_file = os.path.join(self.raw_data_dir, 'regional_data.csv')
        
        # Check if cached data is available and not too old
        if use_cache and os.path.exists(cache_file):
            file_modified = datetime.fromtimestamp(os.path.getmtime(cache_file))
            if datetime.now() - file_modified < timedelta(days=cache_days):
                logger.info(f"Using cached regional data from {file_modified}")
                return pd.read_csv(cache_file)
        
        logger.info("Fetching regional vaccination data")
        try:
            # In a real implementation, we would use web scraping or API calls
            # For this example, we'll simulate the data
            
            # Simulate scraping delay
            time.sleep(1)
            
            # Create simulated regional data
            simulated_data = self._generate_simulated_regional_data()
            
            # Save to cache
            os.makedirs(self.raw_data_dir, exist_ok=True)
            simulated_data.to_csv(cache_file, index=False)
            logger.info(f"Saved regional data to {cache_file}")
            
            return simulated_data
            
        except Exception as e:
            logger.error(f"Error fetching regional data: {e}", exc_info=True)
            
            # If cache exists but is old, use it as fallback
            if os.path.exists(cache_file):
                logger.warning("Using outdated cache as fallback")
                return pd.read_csv(cache_file)
            
            raise
    
    def _generate_simulated_vaccination_data(self):
        """
        Generate simulated vaccination data for demonstration purposes.
        
        Returns:
            pandas.DataFrame: Simulated vaccination data
        """
        from config import REGIONS, VACCINE_TYPES
        import numpy as np
        
        # Generate dates for the past year
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        data = []
        
        # Population of South Korea (approximate)
        population = 51_000_000
        
        # Create cumulative data
        cumulative_first_dose = 0
        cumulative_second_dose = 0
        cumulative_booster = 0
        
        # Growth rate parameters
        first_dose_max = 0.85 * population  # 85% of population gets first dose
        second_dose_lag = 28  # days between first and second dose
        booster_lag = 180  # days between second dose and booster
        
        # Vaccine distribution
        vaccine_distribution = {
            'Pfizer': 0.45,
            'Moderna': 0.30,
            'AstraZeneca': 0.15,
            'Janssen': 0.05,
            'Novavax': 0.05
        }
        
        for i, date in enumerate(dates):
            date_str = date.strftime(DATE_FORMAT)
            
            # First dose follows logistic growth curve
            days_passed = (date - start_date).days
            first_dose_rate = 1 / (1 + np.exp(-0.015 * (days_passed - 120)))
            daily_first_dose = int(first_dose_max * first_dose_rate) - cumulative_first_dose
            daily_first_dose = max(0, daily_first_dose)
            cumulative_first_dose += daily_first_dose
            
            # Second dose follows first dose with a lag
            if i >= second_dose_lag:
                previous_first_dose = int(first_dose_max * (1 / (1 + np.exp(-0.015 * (days_passed - second_dose_lag - 120)))))
                daily_second_dose = previous_first_dose - cumulative_second_dose
                daily_second_dose = max(0, daily_second_dose)
                cumulative_second_dose += daily_second_dose
            else:
                daily_second_dose = 0
            
            # Booster follows second dose with a lag
            if i >= second_dose_lag + booster_lag:
                previous_second_dose = int(first_dose_max * (1 / (1 + np.exp(-0.015 * (days_passed - second_dose_lag - booster_lag - 120)))))
                daily_booster = previous_second_dose - cumulative_booster
                daily_booster = max(0, min(daily_booster, previous_second_dose * 0.7))  # 70% of second dose recipients get booster
                cumulative_booster += daily_booster
            else:
                daily_booster = 0
            
            # Add daily data
            data.append({
                'date': date_str,
                'daily_first_dose': daily_first_dose,
                'cumulative_first_dose': cumulative_first_dose,
                'daily_second_dose': daily_second_dose,
                'cumulative_second_dose': cumulative_second_dose,
                'daily_booster': daily_booster,
                'cumulative_booster': cumulative_booster,
                'first_dose_percentage': round(cumulative_first_dose / population * 100, 2),
                'second_dose_percentage': round(cumulative_second_dose / population * 100, 2),
                'booster_percentage': round(cumulative_booster / population * 100, 2)
            })
            
            # Add vaccine type data
            for vaccine_type in VACCINE_TYPES:
                total_daily = daily_first_dose + daily_second_dose + daily_booster
                data[-1][f'{vaccine_type}_daily'] = int(total_daily * vaccine_distribution[vaccine_type])
                data[-1][f'{vaccine_type}_percentage'] = vaccine_distribution[vaccine_type] * 100
        
        return pd.DataFrame(data)
    
    def _generate_simulated_daily_stats(self):
        """
        Generate simulated daily COVID-19 statistics for demonstration purposes.
        
        Returns:
            pandas.DataFrame: Simulated daily statistics
        """
        import numpy as np
        
        # Generate dates for the past year
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        
        data = []
        
        # Base numbers
        base_cases = 2000
        base_deaths = 20
        base_tests = 100000
        
        # Trend parameters
        trend_period = 90  # Days for a full cycle
        trend_amplitude_cases = 1500
        trend_amplitude_deaths = 15
        vaccination_effect_start = 120  # Days into the data when vaccination starts reducing cases
        
        for i, date in enumerate(dates):
            date_str = date.strftime(DATE_FORMAT)
            
            # Create cyclic trend with declining amplitude due to vaccination
            trend_factor = np.sin(2 * np.pi * i / trend_period)
            vaccination_factor = max(0, 1 - max(0, i - vaccination_effect_start) / 300)
            
            # Calculate daily numbers with random noise
            daily_cases = int(max(100, (base_cases + trend_amplitude_cases * trend_factor) * vaccination_factor * (1 + 0.2 * np.random.randn())))
            daily_deaths = int(max(0, (base_deaths + trend_amplitude_deaths * trend_factor) * vaccination_factor * (1 + 0.3 * np.random.randn())))
            daily_tests = int(max(10000, base_tests * (1 + 0.1 * np.random.randn())))
            
            # Test positivity rate
            positivity_rate = round(daily_cases / daily_tests * 100, 2)
            
            data.append({
                'date': date_str,
                'daily_cases': daily_cases,
                'daily_deaths': daily_deaths,
                'daily_tests': daily_tests,
                'positivity_rate': positivity_rate
            })
        
        return pd.DataFrame(data)
    
    def _generate_simulated_regional_data(self):
        """
        Generate simulated regional vaccination data for demonstration purposes.
        
        Returns:
            pandas.DataFrame: Simulated regional vaccination data
        """
        from config import REGIONS
        import numpy as np
        
        # Population distribution (approximate)
        population_distribution = {
            'Seoul': 0.20,
            'Busan': 0.07,
            'Daegu': 0.05,
            'Incheon': 0.06,
            'Gwangju': 0.03,
            'Daejeon': 0.03,
            'Ulsan': 0.02,
            'Sejong': 0.01,
            'Gyeonggi': 0.25,
            'Gangwon': 0.03,
            'Chungbuk': 0.03,
            'Chungnam': 0.04,
            'Jeonbuk': 0.03,
            'Jeonnam': 0.03,
            'Gyeongbuk': 0.05,
            'Gyeongnam': 0.06,
            'Jeju': 0.01
        }
        
        # Total population
        total_population = 51_000_000
        
        # Vaccination rates with regional variations
        base_first_dose = 0.85  # 85% of population got first dose
        base_second_dose = 0.80  # 80% of population got second dose
        base_booster = 0.50  # 50% of population got booster
        
        data = []
        
        for region in REGIONS:
            # Regional population
            population = int(total_population * population_distribution[region])
            
            # Regional variation factor (some regions have higher/lower rates)
            variation_factor = 1 + 0.1 * np.random.randn()
            
            # Calculate doses with regional variation
            first_dose = int(population * base_first_dose * variation_factor)
            second_dose = int(population * base_second_dose * variation_factor)
            booster = int(population * base_booster * variation_factor)
            
            # Ensure logical progression
            first_dose = max(first_dose, second_dose)
            second_dose = max(second_dose, booster)
            
            data.append({
                'region': region,
                'population': population,
                'first_dose': first_dose,
                'second_dose': second_dose,
                'booster': booster,
                'first_dose_percentage': round(first_dose / population * 100, 2),
                'second_dose_percentage': round(second_dose / population * 100, 2),
                'booster_percentage': round(booster / population * 100, 2)
            })
        
        return pd.DataFrame(data)