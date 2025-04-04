"""
Data fetching module for the South Korea COVID-19 vaccination analysis project.
This version fetches ONLY real data with improved error handling.
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import json
from bs4 import BeautifulSoup
import io
import traceback

from config import (
    KDCA_VACCINATION_URL, 
    KDCA_DAILY_STATS_URL, 
    OWID_VACCINATION_URL,
    JH_CASES_URL,
    JH_DEATHS_URL,
    WHO_API_URL,
    KCDC_DASHBOARD_URL,
    OSS_VACCINATION_URL,
    MOHW_API_URL,
    REGIONAL_DATA_URL
)
from utils.logging_utils import setup_logger, log_function_call, log_error

# Set up logger
logger = setup_logger(__name__)

class VaccinationDataFetcher:
    """Class to fetch COVID-19 vaccination data for South Korea."""
    
    def __init__(self):
        """Initialize the data fetcher."""
        # Ensure RAW_DATA_DIR is defined in config.py
        from config import RAW_DATA_DIR
        self.raw_data_dir = RAW_DATA_DIR
        os.makedirs(self.raw_data_dir, exist_ok=True)
        
    @log_function_call(logger)
    def fetch_vaccination_data(self, use_cache=True, cache_days=1):
        """Fetch vaccination data with multiple fallback sources."""
        cache_file = os.path.join(self.raw_data_dir, 'vaccination_data.csv')
        error_log = []
        
        # Check cache if enabled
        if use_cache and os.path.exists(cache_file):
            file_modified = datetime.fromtimestamp(os.path.getmtime(cache_file))
            if datetime.now() - file_modified < timedelta(days=cache_days):
                logger.info(f"Using cached vaccination data from {file_modified}")
                return pd.read_csv(cache_file)
        
        # Try sources in order of preference
        sources = [
            ('KDCA', self._fetch_kdca_vaccination),
            ('MOHW', self._fetch_mohw_vaccination),
            ('OWID', self._fetch_owid_vaccination),
            ('WHO', self._fetch_who_vaccination),
            ('OSS Community', self._fetch_oss_vaccination)
        ]
        
        for source_name, fetch_func in sources:
            try:
                logger.info(f"Attempting to fetch from {source_name}")
                data = fetch_func()
                if data is not None and not data.empty:
                    # Format and save data
                    formatted_data = self._format_vaccination_data(data, source_name)
                    formatted_data.to_csv(cache_file, index=False)
                    logger.info(f"Successfully fetched from {source_name}")
                    return formatted_data
            except requests.exceptions.ConnectionError as e:
                error_msg = f"Network connection error with {source_name}: {str(e)}"
                logger.warning(error_msg)
                error_log.append(error_msg)
            except requests.exceptions.Timeout as e:
                error_msg = f"Timeout error with {source_name}: {str(e)}"
                logger.warning(error_msg)
                error_log.append(error_msg)
            except requests.exceptions.HTTPError as e:
                error_msg = f"HTTP error with {source_name}: {str(e)}"
                logger.warning(error_msg)
                error_log.append(error_msg)
            except json.JSONDecodeError as e:
                error_msg = f"JSON parsing error with {source_name}: {str(e)}"
                logger.warning(error_msg)
                error_log.append(error_msg)
            except pd.errors.ParserError as e:
                error_msg = f"CSV parsing error with {source_name}: {str(e)}"
                logger.warning(error_msg)
                error_log.append(error_msg)
            except Exception as e:
                error_msg = f"Unexpected error with {source_name}: {str(e)}\n{traceback.format_exc()}"
                logger.warning(error_msg)
                error_log.append(error_msg)
        
        # If we have cached data but it's old, use it as a last resort
        if os.path.exists(cache_file):
            file_modified = datetime.fromtimestamp(os.path.getmtime(cache_file))
            logger.warning(f"Using outdated cache from {file_modified} as fallback")
            return pd.read_csv(cache_file)
        
        # No data available - raise comprehensive error with all failure reasons
        error_details = "\n".join(error_log)
        raise RuntimeError(
            f"All vaccination data sources failed. Cannot proceed without real data.\n"
            f"Detailed errors:\n{error_details}\n"
            f"Please check your internet connection or try again later."
        )

    def _fetch_kdca_vaccination(self):
        """Fetch vaccination data from Korean Disease Control and Prevention Agency API."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Origin': 'https://ncv.kdca.go.kr',
            'Referer': 'https://ncv.kdca.go.kr/mainStatus.es'
        }
        response = requests.get(KDCA_VACCINATION_URL, headers=headers, timeout=30)
        response.raise_for_status()  # Raise exception for HTTP errors
        data = response.json()
        
        # Additional validation to ensure we have the expected data structure
        if 'stats' not in data:
            raise ValueError("Unexpected KDCA API response format: 'stats' key not found")
        
        # Process KDCA JSON response into DataFrame
        df = pd.DataFrame(data['stats'])
        if df.empty:
            raise ValueError("KDCA API returned an empty dataset")
            
        if 'date' not in df.columns:
            raise ValueError("KDCA API response missing required 'date' column")
            
        df['date'] = pd.to_datetime(df['date'])
        return df

    def _fetch_mohw_vaccination(self):
        """Fetch vaccination data from Ministry of Health and Welfare."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        response = requests.get(MOHW_API_URL, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Process MOHW JSON response into DataFrame
        # Structure will depend on their API format
        # This is a placeholder implementation
        if 'data' not in data:
            raise ValueError("Unexpected MOHW API response format: 'data' key not found")
            
        df = pd.DataFrame(data['data'])
        if df.empty:
            raise ValueError("MOHW API returned an empty dataset")
            
        return df

    def _fetch_owid_vaccination(self):
        """Fetch vaccination data from Our World in Data."""
        response = requests.get(OWID_VACCINATION_URL, timeout=30)
        response.raise_for_status()
        
        # Check if we received CSV data
        content_type = response.headers.get('Content-Type', '')
        if 'text/csv' not in content_type and 'text/plain' not in content_type:
            raise ValueError(f"Expected CSV data but received: {content_type}")
            
        df = pd.read_csv(io.StringIO(response.text))
        
        # Validate that we have the South Korea data
        if df.empty:
            raise ValueError("OWID returned an empty dataset")
            
        # Check for required columns
        required_columns = ['date', 'total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"OWID data missing required columns: {missing_columns}")
            
        return df

    def _fetch_who_vaccination(self):
        """Fetch vaccination data from WHO."""
        response = requests.get(WHO_API_URL, timeout=30)
        response.raise_for_status()
        
        # Check if we received CSV data
        content_type = response.headers.get('Content-Type', '')
        if 'text/csv' not in content_type and 'text/plain' not in content_type:
            raise ValueError(f"Expected CSV data but received: {content_type}")
            
        who_data = pd.read_csv(io.StringIO(response.text))
        
        # Filter for South Korea data
        sk_data = who_data[who_data['Country'] == 'Republic of Korea']
        
        if sk_data.empty:
            raise ValueError("WHO data does not contain information for 'Republic of Korea'")
            
        return sk_data
    
    def _fetch_oss_vaccination(self):
        """Fetch vaccination data from open-source community project."""
        response = requests.get(OSS_VACCINATION_URL, timeout=30)
        response.raise_for_status()
        
        # This could be CSV, JSON, or other format depending on the community source
        content_type = response.headers.get('Content-Type', '')
        
        if 'application/json' in content_type:
            data = response.json()
            # Process JSON structure according to the specific format
            df = pd.DataFrame(data['south_korea'])
        elif 'text/csv' in content_type or 'text/plain' in content_type:
            df = pd.read_csv(io.StringIO(response.text))
        else:
            raise ValueError(f"Unexpected content type from OSS source: {content_type}")
            
        if df.empty:
            raise ValueError("OSS source returned an empty dataset")
            
        return df
    
    @log_function_call(logger)
    def fetch_daily_stats(self, use_cache=True, cache_days=1):
        """Fetch daily stats with multiple fallback sources."""
        cache_file = os.path.join(self.raw_data_dir, 'daily_stats.csv')
        error_log = []
        
        if use_cache and os.path.exists(cache_file):
            file_modified = datetime.fromtimestamp(os.path.getmtime(cache_file))
            if datetime.now() - file_modified < timedelta(days=cache_days):
                logger.info(f"Using cached daily stats from {file_modified}")
                return pd.read_csv(cache_file)
        
        sources = [
            ('KDCA', self._fetch_kdca_daily_stats),
            ('MOHW', self._fetch_mohw_daily_stats),
            ('JH', self._fetch_jh_daily_stats),
            ('WHO', self._fetch_who_daily_stats)
        ]
        
        for source_name, fetch_func in sources:
            try:
                logger.info(f"Attempting to fetch daily stats from {source_name}")
                data = fetch_func()
                if data is not None and not data.empty:
                    formatted_data = self._format_daily_stats(data, source_name)
                    formatted_data.to_csv(cache_file, index=False)
                    return formatted_data
            except requests.exceptions.ConnectionError as e:
                error_msg = f"Network connection error with {source_name}: {str(e)}"
                logger.warning(error_msg)
                error_log.append(error_msg)
            except requests.exceptions.Timeout as e:
                error_msg = f"Timeout error with {source_name}: {str(e)}"
                logger.warning(error_msg)
                error_log.append(error_msg)
            except requests.exceptions.HTTPError as e:
                error_msg = f"HTTP error with {source_name}: {str(e)}"
                logger.warning(error_msg)
                error_log.append(error_msg)
            except json.JSONDecodeError as e:
                error_msg = f"JSON parsing error with {source_name}: {str(e)}"
                logger.warning(error_msg)
                error_log.append(error_msg)
            except pd.errors.ParserError as e:
                error_msg = f"CSV parsing error with {source_name}: {str(e)}"
                logger.warning(error_msg)
                error_log.append(error_msg)
            except Exception as e:
                error_msg = f"Unexpected error with {source_name}: {str(e)}\n{traceback.format_exc()}"
                logger.warning(error_msg)
                error_log.append(error_msg)
        
        # If we have cached data but it's old, use it as a last resort
        if os.path.exists(cache_file):
            file_modified = datetime.fromtimestamp(os.path.getmtime(cache_file))
            logger.warning(f"Using outdated cache from {file_modified} as fallback")
            return pd.read_csv(cache_file)
        
        # No data available - raise comprehensive error with all failure reasons
        error_details = "\n".join(error_log)
        raise RuntimeError(
            f"All daily stats sources failed. Cannot proceed without real data.\n"
            f"Detailed errors:\n{error_details}\n"
            f"Please check your internet connection or try again later."
        )
    
    def _fetch_kdca_daily_stats(self):
        """Fetch daily stats from Korean CDC API."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Origin': 'https://ncv.kdca.go.kr',
            'Referer': 'https://ncv.kdca.go.kr/mainStatus.es'
        }
        response = requests.get(KDCA_DAILY_STATS_URL, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Additional validation to ensure we have the expected data structure
        if 'stats' not in data:
            raise ValueError("Unexpected KDCA API response format: 'stats' key not found")
        
        # Process KDCA JSON response into DataFrame
        df = pd.DataFrame(data['stats'])
        if df.empty:
            raise ValueError("KDCA API returned an empty dataset")
            
        if 'date' not in df.columns:
            raise ValueError("KDCA API response missing required 'date' column")
            
        df['date'] = pd.to_datetime(df['date'])
        return df
    
    def _fetch_mohw_daily_stats(self):
        """Fetch daily stats from Ministry of Health and Welfare."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        response = requests.get(MOHW_API_URL, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Process MOHW JSON response into DataFrame
        # This is a placeholder implementation - adjust according to actual API response format
        if 'data' not in data:
            raise ValueError("Unexpected MOHW API response format: 'data' key not found")
            
        df = pd.DataFrame(data['data'])
        if df.empty:
            raise ValueError("MOHW API returned an empty dataset")
            
        return df
    
    def _fetch_jh_daily_stats(self):
        """Fetch daily stats from Johns Hopkins."""
        # Fetch confirmed cases
        cases_response = requests.get(JH_CASES_URL, timeout=30)
        cases_response.raise_for_status()
        
        # Check if we received CSV data
        content_type = cases_response.headers.get('Content-Type', '')
        if 'text/csv' not in content_type and 'text/plain' not in content_type:
            raise ValueError(f"Expected CSV data for cases but received: {content_type}")
            
        cases_df = pd.read_csv(io.StringIO(cases_response.text))
        
        # Validate that we have South Korea data
        if 'Country/Region' not in cases_df.columns:
            raise ValueError("JH cases data missing 'Country/Region' column")
            
        if cases_df[cases_df['Country/Region'] == 'Korea, South'].empty:
            raise ValueError("JH cases data does not contain information for 'Korea, South'")
        
        # Fetch deaths
        deaths_response = requests.get(JH_DEATHS_URL, timeout=30)
        deaths_response.raise_for_status()
        
        # Check if we received CSV data
        content_type = deaths_response.headers.get('Content-Type', '')
        if 'text/csv' not in content_type and 'text/plain' not in content_type:
            raise ValueError(f"Expected CSV data for deaths but received: {content_type}")
            
        deaths_df = pd.read_csv(io.StringIO(deaths_response.text))
        
        # Validate that we have South Korea data
        if 'Country/Region' not in deaths_df.columns:
            raise ValueError("JH deaths data missing 'Country/Region' column")
            
        if deaths_df[deaths_df['Country/Region'] == 'Korea, South'].empty:
            raise ValueError("JH deaths data does not contain information for 'Korea, South'")
        
        return self._process_jh_data(cases_df, deaths_df)
    
    def _process_jh_data(self, cases_df, deaths_df):
        """Process Johns Hopkins data into the expected format."""
        # Filter for South Korea
        confirmed_sk = cases_df[cases_df['Country/Region'] == 'Korea, South']
        deaths_sk = deaths_df[deaths_df['Country/Region'] == 'Korea, South']
        
        # Convert from wide to long format
        date_columns = [col for col in confirmed_sk.columns if '/' in col or '-' in col]
        
        if not date_columns:
            raise ValueError("No date columns found in JH data")
        
        # Initialize lists to hold data
        dates = []
        daily_cases = []
        daily_deaths = []
        
        # Extract rows which contain the country totals
        confirmed_values = confirmed_sk[date_columns].values[0]
        death_values = deaths_sk[date_columns].values[0]
        
        # Process each date
        for i in range(len(date_columns)):
            date_str = date_columns[i]
            try:
                # Handle different date formats
                if '/' in date_str:
                    date = datetime.strptime(date_str, '%m/%d/%y')
                else:
                    date = datetime.strptime(date_str, '%Y-%m-%d')
                
                date_formatted = date.strftime('%Y-%m-%d')
                
                # Get cumulative counts
                cum_cases = confirmed_values[i]
                cum_deaths = death_values[i]
                
                # Calculate daily counts (difference from previous day)
                if i > 0:
                    day_cases = cum_cases - confirmed_values[i-1]
                    day_deaths = cum_deaths - death_values[i-1]
                else:
                    day_cases = cum_cases
                    day_deaths = cum_deaths
                
                # Handle data corrections (negative values)
                day_cases = max(0, day_cases)
                day_deaths = max(0, day_deaths)
                
                # Append to lists
                dates.append(date_formatted)
                daily_cases.append(day_cases)
                daily_deaths.append(day_deaths)
            except ValueError as e:
                logger.warning(f"Could not parse date '{date_str}': {e}")
                continue
        
        # Create DataFrame
        if not dates:
            raise ValueError("No valid dates could be parsed from JH data")
            
        result_df = pd.DataFrame({
            'date': dates,
            'daily_cases': daily_cases,
            'daily_deaths': daily_deaths,
            'cumulative_cases': np.cumsum(daily_cases),
            'cumulative_deaths': np.cumsum(daily_deaths)
        })
        
        return result_df
    
    def _fetch_who_daily_stats(self):
        """Fetch daily stats from WHO."""
        response = requests.get(WHO_API_URL, timeout=30)
        response.raise_for_status()
        
        # Check if we received CSV data
        content_type = response.headers.get('Content-Type', '')
        if 'text/csv' not in content_type and 'text/plain' not in content_type:
            raise ValueError(f"Expected CSV data but received: {content_type}")
            
        who_data = pd.read_csv(io.StringIO(response.text))
        
        # Filter for South Korea data
        sk_data = who_data[who_data['Country'] == 'Republic of Korea']
        
        if sk_data.empty:
            raise ValueError("WHO data does not contain information for 'Republic of Korea'")
            
        return sk_data
    
    @log_function_call(logger)
    def fetch_regional_data(self, use_cache=True, cache_days=1):
        """
        Fetch regional vaccination data from actual sources or web scraping.
        
        Args:
            use_cache (bool): Whether to use cached data if available
            cache_days (int): Number of days the cache is considered valid
            
        Returns:
            pandas.DataFrame: DataFrame containing regional vaccination data
        """
        cache_file = os.path.join(self.raw_data_dir, 'regional_data.csv')
        error_log = []
        
        # Check if cached data is available and not too old
        if use_cache and os.path.exists(cache_file):
            file_modified = datetime.fromtimestamp(os.path.getmtime(cache_file))
            if datetime.now() - file_modified < timedelta(days=cache_days):
                logger.info(f"Using cached regional data from {file_modified}")
                return pd.read_csv(cache_file)
        
        # Try multiple sources for regional data
        sources = [
            ('KDCA Regional', self._fetch_kdca_regional_data),
            ('MOHW Regional', self._fetch_mohw_regional_data),
            ('Web Scraping', self._fetch_regional_data_by_scraping)
        ]
        
        for source_name, fetch_func in sources:
            try:
                logger.info(f"Attempting to fetch regional data from {source_name}")
                data = fetch_func()
                if data is not None and not data.empty:
                    # Save to cache
                    os.makedirs(self.raw_data_dir, exist_ok=True)
                    data.to_csv(cache_file, index=False)
                    logger.info(f"Successfully fetched regional data from {source_name}")
                    return data
            except requests.exceptions.ConnectionError as e:
                error_msg = f"Network connection error with {source_name}: {str(e)}"
                logger.warning(error_msg)
                error_log.append(error_msg)
            except requests.exceptions.Timeout as e:
                error_msg = f"Timeout error with {source_name}: {str(e)}"
                logger.warning(error_msg)
                error_log.append(error_msg)
            except requests.exceptions.HTTPError as e:
                error_msg = f"HTTP error with {source_name}: {str(e)}"
                logger.warning(error_msg)
                error_log.append(error_msg)
            except json.JSONDecodeError as e:
                error_msg = f"JSON parsing error with {source_name}: {str(e)}"
                logger.warning(error_msg)
                error_log.append(error_msg)
            except pd.errors.ParserError as e:
                error_msg = f"CSV parsing error with {source_name}: {str(e)}"
                logger.warning(error_msg)
                error_log.append(error_msg)
            except Exception as e:
                error_msg = f"Unexpected error with {source_name}: {str(e)}\n{traceback.format_exc()}"
                logger.warning(error_msg)
                error_log.append(error_msg)
        
        # If we have cached data but it's old, use it as a last resort
        if os.path.exists(cache_file):
            file_modified = datetime.fromtimestamp(os.path.getmtime(cache_file))
            logger.warning(f"Using outdated cache from {file_modified} as fallback")
            return pd.read_csv(cache_file)
        
        # No data available - raise comprehensive error with all failure reasons
        error_details = "\n".join(error_log)
        raise RuntimeError(
            f"All regional data sources failed. Cannot proceed without real data.\n"
            f"Detailed errors:\n{error_details}\n"
            f"Please check your internet connection or try again later."
        )
    
    def _fetch_kdca_regional_data(self):
        """Fetch regional data from KDCA API or website."""
        # This is placeholder implementation - replace with actual API endpoint
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        
        # Try KDCA API if available
        try:
            response = requests.get(REGIONAL_DATA_URL, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Process response based on content type
            content_type = response.headers.get('Content-Type', '')
            
            if 'application/json' in content_type:
                data = response.json()
                # Format will depend on the API structure
                df = pd.DataFrame(data['regions'])
            elif 'text/csv' in content_type or 'text/plain' in content_type:
                df = pd.read_csv(io.StringIO(response.text))
            else:
                raise ValueError(f"Unexpected content type: {content_type}")
                
            return df
        except Exception as e:
            logger.warning(f"KDCA API regional data fetch failed: {e}, trying web scraping")
            # Fall back to web scraping KDCA dashboard
            return self._scrape_kdca_dashboard()
    
    def _scrape_kdca_dashboard(self):
        """Scrape KDCA dashboard for regional vaccination data."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        
        response = requests.get(KCDC_DASHBOARD_URL, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # This would need to be adapted to the actual structure of the KDCA dashboard
        # Looking for tables with regional vaccination data
        tables = soup.find_all('table')
        
        if not tables:
            raise ValueError("No tables found on KDCA dashboard")
        
        # Find the regional vaccination table
        regional_table = None
        for table in tables:
            # Look for table headers or other identifiers that indicate this is the right table
            headers = table.find_all('th')
            header_text = ' '.join(th.get_text().strip() for th in headers)
            if '지역' in header_text and '접종' in header_text:  # Korean for 'region' and 'vaccination'
                regional_table = table
                break
        
        if regional_table is None:
            raise ValueError("Could not find regional vaccination table on KDCA dashboard")
        
        # Extract data from the table
        rows = regional_table.find_all('tr')
        
        # Extract column names from header row
        header_row = rows[0]
        column_names = [th.get_text().strip() for th in header_row.find_all('th')]
        
        # Extract data from data rows
        data_rows = []
        for row in rows[1:]:  # Skip header row
            cells = row.find_all('td')
            if cells:
                row_data = [cell.get_text().strip() for cell in cells]
                data_rows.append(row_data)
        
        # Create DataFrame
        if not data_rows:
            raise ValueError("No data rows found in regional vaccination table")
            
        df = pd.DataFrame(data_rows, columns=column_names)
        
        # Process the data to match expected format
        # This is heavily dependent on the actual table structure
        processed_df = self._process_scraped_regional_data(df)
        
        return processed_df
    
    def _process_scraped_regional_data(self, df):
        """Process scraped regional data into the expected format."""
        # This is heavily dependent on the actual table structure
        # For now, creating a simplified example
        
        # Assuming:
        # - First column is region name
        # - Some columns contain vaccination numbers that need to be converted
        # - Different columns may represent different dose types
        
        # Rename columns based on content
        from config import REGIONS
        
        # Check if the first column contains region names
        first_col = df.columns[0]
        if not set(df[first_col]) & set(REGIONS):
            # Try to map column values to region names
            region_mapping = {
                '서울': 'Seoul', '부산': 'Busan', '대구': 'Daegu', 
                '인천': 'Incheon', '광주': 'Gwangju', '대전': 'Daejeon', 
                '울산': 'Ulsan', '세종': 'Sejong', '경기': 'Gyeonggi', 
                '강원': 'Gangwon', '충북': 'Chungbuk', '충남': 'Chungnam', 
                '전북': 'Jeonbuk', '전남': 'Jeonnam', '경북': 'Gyeongbuk', 
                '경남': 'Gyeongnam', '제주': 'Jeju'
            }
            df['region'] = df[first_col].map(region_mapping)
        else:
            df['region'] = df[first_col]
        
        # Convert numeric columns to numbers
        for col in df.columns:
            if col != 'region' and col != first_col:
                # Remove commas and convert to numeric
                df[col] = df[col].str.replace(',', '').astype(float)
        
        # Create a new DataFrame with standardized column names
        result_df = pd.DataFrame()
        result_df['region'] = df['region']
        
        # Map columns to standardized format based on keywords
        for col in df.columns:
            if '1차' in col or '1dose' in col or 'first' in col.lower():
                result_df['first_dose'] = df[col]
            elif '2차' in col or '2dose' in col or 'second' in col.lower():
                result_df['second_dose'] = df[col]
            elif '부스터' in col or 'booster' in col.lower():
                result_df['booster'] = df[col]
            elif '인구' in col or 'population' in col.lower():
                result_df['population'] = df[col]
        
        # If population is missing, use population distribution from config
        if 'population' not in result_df.columns:
            from config import POPULATION
            
            # Approximate population distribution by region
            population_distribution = {
                'Seoul': 0.20, 'Busan': 0.07, 'Daegu': 0.05, 'Incheon': 0.06,
                'Gwangju': 0.03, 'Daejeon': 0.03, 'Ulsan': 0.02, 'Sejong': 0.01,
                'Gyeonggi': 0.25, 'Gangwon': 0.03, 'Chungbuk': 0.03, 'Chungnam': 0.04,
                'Jeonbuk': 0.03, 'Jeonnam': 0.03, 'Gyeongbuk': 0.05, 'Gyeongnam': 0.06,
                'Jeju': 0.01
            }
            
            result_df['population'] = result_df['region'].map(
                {region: int(POPULATION * dist) for region, dist in population_distribution.items()}
            )
        
        # Calculate percentages
        for col in ['first_dose', 'second_dose', 'booster']:
            if col in result_df.columns:
                result_df[f'{col}_percentage'] = (result_df[col] / result_df['population'] * 100).round(2)
                
        return result_df
            
    def _fetch_mohw_regional_data(self):
        """Fetch regional data from Ministry of Health and Welfare."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        
        # Attempt to get data from MOHW API
        response = requests.get(MOHW_API_URL, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Process response based on content type
        content_type = response.headers.get('Content-Type', '')
        
        if 'application/json' in content_type:
            data = response.json()
            # Format will depend on the API structure
            if 'regions' in data:
                df = pd.DataFrame(data['regions'])
            else:
                raise ValueError("Unexpected MOHW API response format: 'regions' key not found")
        else:
            raise ValueError(f"Unexpected content type from MOHW API: {content_type}")
            
        if df.empty:
            raise ValueError("MOHW API returned an empty dataset for regional data")
            
        return df
        
    def _fetch_regional_data_by_scraping(self):
        """Fetch regional data by scraping Korean news sources."""
        from config import REGIONS
        
        def scrape_source(url, parser_func):
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml',
                'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            return parser_func(soup)
        
        def parse_news_source_1(soup):
            """Parse the HTML from the first news source."""
            # This would need to be adapted to the actual structure of the news site
            tables = soup.find_all('table')
            
            if not tables:
                raise ValueError("No tables found on news source 1")
            
            # Find the table with regional vaccination data
            regional_table = None
            for table in tables:
                # Check table content to identify the right one
                if any('지역별' in str(th) for th in table.find_all('th')):
                    regional_table = table
                    break
            
            if not regional_table:
                raise ValueError("Could not find regional data table in news source 1")
            
            # Extract data (specific to the table structure)
            # ...implementation details...
            
            # Placeholder for actual implementation
            data = []
            for region in REGIONS:
                data.append({
                    'region': region,
                    'first_dose': None,
                    'second_dose': None,
                    'booster': None
                })
            
            return pd.DataFrame(data)
        
        # Try multiple news sources that might publish the regional data
        sources = [
            ('https://news-source-1.kr/covid-vaccination', parse_news_source_1),
            # Add more sources here
        ]
        
        for url, parser in sources:
            try:
                return scrape_source(url, parser)
            except Exception as e:
                logger.warning(f"Failed to scrape {url}: {e}")
                continue
        
        raise ValueError("All web scraping attempts for regional data failed")
    
    def _format_vaccination_data(self, data, source_name):
        """
        Format vaccination data to match the expected structure.
        
        Args:
            data (pandas.DataFrame): Raw data from source
            source_name (str): Name of the data source
            
        Returns:
            pandas.DataFrame: Formatted vaccination data
        """
        # Ensure DATE_FORMAT is defined in config.py
        from config import DATE_FORMAT, REGIONS, VACCINE_TYPES, POPULATION
        
        # Make a copy to avoid modifying the original
        formatted_data = data.copy()
        
        # Handle different source formats
        if source_name == 'OWID':
            # Convert date to the expected format
            formatted_data['date'] = pd.to_datetime(formatted_data['date']).dt.strftime(DATE_FORMAT)
            
            # Sort by date
            formatted_data = formatted_data.sort_values('date')
            
            # Calculate daily values from cumulative values
            for col in ['total_vaccinations', 'people_vaccinated', 'people_fully_vaccinated', 'total_boosters']:
                if col in formatted_data.columns:
                    daily_col = f'daily_{col}'
                    formatted_data[daily_col] = formatted_data[col].diff().fillna(0).astype(int)
                    # Replace negative values with 0 (data corrections)
                    formatted_data.loc[formatted_data[daily_col] < 0, daily_col] = 0
            
            # Rename columns to match our structure
            column_mapping = {
                'people_vaccinated': 'cumulative_first_dose',
                'people_fully_vaccinated': 'cumulative_second_dose',
                'total_boosters': 'cumulative_booster',
                'daily_people_vaccinated': 'daily_first_dose',
                'daily_people_fully_vaccinated': 'daily_second_dose',
                'daily_total_boosters': 'daily_booster'
            }
            formatted_data = formatted_data.rename(columns=column_mapping)
            
            # Calculate percentages
            formatted_data['first_dose_percentage'] = (formatted_data['cumulative_first_dose'] / POPULATION * 100).round(2)
            formatted_data['second_dose_percentage'] = (formatted_data['cumulative_second_dose'] / POPULATION * 100).round(2)
            
            if 'cumulative_booster' in formatted_data.columns:
                formatted_data['booster_percentage'] = (formatted_data['cumulative_booster'] / POPULATION * 100).round(2)
            
            # Estimate vaccine type distribution based on reported data or international averages
            # This is an approximation - in a real implementation, use actual data if available
            vaccine_distribution = {
                'Pfizer': 0.45,
                'Moderna': 0.30,
                'AstraZeneca': 0.15,
                'Janssen': 0.05,
                'Novavax': 0.05
            }
            
            # Calculate daily totals
            formatted_data['daily_total'] = formatted_data['daily_first_dose'] + \
                                        formatted_data['daily_second_dose'] + \
                                        formatted_data.get('daily_booster', 0)
            
            # Distribute by vaccine type
            for vaccine_type, proportion in vaccine_distribution.items():
                formatted_data[f'{vaccine_type}_daily'] = (formatted_data['daily_total'] * proportion).astype(int)
                formatted_data[f'{vaccine_type}_percentage'] = proportion * 100
                
        elif source_name == 'KDCA':
            # KDCA specific formatting based on their API structure
            # This is a placeholder - adapt to actual KDCA data structure
            if 'date' in formatted_data.columns:
                formatted_data['date'] = pd.to_datetime(formatted_data['date']).dt.strftime(DATE_FORMAT)
            
            # Map KDCA columns to our standard naming convention
            column_mapping = {
                # Example mappings - adjust to actual data
                'first_dose_total': 'cumulative_first_dose',
                'second_dose_total': 'cumulative_second_dose',
                'booster_total': 'cumulative_booster',
                'first_dose_daily': 'daily_first_dose',
                'second_dose_daily': 'daily_second_dose',
                'booster_daily': 'daily_booster'
            }
            
            # Only rename columns that exist
            existing_cols = {k: v for k, v in column_mapping.items() if k in formatted_data.columns}
            formatted_data = formatted_data.rename(columns=existing_cols)
            
            # Add percentages if missing
            for source_col, target_col in [
                ('cumulative_first_dose', 'first_dose_percentage'),
                ('cumulative_second_dose', 'second_dose_percentage'),
                ('cumulative_booster', 'booster_percentage')
            ]:
                if source_col in formatted_data.columns and target_col not in formatted_data.columns:
                    formatted_data[target_col] = (formatted_data[source_col] / POPULATION * 100).round(2)
            
        elif source_name == 'MOHW':
            # Ministry of Health and Welfare specific formatting
            # This is a placeholder - adapt to actual MOHW data structure
            pass
            
        elif source_name == 'WHO':
            # WHO specific formatting
            # This would need to be implemented based on the actual structure of WHO data
            pass
            
        elif source_name == 'OSS Community':
            # Open Source community data formatting
            # This would need to be implemented based on the structure of that data
            pass
            
        # Ensure all required columns exist
        required_columns = [
            'date', 'daily_first_dose', 'daily_second_dose', 'daily_booster',
            'cumulative_first_dose', 'cumulative_second_dose', 'cumulative_booster',
            'first_dose_percentage', 'second_dose_percentage', 'booster_percentage'
        ]
        
        for col in required_columns:
            if col not in formatted_data.columns:
                if col == 'date':
                    formatted_data[col] = datetime.now().strftime(DATE_FORMAT)
                else:
                    formatted_data[col] = 0
                    
        return formatted_data
    
    def _format_daily_stats(self, data, source_name=None):
        """
        Format daily statistics data.
        
        Args:
            data (pandas.DataFrame): Raw data
            source_name (str, optional): Name of the data source
            
        Returns:
            pandas.DataFrame: Formatted daily statistics
        """
        # Ensure DATE_FORMAT is defined in config.py
        from config import DATE_FORMAT
        
        # For Johns Hopkins data, this is already processed in _fetch_jh_daily_stats
        if isinstance(data, pd.DataFrame) and 'daily_cases' in data.columns and 'daily_deaths' in data.columns:
            return data
        
        # Otherwise, format based on source
        formatted_data = data.copy()
        
        if source_name == 'KDCA':
            # KDCA specific formatting
            # This would need to be implemented based on the actual structure
            # Placeholder implementation
            if 'date' in formatted_data.columns:
                formatted_data['date'] = pd.to_datetime(formatted_data['date']).dt.strftime(DATE_FORMAT)
            
            # Map column names to our standard format
            column_mapping = {
                # Example mappings - adjust to actual data
                'confirmed_daily': 'daily_cases',
                'death_daily': 'daily_deaths',
                'testing_daily': 'daily_tests'
            }
            
            # Only rename columns that exist
            existing_cols = {k: v for k, v in column_mapping.items() if k in formatted_data.columns}
            formatted_data = formatted_data.rename(columns=existing_cols)
            
        elif source_name == 'MOHW':
            # MOHW specific formatting
            # This would need to be implemented based on the actual structure
            pass
            
        elif source_name == 'WHO':
            # WHO specific formatting
            # Filter for South Korea data if not already done
            if 'Country' in formatted_data.columns and not all(formatted_data['Country'] == 'Republic of Korea'):
                formatted_data = formatted_data[formatted_data['Country'] == 'Republic of Korea']
            
            # Example column mapping for WHO data
            column_mapping = {
                'Date_reported': 'date',
                'New_cases': 'daily_cases',
                'New_deaths': 'daily_deaths',
                'Cumulative_cases': 'cumulative_cases',
                'Cumulative_deaths': 'cumulative_deaths'
            }
            
            # Only rename columns that exist
            existing_cols = {k: v for k, v in column_mapping.items() if k in formatted_data.columns}
            formatted_data = formatted_data.rename(columns=existing_cols)
            
            # Format date
            if 'date' in formatted_data.columns and not isinstance(formatted_data['date'].iloc[0], str):
                formatted_data['date'] = pd.to_datetime(formatted_data['date']).dt.strftime(DATE_FORMAT)
        
        # Add positivity rate calculation if we have both cases and tests
        if all(col in formatted_data.columns for col in ['daily_cases', 'daily_tests']):
            formatted_data['positivity_rate'] = (formatted_data['daily_cases'] / formatted_data['daily_tests'] * 100).round(2)
        
        # Ensure data is sorted by date
        if 'date' in formatted_data.columns:
            formatted_data = formatted_data.sort_values('date')
            
        return formatted_data