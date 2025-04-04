"""
Configuration settings for the South Korea COVID-19 vaccination analysis project.
"""

import os
from pathlib import Path

# Base directory
BASE_DIR = Path(__file__).parent.absolute()

# Data directories
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')

# Create directories if they don't exist
for directory in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, OUTPUT_DIR, LOGS_DIR]:
    os.makedirs(directory, exist_ok=True)

# Data source URLs
VACCINATION_DATA_URL = "https://ncv.kdca.go.kr/eng/mainStatus.es?mid=a11702000000"
DAILY_STATS_URL = "https://ncv.kdca.go.kr/eng/bdBoardList.es?mid=a30401000000"

# Korean regions (in English)
REGIONS = [
    'Seoul', 'Busan', 'Daegu', 'Incheon', 'Gwangju', 'Daejeon', 'Ulsan', 
    'Sejong', 'Gyeonggi', 'Gangwon', 'Chungbuk', 'Chungnam', 'Jeonbuk', 
    'Jeonnam', 'Gyeongbuk', 'Gyeongnam', 'Jeju'
]

# Vaccine types
VACCINE_TYPES = ['Pfizer', 'Moderna', 'AstraZeneca', 'Janssen', 'Novavax']

# Date format
DATE_FORMAT = '%Y-%m-%d'

# Visualization settings
VISUALIZATION_DPI = 300
FIGURE_SIZE = (12, 8)
COLOR_PALETTE = {
    'Pfizer': '#2c7fb8',
    'Moderna': '#7fcdbb',
    'AstraZeneca': '#edf8b1',
    'Janssen': '#2c7fb8',
    'Novavax': '#253494',
    'First Dose': '#66c2a5',
    'Second Dose': '#fc8d62',
    'Booster': '#8da0cb',
    'Fourth Dose': '#e78ac3'
}

# Logging configuration
LOG_LEVEL = 'INFO'
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FILE = os.path.join(LOGS_DIR, 'vaccine_analysis.log')