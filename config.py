# Data source URLs (updated with multiple reliable sources)
# Primary source: KDCA (Korean Disease Control and Prevention Agency)
KDCA_VACCINATION_URL = "https://ncv.kdca.go.kr/api/stats/mainStatus"
KDCA_DAILY_STATS_URL = "https://ncv.kdca.go.kr/api/stats/dailyStats"
KCDC_DASHBOARD_URL = "https://ncv.kdca.go.kr/mainStatus.es"  # For web scraping if API fails

# Ministry of Health and Welfare
MOHW_API_URL = "https://www.mohw.go.kr/react/api/covidVaccineStatus.jsp"

# Regional data API (from KDCA or other government sources)
REGIONAL_DATA_URL = "https://ncv.kdca.go.kr/api/stats/regionalStatus"

# Secondary source: Our World in Data
OWID_VACCINATION_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/country_data/South Korea.csv"

# Open-source community project data (provides curated Korean COVID data)
OSS_VACCINATION_URL = "https://github.com/jooeungen/coronaboard_kr/blob/master/korea_data.csv"

# Tertiary source: Johns Hopkins CSSE
JH_CASES_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
JH_DEATHS_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"

# Final fallback: WHO
WHO_API_URL = "https://covid19.who.int/WHO-COVID-19-global-data.csv"

# Directory paths
import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, 'data', 'processed')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

# Define date format for consistency
DATE_FORMAT = '%Y-%m-%d'

# South Korea population (approximate)
POPULATION = 51_000_000

# Define regions in South Korea
REGIONS = [
    'Seoul', 'Busan', 'Daegu', 'Incheon', 'Gwangju', 'Daejeon', 'Ulsan', 'Sejong',
    'Gyeonggi', 'Gangwon', 'Chungbuk', 'Chungnam', 'Jeonbuk', 'Jeonnam', 'Gyeongbuk', 'Gyeongnam', 'Jeju'
]

# Define vaccine types
VACCINE_TYPES = ['Pfizer', 'Moderna', 'AstraZeneca', 'Janssen', 'Novavax']

# Define color palette for consistent visualizations
COLOR_PALETTE = {
    'First Dose': '#4e79a7',
    'Second Dose': '#f28e2c',
    'Booster': '#59a14f',
    'Cases': '#e15759',
    'Deaths': '#76b7b2',
    'Tests': '#edc949',
    'Pfizer': '#af7aa1',
    'Moderna': '#ff9da7',
    'AstraZeneca': '#9c755f',
    'Janssen': '#bab0ab',
    'Novavax': '#d3d3d3'
}

# Visualization settings
VISUALIZATION_DPI = 300
FIGURE_SIZE = (12, 8)