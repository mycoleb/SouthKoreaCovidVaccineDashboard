# South Korea COVID-19 Vaccination Analysis

This project analyzes COVID-19 vaccination data from South Korea. It fetches data from multiple official sources, processes it to extract meaningful insights, and generates visualizations to present the findings in an interactive dashboard.

## Features

- **Robust Real Data Collection**: Fetches COVID-19 vaccination data exclusively from official and reliable sources:
  - Korean Disease Control and Prevention Agency (KDCA)
  - Ministry of Health and Welfare (MOHW)
  - Our World in Data (OWID)
  - Johns Hopkins University CSSE
  - World Health Organization (WHO)
  - Open-source community datasets
- **Automatic Fallback System**: If primary data sources are unavailable, the system automatically tries alternative sources
- **Enhanced Error Handling**: Detailed error reporting for each data source with specific error types
- **Automatic Retries**: Configurable retry mechanism for transient network issues
- **Data Processing**: Cleans and analyzes the data to extract insights
- **Visualization**: Creates comprehensive visualizations:
  - Vaccination progress over time
  - Regional comparison of vaccination rates
  - Distribution of different vaccine types
  - Impact of vaccination on COVID-19 cases
  - Regional heatmaps
- **Dashboard**: Combines all visualizations into a single interactive HTML dashboard

## Project Structure

```
project/
├── __init__.py             # Package initialization
├── config.py               # Configuration settings and constants
├── data_fetcher.py         # Data fetching from real sources
├── data_processor.py       # Data cleaning and processing
├── main.py                 # Main script that runs the entire workflow
├── visualizer.py           # Data visualization functions
├── README.md               # Project documentation
├── data/                   # Directory for storing data
│   ├── raw/                # Raw data from sources
│   └── processed/          # Processed data
├── output/                 # Generated visualizations and dashboard
└── utils/
    ├── __init__.py         # Utilities package initialization 
    └── logging_utils.py    # Logging configuration and utilities
```

## Data Sources

This project uses the following data sources in order of priority:

1. **Korean Disease Control and Prevention Agency (KDCA)**:
   - Primary source for vaccination data, daily statistics, and regional breakdowns
   - URL: https://ncv.kdca.go.kr/

2. **Ministry of Health and Welfare (MOHW)**:
   - Secondary source for Korean vaccination data
   - URL: https://www.mohw.go.kr/

3. **Our World in Data (OWID)**:
   - International source that compiles vaccination data for South Korea
   - URL: https://github.com/owid/covid-19-data/tree/master/public/data/vaccinations/country_data

4. **Johns Hopkins University CSSE**:
   - Source for cases and deaths data
   - URL: https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_time_series

5. **World Health Organization (WHO)**:
   - Final fallback source
   - URL: https://covid19.who.int/

6. **Open-Source Community Data**:
   - Community-maintained datasets focused on South Korea
   - URL: https://github.com/jooeungen/coronaboard_kr (example)

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/sk-covid-vaccination.git
   cd sk-covid-vaccination
   ```

2. Create and activate a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

4. Run the setup script to ensure all directories exist:
   ```
   python setup.py
   ```

## Usage

### Running the Analysis

Run the main script to execute the entire workflow:

```
python main.py
```

This will:
1. Fetch the latest data from multiple real sources (with automatic fallbacks)
2. Process the data
3. Generate visualizations
4. Create a dashboard
5. Open the dashboard in your default web browser

### Command Line Options

The script accepts the following command line arguments:

- `--refresh`: Force refresh data from sources (ignore cache)
- `--no-browser`: Do not open the dashboard in a browser after completion
- `--retry N`: Set the number of retry attempts for data fetching (default: 3)

Example:
```
python main.py --refresh --retry 5
```

## Error Handling

The project includes comprehensive error handling:

1. **Source-specific error detection**: Different error types (network, timeout, HTTP status, parsing) are detected and reported separately for each source
2. **Automatic fallbacks**: If one source fails, the system automatically tries alternative sources
3. **Caching mechanism**: If all real-time sources fail but cached data exists, that data is used as a fallback
4. **Detailed error reporting**: The log files contain detailed error information to aid in troubleshooting
5. **Retry mechanism**: Configurable retry mechanism for intermittent failures

## Output

The analysis generates the following outputs in the `output/` directory:

- `vaccination_progress.png`: Line chart showing vaccination progress over time
- `regional_comparison.png`: Bar chart comparing vaccination rates across regions
- `vaccine_distribution.png`: Pie chart showing distribution of vaccine types
- `vaccination_impact.png`: Line chart showing impact of vaccination on COVID-19 cases
- `regional_heatmap.png`: Heatmap of vaccination rates by region
- `dashboard.html`: Interactive HTML dashboard combining all visualizations

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Vaccination data is sourced from multiple official sources including KDCA and Our World in Data
- Cases and deaths data is sourced from Johns Hopkins University CSSE and WHO
- The visualization designs are inspired by the Our World in Data COVID-19 dashboard

## TODO

- Add Korean language support for dashboard and visualizations
- Implement advanced web scraping for more detailed regional data from KDCA website
- Add more demographic analysis (vaccination by age group, gender, etc.)
- Implement automatic data updates via GitHub Actions
- Enhance interactive charts using Plotly
- Create a web application for real-time monitoring
- Add data validation checks to verify consistency across sources