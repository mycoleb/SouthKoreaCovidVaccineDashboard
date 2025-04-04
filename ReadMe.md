# South Korea COVID-19 Vaccination Analysis

This project analyzes COVID-19 vaccination data from South Korea. It fetches data, processes it to extract meaningful insights, and generates visualizations to present the findings in an interactive dashboard.

## Features

- **Data Collection**: Fetches COVID-19 vaccination data from South Korea's official sources
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
├── data_fetcher.py         # Data fetching from API or local files
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

## Usage

### Running the Analysis

Run the main script to execute the entire workflow:

```
python main.py
```

This will:
1. Fetch the latest data from sources (or use cached data if available)
2. Process the data
3. Generate visualizations
4. Create a dashboard
5. Open the dashboard in your default web browser

### Command Line Options

The script accepts the following command line arguments:

- `--refresh`: Force refresh data from sources (ignore cache)
- `--no-browser`: Do not open the dashboard in a browser after completion

Example:
```
python main.py --refresh --no-browser
```

## Requirements

- Python 3.8+
- pandas
- numpy
- matplotlib
- seaborn
- requests
- BeautifulSoup4

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

- Data is sourced from the Korea Disease Control and Prevention Agency (KDCA)
- The visualization designs are inspired by the Our World in Data COVID-19 dashboard

## TODO

- Add more demographic analysis (vaccination by age group, gender, etc.)
- Implement automatic data updates via GitHub Actions
- Add interactive charts using Plotly
- Create a web application for real-time monitoring