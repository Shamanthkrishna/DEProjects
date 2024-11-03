# ETL Process for Largest Banks Data

## Overview

This repository contains a Python script that performs an ETL (Extract, Transform, Load) process to gather and analyze data from an archived Wikipedia page listing the largest banks in the world. The script extracts data about each bank's market capitalization in USD and transforms it into multiple currencies (GBP, EUR, INR) using exchange rates sourced from a CSV file. Finally, it loads the transformed data into a CSV file and an SQLite database for further analysis.

## Features

- **Data Extraction**: Scrapes bank data from the archived Wikipedia page.
- **Data Transformation**: Converts market capitalization values from USD to GBP, EUR, and INR using real-time exchange rates.
- **Data Loading**: Saves the transformed data to a CSV file and loads it into an SQLite database.
- **Data Querying**: Runs SQL queries to retrieve and display data from the SQLite database.

## Requirements

Before running the script, ensure you have the following installed:

- Python 3.x
- Required Python libraries:
  - `requests`
  - `pandas`
  - `numpy`
  - `beautifulsoup4`
  - `sqlite3` (included with Python)

You can install the required libraries using pip:

```bash
pip install requests pandas numpy beautifulsoup4
```

## Usage

1. **Clone the repository**:

   ```bash
   git clone https://github.com/Shamanthkrishna/DEProjects.git
   cd DEProjects
   ```

2. **Prepare the exchange rates file**:

   Create a CSV file named `exchange_rate.csv` in the same directory as the script. The file should contain currency exchange rates in the following format:

   ```
   Currency,Rate
   USD,1.00
   GBP,0.80
   EUR,0.90
   INR,74.50
   ```

3. **Run the script**:

   Execute the script using Python:

   ```bash
   python etl_script.py
   ```

   The script will log the progress of the ETL process and save the data to `Largest_banks_data.csv` and an SQLite database named `World_Economies.db`.

## Example Queries

Once the data is loaded into the SQLite database, you can run the following SQL queries to analyze the data:

- Retrieve all bank data:

  ```sql
  SELECT * FROM Largest_banks;
  ```

- Calculate the average market capitalization in GBP:

  ```sql
  SELECT AVG(MC_GBP_Billion) FROM Largest_banks;
  ```

- List the names of the first five banks:

  ```sql
  SELECT Name FROM Largest_banks LIMIT 5;
  ```

## Logging

The script logs important milestones and errors during execution in a file named `code_log.txt`. This can be helpful for debugging and tracking the progress of the ETL process.

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.
