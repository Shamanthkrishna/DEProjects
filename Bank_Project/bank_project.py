import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import numpy as np
import sqlite3

# URL of the archived Wikipedia page containing the list of largest banks
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
# Columns for the DataFrame
table_attribs = ["Name", "MC_USD_Billion"]
table_attribs_1 = ["MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]

# Database and CSV file names
db_name = 'World_Economies.db'
table_name = 'Largest_banks'
csv_path = 'Largest_banks_data.csv'

# Log progress to keep track of stages during ETL operations
def log_progress(message):
    ''' This function logs a given message with a timestamp to a log file. '''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # Get current timestamp
    timestamp = now.strftime(timestamp_format) 
    log_message = f"{timestamp} : {message}"
    with open("code_log.txt", "a") as f: 
        f.write(timestamp + ' : ' + message + '\n')
    print(log_message)

# Extract data from the website
def extract(url, table_attribs):
    ''' This function scrapes bank data from the website and returns it as a DataFrame. '''
    
    # Request the web page content
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    
    # Locate the table in HTML
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    
    # Process each row in the table
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            # Extract bank name from <a> tag(s) in the second column
            a_tags = col[1].find_all('a')
            if len(a_tags) > 1:
                # Extract text from the second <a> tag
                bank_name = a_tags[1].text.strip()
            elif len(a_tags) == 1:
                # If only one <a> tag, extract from the first
                bank_name = a_tags[0].text.strip()
            else:
                bank_name = None  # If no <a> tags found
            
            # Process market cap value in USD
            try:
                mc_value = float(col[2].contents[0].replace(',', '').rstrip())
            except (IndexError, ValueError):
                mc_value = np.nan
    
            # Store the data in a temporary dictionary
            data_dict = {"Name": bank_name, "MC_USD_Billion": mc_value}
            df1 = pd.DataFrame(data_dict, index=[0])
            
            # Append row to the DataFrame
            df = pd.concat([df, df1], ignore_index=True)

    return df

# Transform data by adding columns for other currencies
def transform(df):
    ''' This function adds columns for the Market Cap in GBP, EUR, and INR currencies. '''
    
    # Get exchange rate information from a CSV file
    try:
        exchange_rates_df = pd.read_csv('./exchange_rate.csv')
        exchange_rate = exchange_rates_df.set_index('Currency').to_dict()['Rate']
    except FileNotFoundError:
        log_progress("Error: exchange_rate.csv file not found.")
        return None  # or raise an exception
    try:
        # Calculate values in GBP, EUR, and INR using exchange rates
        df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
        df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
        df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    except KeyError as e:
        log_progress(f"Error: Exchange rate for {e} not found.")
        return None
    
    return df

# Load data to a CSV file
def load_to_csv(df, csv_path):
    ''' This function saves the DataFrame to a CSV file. '''
    if df is not None:
        df.to_csv(csv_path, index=False)
    else:
        log_progress("DataFrame is None, CSV file not saved.")

# Load data into a database
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the DataFrame to a database table. '''
    if df is not None:
        df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    else:
        log_progress("DataFrame is None, data not loaded to Database.")

# Run a SQL query on the database and print results
def run_query(query_statement, sql_connection):
    ''' This function runs a SQL query and prints the results. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Log the start of the ETL process
log_progress("Preliminaries complete. Initiating ETL process.")

# Extract data from the webpage
df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process.")

# Transform data to add currency conversions
df = transform(df)
if df is not None:
    log_progress("Data transformation complete. Initiating loading process.")
else:
    log_progress("Data transformation failed. Exiting process.")
    exit()

# Save the transformed data to a CSV file
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')

# Connect to the SQLite database
try:
    sql_connection = sqlite3.connect(db_name)
    log_progress('SQL Connection initiated.')
    
    # Load data into the SQLite database table
    load_to_db(df, sql_connection, table_name)
    log_progress('Data loaded to Database as table. Running the query')
    
    # Define SQL queries to run on the database
    query_statement_1 = f"SELECT * from {table_name}"
    query_statement_2 = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
    query_statement_3 = f"SELECT Name from {table_name} LIMIT 5"
    
    # Run queries and print results
    run_query(query_statement_1, sql_connection)
    run_query(query_statement_2, sql_connection)
    run_query(query_statement_3, sql_connection)
    
except sqlite3.Error as e:
    log_progress(f"Error connecting to the database: {e}")
finally:
    log_progress('Process Complete.')
    if 'sql_connection' in locals() and sql_connection:
        sql_connection.close()
