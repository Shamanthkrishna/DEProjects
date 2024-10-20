### Country-GDP ETL Process

This project demonstrates a basic ETL (Extract, Transform, Load) process performed on Country-GDP data sourced from a Wikipedia page. The objective is to extract the GDP data for various countries, transform it into a more useful format, and load the results into a CSV file and an SQLite database for further analysis.

#### Workflow Overview:
1. **Extract**:
    - The function `extract()` fetches the webpage content from the provided URL, which is an archived version of a Wikipedia page listing countries by nominal GDP.
    - The HTML content is parsed using BeautifulSoup to locate the relevant table (`<tbody>`) that contains the required data.
    - The GDP data for each country is extracted and stored in a Pandas DataFrame for further processing.

2. **Transform**:
    - The `transform()` function takes the raw extracted GDP data (in USD Millions) and converts it into USD Billions by dividing by 1000.
    - It also removes any unnecessary commas and ensures that the resulting values are rounded to two decimal places for consistency.
    - The transformed data is then returned with a new column name, `GDP_USD_billions`.

3. **Load**:
    - In the loading phase, the data is saved both to a CSV file and an SQLite database.
    - The `load_to_csv()` function exports the cleaned and transformed data to a CSV file.
    - The `load_to_db()` function stores the data in a database table using an SQLite connection, with the option to replace the table if it already exists.

4. **Query Execution**:
    - Once the data is loaded into the database, the `run_query()` function is used to execute a SQL query to retrieve countries with a GDP of 100 billion USD or more. The results are printed for verification.

5. **Logging**:
    - The `log_progress()` function helps track the progress of the ETL process by recording key actions in a log file along with timestamps.

#### Example Queries:
- The following SQL query is executed as part of the workflow:
  ```sql
  SELECT * FROM Countries_by_GDP WHERE GDP_USD_billions >= 100
  ```

#### Dependencies:
- `requests`: To fetch webpage content.
- `BeautifulSoup`: To parse and extract data from HTML.
- `pandas`: For data manipulation.
- `numpy`: For numerical operations such as rounding.
- `sqlite3`: For database operations.
  
#### Usage:
1. Ensure all dependencies are installed.
2. Run the script to perform the ETL process and generate the output as a CSV file and an SQLite database.
3. Check the generated log file (`etl_project_log.txt`) for progress updates.

This project serves as a simple example of how to handle real-world ETL tasks, from data extraction to transformation and storage in both file and database formats.
