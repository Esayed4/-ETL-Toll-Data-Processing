# ETL Toll Data Processing

This project implements an ETL (Extract, Transform, Load) process using Apache Airflow. The workflow extracts data from various file formats, transforms it, and consolidates it into a single dataset for further analysis. This is part of a final assignment for mastering ETL techniques.

## Project Structure

- **DAG Definition**: The main logic is defined in `ETL_toll_data.py`, where tasks are orchestrated using Airflow.
- **Data Sources**:
  - CSV file: `vehicle-data.csv`
  - TSV file: `tollplaza-data.tsv`
  - Fixed-width file: `payment-data.txt`
- **Staging Area**: Extracted data is stored in a staging directory for further processing.

## Requirements

- Apache Airflow
- Bash
- Python 3.x

## Installation

1. **Clone the repository**:
    ```sh
    git clone https://github.com/yourusername/etl-toll-data-processing.git
    cd etl-toll-data-processing
    ```

2. **Install Apache Airflow**:
   Follow the [official installation guide](https://airflow.apache.org/docs/apache-airflow/stable/start.html#installation) for your environment.

3. **Set up the Airflow environment**:
   Configure your Airflow instance as per your requirements.

## Usage

1. Place your data files (`tolldata.tgz`, `vehicle-data.csv`, `tollplaza-data.tsv`, `payment-data.txt`) in the appropriate directory as specified in the DAG.

2. Start the Airflow web server:
    ```sh
    airflow webserver --port 8080
    ```

3. Start the Airflow scheduler:
    ```sh
    airflow scheduler
    ```

4. Access the Airflow UI by navigating to `http://localhost:8080` in your web browser.

5. Trigger the `ETL_toll_data` DAG from the UI.

## Workflow Overview

The ETL process consists of the following tasks:

1. **Unzip Data**: Extracts the compressed data file.
2. **Extract Data from CSV**: Reads specific columns from a CSV file.
3. **Extract Data from TSV**: Converts a TSV file to CSV and extracts specific columns.
4. **Extract Data from Fixed Width**: Reads specific character positions from a fixed-width file and converts spaces to commas.
5. **Consolidate Data**: Merges all extracted data files into a single CSV file.
6. **Transform Data**: Transforms data by converting specific columns to uppercase.

## Additional Notes

- Ensure that the paths in the `ETL_toll_data.py` script match your file structure.
- Modify the email settings in the `default_args` dictionary if you wish to receive notifications.

## Contributing

If you wish to contribute to this project, feel free to fork the repository and submit a pull request.

## License

This project is licensed under the MIT License.
