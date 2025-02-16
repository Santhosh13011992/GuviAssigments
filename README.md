# GuviAssigments

# ETL Process for Data Extraction, Transformation, and Loading

## Overview
This project implements an ETL process to extract data from CSV, JSON, and XML files, transform it into a standardized format, and load it into a CSV file for further analysis.

## Requirements
- Python 3.x
- Libraries: pandas, glob, xml.etree.ElementTree, datetime

## Steps to Execute
1. **Gather Data Files**
   - Download the dataset:
     ```bash
     wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip
     ```
   - Unzip the downloaded file:
     ```bash
     unzip source.zip -d ./unzipped_folder
     ```

2. **Import Libraries and Set Paths**
   - Import necessary libraries and set up paths for log files and output files.

3. **Define ETL Functions**
   - Create functions for extracting, transforming, and loading data.

4. **Execute ETL Process**
   - Run the ETL process and log each phase.

## Logging
Logs are saved in `log_file.txt` for auditing purposes.

## Output
The final transformed data is saved in `transformed_data.csv`.# ETL Process for Data Extraction, Transformation, and Loading

## Overview
This project implements an ETL process to extract data from CSV, JSON, and XML files, transform it into a standardized format, and load it into a CSV file for further analysis.

## Requirements
- Python 3.x
- Libraries: pandas, glob, xml.etree.ElementTree, datetime, io, os, asyncio, aiofiles

## Steps to Execute
1. **Gather Data Files**
   - Download the dataset:
     ```bash
     wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip
     ```
   - Unzip the downloaded file:
     ```bash
     unzip source.zip -d ./unzipped_folder
     ```

2. **Import Libraries and Set Paths**
   - Import necessary libraries and set up paths for log files and output files.

3. **Define ETL Functions**
   - Create functions for extracting, transforming, and loading data.

4. **Execute ETL Process**
   - Run the ETL process and log each phase.

## Logging
Logs are saved in `log_file.txt` for auditing purposes.

## Output
The final transformed data is saved in `transformed_data.csv`.
