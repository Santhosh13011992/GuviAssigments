import io
import os
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import asyncio
import aiofiles
import configparser

# Load configuration
config = configparser.ConfigParser()
config.read('config.ini')

class Logger:
    """Singleton Logger class to log messages to a file."""
    _instance = None

    def __new__(cls):
        """Create a new instance of Logger if it doesn't exist."""
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
        return cls._instance

    def log(self, msg):
        """Log a message with a timestamp to the log file."""
        log_file = config.get('Logging', 'log_file')
        with open(log_file, 'a') as f:
            f.write(f"{datetime.now()}: {msg}\n")

def log_function(context=None):
    """Decorator to log the start and completion of an asynchronous function with context."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            logger = Logger()
            context_info = f"Context: {context}" if context else "No context provided"
            logger.log(f"{func.__name__} started. {context_info}")
            result = await func(*args, **kwargs)
            logger.log(f"{func.__name__} completed. {context_info}")
            return result
        return wrapper
    return decorator

class DataExtractor:
    """Base class for data extractors."""
    async def extract(self, path):
        raise NotImplementedError("Extract method not implemented.")

class CSVExtractor(DataExtractor):
    """Extractor class for CSV files."""
    @log_function(context="Extracting CSV data")
    async def extract(self, path):
        """Extract data from a CSV file."""
        async with aiofiles.open(path, mode='r') as f:
            content = await f.read()
            return pd.read_csv(io.StringIO(content))

class JSONExtractor(DataExtractor):
    """Extractor class for JSON files."""
    @log_function(context="Extracting JSON data")
    async def extract(self, path):
        """Extract data from a JSON file."""
        async with aiofiles.open(path, mode='r') as f:
            content = await f.read()
            return pd.read_json(io.StringIO(content), lines=True)

class XMLExtractor(DataExtractor):
    """Extractor class for XML files."""
    @log_function(context="Extracting XML data")
    async def extract(self, path):
        """Extract data from an XML file."""
        return pd.DataFrame([{child.tag: child.text for child in elem} for elem in ET.parse(path).getroot()])

class DataExtractorFactory:
    """Factory class to create data extractors based on file type."""
    @staticmethod
    def get_extractor(ft):
        """Return the appropriate extractor class based on the file type."""
        extractors = {
            'csv': CSVExtractor(),
            'json': JSONExtractor(),
            'xml': XMLExtractor()
        }
        return extractors.get(ft, lambda: ValueError("Unsupported file type"))

class ETLProcess:
    """Class to handle the ETL (Extract, Transform, Load) process."""
    def __init__(self, output_file):
        """Initialize the ETL process with the output file path."""
        self.output_file = output_file

    @log_function(context="Transforming data")
    async def transform_data(self, df):
        """Transform the data by converting height and weight to metric units."""
        df[['height', 'weight']] = df[['height', 'weight']].apply(pd.to_numeric, errors='coerce').multiply([0.0254, 0.453592])
        return df[['name', 'height', 'weight']]

    @log_function(context="Loading data")
    async def load_data(self, df):
        """Load the transformed data into a CSV file."""
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        await asyncio.to_thread(df.to_csv, self.output_file, index=False)

    @log_function(context="Processing file")
    async def process_file(self, file: str) -> pd.DataFrame:
        """Process a single file by extracting its data."""
        try:
            extractor = DataExtractorFactory.get_extractor(file.split('.')[-1])
            return await extractor.extract(file)
        except ValueError as ve:
            Logger().log(f"ValueError processing file {file}: {ve}")
        except FileNotFoundError as fnf_error:
            Logger().log(f"FileNotFoundError: {fnf_error}")
        except Exception as e:
            Logger().log(f"Error processing file {file}: {e}")
        return pd.DataFrame()

    @log_function(context="ETL Process")
    async def run(self):
        """Run the ETL process on all files in the specified directory."""
        files = glob.glob('./unzipped_folder/*')
        tasks = [self.process_file(file) for file in files]
        results = await asyncio.gather(*tasks)
        await self.load_data(await self.transform_data(pd.concat(results, ignore_index=True)))

if __name__ == "__main__":
    output_file_path = os.path.join(os.getcwd(), config.get('Output', 'output_file'))
    asyncio.run(ETLProcess(output_file=output_file_path).run())
