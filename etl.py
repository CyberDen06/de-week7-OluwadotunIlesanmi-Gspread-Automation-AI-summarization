import pandas as pd
from typing import List, Dict, Any
from src.utils import GoogleSheetsManager, GroqClient, determine_action_needed, clean_text
import logging

logger = logging.getLogger(__name__)

class ETLPipeline:
    """ETL Pipeline for review analysis"""
    
    def __init__(self, sheet_name: str):
        self.sheets_manager = GoogleSheetsManager()
        self.groq_client = GroqClient()
        self.sheet_name = sheet_name
        self.sheets_manager.open_sheet(sheet_name)
    
    def extract_raw_data(self) -> List[Dict]:
        """Extract data from raw_data worksheet"""
        logger.info("Extracting data from raw_data worksheet...")
        return self.sheets_manager.get_worksheet_data('raw_data')
    
    def transform_to_staging(self, raw_data: List[Dict]) -> List[Dict]:
        """Transform raw data to staging format"""
        logger.info("Transforming data for staging...")
        
        staging_data = []
        for row in raw_data:
            # Clean and transform data
            cleaned_row = {}
            for key, value in row.items():
                if isinstance(value, str):
                    cleaned_row[key] = clean_text(value)
                else:
                    cleaned_row[key] = value
            
            # Ensure all required fields
            if 'Review Text' not in cleaned_row:
                cleaned_row['Review Text'] = ''
            
            staging_data.append(cleaned_row)
        
        return staging_data
    
    def transform_to_processed(self, staging_data: List[Dict]) -> List[Dict]:
        """Transform staging data to processed format with LLM analysis"""
        logger.info("Processing data with LLM analysis...")
        
        processed_data = []
        
        for i, row in enumerate(staging_data):
            logger.info(f"Processing row {i+1}/{len(staging_data)}")
            
            review_text = row.get('Review Text', '')
            
            # Analyze review with Groq LLM
            analysis_result = self.groq_client.analyze_review(review_text)
            
            # Create processed row
            processed_row = row.copy()
            processed_row['AI Sentiment'] = analysis_result['sentiment']
            processed_row['AI Summary'] = analysis_result['summary']
            processed_row['Action Needed?'] = determine_action_needed(analysis_result['sentiment'])
            
            processed_data.append(processed_row)
            
            # Small delay to respect API limits
            import time
            time.sleep(0.5)
        
        return processed_data
    
    def load_staging_data(self, staging_data: List[Dict]):
        """Load data to staging worksheet"""
        if not staging_data:
            logger.warning("No staging data to load")
            return
        
        headers = list(staging_data[0].keys())
        self.sheets_manager.write_data_to_worksheet('staging', staging_data, headers)
    
    def load_processed_data(self, processed_data: List[Dict]):
        """Load data to processed worksheet"""
        if not processed_data:
            logger.warning("No processed data to load")
            return
        
        headers = list(processed_data[0].keys())
        self.sheets_manager.write_data_to_worksheet('processed', processed_data, headers)
    
    def run_pipeline(self):
        """Run complete ETL pipeline"""
        logger.info("Starting ETL pipeline...")
        
        try:
            # Extract
            raw_data = self.extract_raw_data()
            logger.info(f"Extracted {len(raw_data)} rows from raw_data")
            
            # Transform to staging
            staging_data = self.transform_to_staging(raw_data)
            self.load_staging_data(staging_data)
            
            # Transform to processed
            processed_data = self.transform_to_processed(staging_data)
            self.load_processed_data(processed_data)
            
            logger.info("ETL pipeline completed successfully")
            return processed_data
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}")
            raise