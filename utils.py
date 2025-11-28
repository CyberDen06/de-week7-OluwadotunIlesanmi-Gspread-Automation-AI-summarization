import gspread
import pandas as pd
import time
from typing import List, Dict, Any, Optional
from groq import Groq
from config import Config
import logging

#  logging set up
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GoogleSheetsManager:
    """Manage Google Sheets operations"""
    
    def __init__(self, service_account_file: str = Config.SERVICE_ACCOUNT):
        self.gc = gspread.service_account(filename='sixth-sequencer-479206-h7-a9f744c7ff19.json')
        self.sheet = None
    
    def open_sheet(self, sheet_name: str):
        """Open Google Sheet by name"""
        try:
            self.sheet = self.gc.open(sheet_name)
            logger.info(f"Successfully opened sheet: {sheet_name}")
            return self.sheet
        except Exception as e:
            logger.error(f"Error opening sheet {sheet_name}: {str(e)}")
            raise
    
    def get_worksheet_data(self, worksheet_name: str) -> List[Dict]:
        """Get all data from a worksheet as list of dictionaries"""
        try:
            worksheet = self.sheet.worksheet(worksheet_name)
            return worksheet.get_all_records()
        except Exception as e:
            logger.error(f"Error getting data from {worksheet_name}: {str(e)}")
            return []
    
    def write_data_to_worksheet(self, worksheet_name: str, data: List[Dict], headers: List[str]):
        """Write data to worksheet"""
        try:
            # Clear existing worksheet or create new
            try:
                worksheet = self.sheet.worksheet(worksheet_name)
                worksheet.clear()
            except gspread.WorksheetNotFound:
                worksheet = self.sheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
            
            # Write headers and data
            worksheet.update([headers] + [[row.get(header, '') for header in headers] for row in data])
            logger.info(f"Successfully wrote {len(data)} rows to {worksheet_name}")
            
        except Exception as e:
            logger.error(f"Error writing to {worksheet_name}: {str(e)}")
            raise

class GroqClient:
    """Groq LLM client for sentiment analysis and summarization"""
    
    def __init__(self):
        self.client = Groq(api_key=Config.GROQ_API_KEY)
        self.model = Config.GROQ_MODEL
    
    def analyze_review(self, review_text: str) -> Dict[str, str]:
        """Analyze review text for sentiment and generate summary"""
        if not review_text or len(review_text.strip()) < 10:
            return {
                'sentiment': 'Neutral',
                'summary': review_text if review_text else 'No review text'
            }
        
        prompt = f"""
        Analyze the following product review and provide:
        1. Sentiment (choose only from: Positive, Negative, Neutral)
        2. A one-sentence summary (if text is too short, just return the original text)
        
        Review: "{review_text}"
        
        Format your response as:
        Sentiment: [Positive/Negative/Neutral]
        Summary: [one-sentence summary]
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=100
            )
            
            result = self._parse_llm_response(response.choices[0].message.content)
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing review: {str(e)}")
            return {
                'sentiment': 'Neutral',
                'summary': review_text if review_text else 'Error in analysis'
            }
    
    def _parse_llm_response(self, response: str) -> Dict[str, str]:
        """Parse LLM response to extract sentiment and summary"""
        sentiment = 'Neutral'
        summary = 'No summary generated'
        
        lines = response.split('\n')
        for line in lines:
            if line.lower().startswith('sentiment:'):
                sentiment = line.split(':', 1)[1].strip()
                if sentiment not in Config.SENTIMENT_LABELS:
                    sentiment = 'Neutral'
            elif line.lower().startswith('summary:'):
                summary = line.split(':', 1)[1].strip()
        
        return {'sentiment': sentiment, 'summary': summary}

def determine_action_needed(sentiment: str) -> str:
    """Determine if action is needed based on sentiment"""
    return 'Yes' if sentiment == 'Negative' else 'No'

def clean_text(text: str) -> str:
    """Clean and standardize text data"""
    if not isinstance(text, str):
        return ''
    return text.strip()