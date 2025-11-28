import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, Any, List
import logging
from src.utils import GoogleSheetsManager

logger = logging.getLogger(__name__)

class ReviewAnalyzer:
    """Analyze processed review data"""
    
    def __init__(self, sheet_name: str):
        self.sheets_manager = GoogleSheetsManager()
        self.sheet_name = sheet_name
        self.sheets_manager.open_sheet(sheet_name)
    
    def get_processed_data(self) -> pd.DataFrame:
        """Get processed data as DataFrame"""
        data = self.sheets_manager.get_worksheet_data('processed')
        return pd.DataFrame(data)
    
    def calculate_sentiment_breakdown(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate sentiment breakdown by clothing class"""
        if 'AI Sentiment' not in df.columns or 'Class Name' not in df.columns:
            logger.error("Required columns not found in data")
            return {}
        
        # Sentiment distribution overall
        overall_sentiment = df['AI Sentiment'].value_counts(normalize=True) * 100
        
        # Sentiment by clothing class
        sentiment_by_class = pd.crosstab(
            df['Class Name'], 
            df['AI Sentiment'], 
            normalize='index'
        ) * 100
        
        return {
            'overall_sentiment': overall_sentiment.to_dict(),
            'sentiment_by_class': sentiment_by_class.to_dict(),
            'raw_sentiment_by_class': sentiment_by_class
        }
    
    def find_extreme_sentiments(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Find clothing classes with highest positive, negative, and neutral sentiments"""
        if 'AI Sentiment' not in df.columns or 'Class Name' not in df.columns:
            return {}
        
        # Count sentiments by class
        sentiment_counts = pd.crosstab(df['Class Name'], df['AI Sentiment'])
        
        # Find classes with highest counts for each sentiment
        results = {}
        
        for sentiment in ['Positive', 'Negative', 'Neutral']:
            if sentiment in sentiment_counts.columns:
                max_class = sentiment_counts[sentiment].idxmax()
                max_count = sentiment_counts[sentiment].max()
                results[f'highest_{sentiment.lower()}'] = {
                    'class_name': max_class,
                    'count': max_count,
                    'percentage': (max_count / len(df)) * 100
                }
        
        return results
    
    def generate_visualizations(self, df: pd.DataFrame, save_path: str = 'charts/'):
        """Generate analytical visualizations"""
        import os
        os.makedirs(save_path, exist_ok=True)
        
        # 1. Overall Sentiment Distribution
        plt.figure(figsize=(10, 6))
        sentiment_counts = df['AI Sentiment'].value_counts()
        plt.pie(sentiment_counts.values, labels=sentiment_counts.index, autopct='%1.1f%%')
        plt.title('Overall Sentiment Distribution')
        plt.savefig(f'{save_path}overall_sentiment.png', bbox_inches='tight')
        plt.close()
        
        # 2. Sentiment by Clothing Class
        plt.figure(figsize=(12, 8))
        sentiment_by_class = pd.crosstab(df['Class Name'], df['AI Sentiment'])
        sentiment_by_class.plot(kind='bar', stacked=True)
        plt.title('Sentiment Distribution by Clothing Class')
        plt.xlabel('Clothing Class')
        plt.ylabel('Number of Reviews')
        plt.xticks(rotation=45)
        plt.legend(title='Sentiment')
        plt.tight_layout()
        plt.savefig(f'{save_path}sentiment_by_class.png', bbox_inches='tight')
        plt.close()
        
        # 3. Action Needed Analysis
        plt.figure(figsize=(10, 6))
        action_by_class = pd.crosstab(df['Class Name'], df['Action Needed?'])
        action_by_class.plot(kind='bar')
        plt.title('Action Needed by Clothing Class')
        plt.xlabel('Clothing Class')
        plt.ylabel('Number of Reviews')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'{save_path}action_needed.png', bbox_inches='tight')
        plt.close()
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive analysis report"""
        df = self.get_processed_data()
        
        if df.empty:
            return {'error': 'No data available for analysis'}
        
        # Calculate metrics
        sentiment_breakdown = self.calculate_sentiment_breakdown(df)
        extreme_sentiments = self.find_extreme_sentiments(df)
        
        # Generate visualizations
        self.generate_visualizations(df)
        
        # Compile report
        report = {
            'total_reviews': len(df),
            'sentiment_distribution': sentiment_breakdown.get('overall_sentiment', {}),
            'extreme_sentiments': extreme_sentiments,
            'action_needed_count': len(df[df['Action Needed?'] == 'Yes']),
            'action_needed_percentage': (len(df[df['Action Needed?'] == 'Yes']) / len(df)) * 100
        }
        
        return report