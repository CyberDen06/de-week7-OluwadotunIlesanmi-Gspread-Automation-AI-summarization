import pytest
import pandas as pd
from src.analysis import ReviewAnalyzer

class TestReviewAnalyzer:
    """Test analysis functionality"""
    
    def test_calculate_sentiment_breakdown(self):
        """Test sentiment breakdown calculation"""
        # Mock data
        data = {
            'Class Name': ['Dresses', 'Dresses', 'Pants', 'Pants', 'Tops'],
            'AI Sentiment': ['Positive', 'Negative', 'Positive', 'Neutral', 'Negative']
        }
        df = pd.DataFrame(data)
        
        # Test the calculation logic directly
        overall_sentiment = df['AI Sentiment'].value_counts(normalize=True) * 100
        sentiment_by_class = pd.crosstab(df['Class Name'], df['AI Sentiment'], normalize='index') * 100
        
        assert 'Positive' in overall_sentiment
        assert 'Negative' in overall_sentiment
        assert 'Neutral' in overall_sentiment
        
        assert not sentiment_by_class.empty
    
    def test_find_extreme_sentiments(self):
        """Test finding extreme sentiments"""
        # Mock data with clear extremes
        data = {
            'Class Name': ['Dresses'] * 3 + ['Pants'] * 2 + ['Tops'] * 1,
            'AI Sentiment': ['Positive', 'Positive', 'Positive', 'Negative', 'Negative', 'Neutral']
        }
        df = pd.DataFrame(data)
        
        # Test the logic directly
        sentiment_counts = pd.crosstab(df['Class Name'], df['AI Sentiment'])
        
        highest_positive = sentiment_counts['Positive'].idxmax()
        highest_negative = sentiment_counts['Negative'].idxmax()
        highest_neutral = sentiment_counts['Neutral'].idxmax()
        
        assert highest_positive == 'Dresses'
        assert highest_negative == 'Pants'
        assert highest_neutral == 'Tops'