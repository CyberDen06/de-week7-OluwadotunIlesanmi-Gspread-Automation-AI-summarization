import pytest
from src.utils import clean_text, determine_action_needed
from src.utils import GroqClient
import os

class TestUtils:
    """Test utility functions"""
    
    def test_clean_text(self):
        """Test text cleaning function"""
        assert clean_text("  Hello World  ") == "Hello World"
        assert clean_text("") == ""
        assert clean_text(None) == ""
        assert clean_text(123) == ""
    
    def test_determine_action_needed(self):
        """Test action needed determination"""
        assert determine_action_needed("Negative") == "Yes"
        assert determine_action_needed("Positive") == "No"
        assert determine_action_needed("Neutral") == "No"
        assert determine_action_needed("Unknown") == "No"

class TestGroqClient:
    """Test Groq client functionality"""
    
    def test_analyze_review_short_text(self):
        """Test analysis of short review text"""
        client = GroqClient()
        result = client.analyze_review("Short")
        
        assert 'sentiment' in result
        assert 'summary' in result
        assert result['sentiment'] in ['Positive', 'Negative', 'Neutral']
    
    def test_analyze_review_empty_text(self):
        """Test analysis of empty review text"""
        client = GroqClient()
        result = client.analyze_review("")
        
        assert result['sentiment'] == 'Neutral'
        assert 'No review text' in result['summary']