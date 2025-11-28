#!/usr/bin/env python3
"""
Main script for Automated Review Analysis Pipeline
"""

import logging
from src.etl import ETLPipeline
from src.analysis import ReviewAnalyzer
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Run the complete review analysis pipeline"""
    
    # Configuration
    SHEET_NAME = "Women_Clothing_Reviews_Analysis"  # Replace with your actual sheet name
    
    try:
        logger.info("Starting Automated Review Analysis Pipeline")
        
        # Step 1: Run ETL Pipeline
        logger.info("=== STEP 1: Running ETL Pipeline ===")
        etl_pipeline = ETLPipeline(SHEET_NAME)
        processed_data = etl_pipeline.run_pipeline()
        
        # Step 2: Perform Analysis
        logger.info("=== STEP 2: Performing Analysis ===")
        analyzer = ReviewAnalyzer(SHEET_NAME)
        report = analyzer.generate_report()
        
        # Step 3: Display Results
        logger.info("=== STEP 3: Analysis Results ===")
        print("\n" + "="*50)
        print("REVIEW ANALYSIS REPORT")
        print("="*50)
        
        print(f"\nTotal Reviews Processed: {report['total_reviews']}")
        
        print("\nSentiment Distribution:")
        for sentiment, percentage in report['sentiment_distribution'].items():
            print(f"  {sentiment}: {percentage:.1f}%")
        
        print("\nExtreme Sentiment Analysis:")
        for sentiment_type, data in report['extreme_sentiments'].items():
            sentiment_name = sentiment_type.replace('highest_', '').title()
            print(f"  Highest {sentiment_name}: {data['class_name']} ({data['count']} reviews)")
        
        print(f"\nAction Needed:")
        print(f"  Reviews requiring action: {report['action_needed_count']} ({report['action_needed_percentage']:.1f}%)")
        
        print(f"\nCharts generated in 'charts/' directory")
        print("="*50)
        
        # Save report to file
        with open('analysis_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info("Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()