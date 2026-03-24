import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AdzunaIngestor:
    def __init__(self):
        self.app_id = os.getenv("ADZUNA_APP_ID")
        self.app_key = os.getenv("ADZUNA_APP_KEY")
        self.bucket = os.getenv("S3_BUCKET_NAME")
        self.base_url = "https://api.adzuna.com/v1/api/jobs/pl/search"
        self.s3 = boto3.client('s3')

    def get_yesterday_path(self) -> str:
        """Generates Hive-style partition path for yesterday's date."""
        yesterday = datetime.now() - timedelta(days=1)
        return yesterday.strftime('year=%Y/month=%m/day=%d')

    def fetch_api_page(self, page: int) -> Dict[str, Any]:
        """Handles the HTTP request to Adzuna for a specific page."""
        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "results_per_page": 50,
            "max_days_old": 1,
            "content-type": "application/json"
        }
        response = requests.get(f"{self.base_url}/{page}", params=params)
        response.raise_for_status()
        return response.json()

    def upload_to_s3(self, data: Dict[str, Any], page: int, date_path: str):
        """Standardizes the S3 key and uploads the JSON payload."""
        file_key = f"raw_jsons/{date_path}/jobs_page_{page}.json"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=file_key,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        logger.info(f"Uploaded: {file_key}")

    def get_total_pages(self, first_page_data: Dict[str, Any]) -> int:
        """Calculates total pages based on job count."""
        count = first_page_data.get('count', 0)
        return (count // 50) + 1

    def run(self, limit_test: bool = True):
        """Orchestrates the ingestion flow."""
        logger.info("Starting daily ingestion...")
        date_path = self.get_yesterday_path()
        
        first_page = self.fetch_api_page(1)
        total_pages = self.get_total_pages(first_page)
        
        if limit_test:
            total_pages = min(total_pages, 3)
            logger.warning(f"Test mode active: Limiting to {total_pages} pages.")

        for page in range(1, total_pages + 1):
            try:
                data = first_page if page == 1 else self.fetch_api_page(page)
                self.upload_to_s3(data, page, date_path)
                
                if page < total_pages:
                    time.sleep(2.5) 
                    
            except Exception as e:
                logger.error(f"Failed on page {page}: {e}")
                continue

        logger.info("Ingestion cycle complete.")

if __name__ == "__main__":
    ingestor = AdzunaIngestor()
    ingestor.run(limit_test=False)