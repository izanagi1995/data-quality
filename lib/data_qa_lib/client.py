"""
HTTP Client for Data QA Backend Communication

Provides an easy interface for authenticating and sending metrics
to the Data QA backend API.
"""

import requests
from typing import Dict, Any, Optional
from pydantic import BaseModel


class DataQAClient:
    """
    HTTP client for communicating with the Data QA backend API.
    
    Uses API key authentication for secure communication between
    the Databricks environment and the backend.
    """
    
    def __init__(self, base_url: str, api_key: str):
        """
        Initialize the Data QA client.
        
        Args:
            base_url: The base URL of the Data QA backend API
            api_key: API key for authentication
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        })
    
    def send_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send collected metrics to the backend API.
        
        Args:
            metrics: Dictionary containing the collected metrics data
            
        Returns:
            Response from the API
            
        Raises:
            requests.RequestException: If the API request fails
        """
        # AI-TODO: Implement actual API endpoint communication
        endpoint = f"{self.base_url}/api/metrics/"
        
        try:
            response = self.session.post(endpoint, json=metrics)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            # AI-NOTE: Specific error handling for API communication failures
            raise requests.RequestException(f"Failed to send metrics to API: {e}")
    
    def health_check(self) -> bool:
        """
        Check if the backend API is accessible and responding.
        
        Returns:
            True if the API is healthy, False otherwise
        """
        try:
            endpoint = f"{self.base_url}/health/"
            response = self.session.get(endpoint)
            return response.status_code == 200
        except requests.RequestException:
            return False