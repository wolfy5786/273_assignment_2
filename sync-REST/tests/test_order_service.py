import pytest
import requests
import time
import os
import structlog
from logging_utils import c_logging as logger



# Get the order service URL from environment or use default
ORDER_SERVICE_URL = os.getenv('ORDER_SERVICE_URL', 'http://localhost:8080')
logger.configure_logging()
log = structlog.get_logger()


class TestOrderServiceBaseline:
    """Baseline tests for order_service POST /order endpoint"""
    
    def test_health_check(self):
        """Verify the order service is running"""
        response = requests.get(f'{ORDER_SERVICE_URL}/health')
        assert response.status_code == 200
        assert response.json()['status'] == 'ok'
    
    def test_order_no_delay_no_fault(self):
        """
        Test Case 1: POST /order with delay=0, fault=false
        Expected: Successful order with all services responding
        """
        start_time = time.time()
        
        params = {
            'delay': 0,
            'fault': 'false'
        }
        
        response = requests.post(
            f'{ORDER_SERVICE_URL}/order',
            params=params,
            timeout=10
        )
        
        end_time = time.time()
        latency_ms = int((end_time - start_time) * 1000)
        
        # Assert response status
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        
        # Parse response JSON
        data = response.json()
        
        # Assert all services are okay
        assert data.get('inventory_service') == 'okay', "Inventory service should be okay"
        assert data.get('notification_service') == 'okay', "Notification service should be okay"
        assert data.get('order_service') == 'okay', "Order service should be okay"
        
        # Assert responses from downstream services are present
        assert 'i_message' in data, "Should contain inventory service message"
        assert 'n_message' in data, "Should contain notification service message"
        
        # Log test metrics
        log.info(f"Status Code: {response.status_code}, Latency: {latency_ms}ms,  Response: {data}")
        
        # Assert reasonable latency (should be quick with no delay)
        assert latency_ms < 5000, f"Latency {latency_ms}ms exceeds expected threshold"
    
    def test_order_with_delay_no_fault(self):
        """
        Test Case 2: POST /order with delay=2, fault=false
        Expected: Successful order but with added delay from inventory service
        """
        start_time = time.time()
        
        params = {
            'delay': 2,
            'fault': 'false'
        }
        
        response = requests.post(
            f'{ORDER_SERVICE_URL}/order',
            params=params,
            timeout=10
        )
        
        end_time = time.time()
        latency_ms = int((end_time - start_time) * 1000)
        
        # Assert response status
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        
        # Parse response JSON
        data = response.json()
        
        # Assert all services are okay
        assert data.get('inventory_service') == 'okay', "Inventory service should be okay"
        assert data.get('notification_service') == 'okay', "Notification service should be okay"
        assert data.get('order_service') == 'okay', "Order service should be okay"
        
        # Assert responses from downstream services are present
        assert 'i_message' in data, "Should contain inventory service message"
        assert 'n_message' in data, "Should contain notification service message"
        
        # Log test metrics
        log.info(f"--- Test 2 Metrics ---  Status Code: {response.status_code}, Latency: {latency_ms}ms,  Response: {data}")
        
        # Assert latency includes the delay (should be at least 2000ms)
        assert latency_ms >= 2000, f"Latency {latency_ms}ms should be at least 2000ms with delay=2"
        assert latency_ms < 7000, f"Latency {latency_ms}ms exceeds expected threshold"


    def test_order_with_longer_delay_no_fault(self):
        """
        Test Case 3: POST /order with delay=4, fault=false
        Expected: Successful order but with longer delay from inventory service
        """
        start_time = time.time()
        
        params = {
            'delay': 4,
            'fault': 'false'
        }
        
        response = requests.post(
            f'{ORDER_SERVICE_URL}/order',
            params=params,
            timeout=15
        )
        
        end_time = time.time()
        latency_ms = int((end_time - start_time) * 1000)
        
        
        # Parse response JSON
        data = response.json()
        
        # Log test metrics
        log.info(f"--- Test 3 Metrics ---  Status Code: {response.status_code}, Latency: {latency_ms}ms,  Response: {data}")


        



if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, '-v', '-s'])
