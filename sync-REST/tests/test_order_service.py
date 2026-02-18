import pytest
import requests
import time
import os
import statistics
import structlog
from logging_utils import c_logging as logger



# Get the order service URL from environment or use default
ORDER_SERVICE_URL = os.getenv('ORDER_SERVICE_URL', 'http://localhost:8080')
logger.configure_logging()
log = structlog.get_logger()
# Number of requests to send per test
NUM_REQUESTS = int(os.getenv('NUM_REQUESTS', '10'))

class TestOrderServiceBaseline:
    """Baseline tests for order_service POST /order endpoint"""
    
    @staticmethod
    def calculate_metrics(latencies):
        """Calculate statistical metrics from latency data"""
        sorted_latencies = sorted(latencies)
        return {
            'count': len(latencies),
            'min': min(latencies),
            'max': max(latencies),
            'mean': statistics.mean(latencies),
            'median': statistics.median(latencies),
            'stdev': statistics.stdev(latencies) if len(latencies) > 1 else 0,
            'p50': statistics.median(latencies),
            'p95': sorted_latencies[int(len(sorted_latencies) * 0.95)] if len(latencies) > 1 else latencies[0],
            'p99': sorted_latencies[int(len(sorted_latencies) * 0.99)] if len(latencies) > 1 else latencies[0],
        }
    @staticmethod
    def print_metrics_table(test_name, metrics, params):
        """Log formatted metrics table"""
        print(f"\n{'='*80}")
        print(f"Test: {test_name}")
        print(f"Parameters: delay={params['delay']}, fault={params['fault']}")
        print(f"Number of Requests: {metrics['count']}")
        print(f"{'='*80}")
        print(f"{'Metric':<15} {'Value (ms)':<15}")
        print(f"{'-'*30}")
        print(f"{'Min':<15} {metrics['min']:<15.2f}")
        print(f"{'Max':<15} {metrics['max']:<15.2f}")
        print(f"{'Mean':<15} {metrics['mean']:<15.2f}")
        print(f"{'Median (P50)':<15} {metrics['median']:<15.2f}")
        print(f"{'P95':<15} {metrics['p95']:<15.2f}")
        print(f"{'P99':<15} {metrics['p99']:<15.2f}")
        print(f"{'Std Dev':<15} {metrics['stdev']:<15.2f}")
        print(f"{'='*80}\n")
    
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
        params = {
            'delay': 0,
            'fault': 'false'
        }
        
        latencies = []
        success_count = 0
        failures = []
        
        print(f"\n[Test 1] Sending {NUM_REQUESTS} requests with delay=0, fault=false...")
        
        for i in range(NUM_REQUESTS):
            try:
                start_time = time.time()
                
                response = requests.post(
                    f'{ORDER_SERVICE_URL}/order',
                    params=params,
                    timeout=10
                )
                
                end_time = time.time()
                latency_ms = (end_time - start_time) * 1000
                latencies.append(latency_ms)
                
                # Validate response
                if response.status_code == 200:
                    data = response.json()
                    if (data.get('inventory_service') == 'okay' and 
                        data.get('notification_service') == 'okay' and 
                        data.get('order_service') == 'okay'):
                        success_count += 1
                    else:
                        failures.append(f"Request {i+1}: Services not all okay")
                else:
                    failures.append(f"Request {i+1}: Status {response.status_code}")
                    
                print(f"  Request {i+1}/{NUM_REQUESTS}: {latency_ms:.2f}ms - Status: {response.status_code}")
                
            except Exception as e:
                failures.append(f"Request {i+1}: {str(e)}")
                print(f"  Request {i+1}/{NUM_REQUESTS}: FAILED - {str(e)}")
        
        # Calculate metrics
        metrics = self.calculate_metrics(latencies)
        
        # Print formatted table
        self.print_metrics_table("No Delay, No Fault", metrics, params)
        
        # Assertions
        assert len(latencies) == NUM_REQUESTS, f"Expected {NUM_REQUESTS} responses, got {len(latencies)}"
        assert success_count == NUM_REQUESTS, f"Expected {NUM_REQUESTS} successes, got {success_count}. Failures: {failures}"
        assert metrics['mean'] < 5000, f"Mean latency {metrics['mean']:.2f}ms exceeds threshold"
        
        # Store metrics for later export
        self.test_1_metrics = metrics
    
    def test_order_with_delay_no_fault(self):
        """
        Test Case 2: POST /order with delay=2, fault=false
        Expected: Successful order but with added delay from inventory service
        """
        params = {
            'delay': 2,
            'fault': 'false'
        }
        
        latencies = []
        success_count = 0
        failures = []
        
        print(f"\n[Test 2] Sending {NUM_REQUESTS} requests with delay=2, fault=false...")
        
        for i in range(NUM_REQUESTS):
            try:
                start_time = time.time()
                
                response = requests.post(
                    f'{ORDER_SERVICE_URL}/order',
                    params=params,
                    timeout=10
                )
                
                end_time = time.time()
                latency_ms = (end_time - start_time) * 1000
                latencies.append(latency_ms)
                
                # Validate response
                if response.status_code == 200:
                    data = response.json()
                    if (data.get('inventory_service') == 'okay' and 
                        data.get('notification_service') == 'okay' and 
                        data.get('order_service') == 'okay'):
                        success_count += 1
                    else:
                        failures.append(f"Request {i+1}: Services not all okay")
                else:
                    failures.append(f"Request {i+1}: Status {response.status_code}")
                    
                print(f"  Request {i+1}/{NUM_REQUESTS}: {latency_ms:.2f}ms - Status: {response.status_code}")
                
            except Exception as e:
                failures.append(f"Request {i+1}: {str(e)}")
                print(f"  Request {i+1}/{NUM_REQUESTS}: FAILED - {str(e)}")
        
        # Calculate metrics
        metrics = self.calculate_metrics(latencies)
        
        # Print formatted table
        self.print_metrics_table("2-Second Delay, No Fault", metrics, params)
        
        # Assertions
        assert len(latencies) == NUM_REQUESTS, f"Expected {NUM_REQUESTS} responses, got {len(latencies)}"
        assert success_count == NUM_REQUESTS, f"Expected {NUM_REQUESTS} successes, got {success_count}. Failures: {failures}"
        assert metrics['mean'] >= 2000, f"Mean latency {metrics['mean']:.2f}ms should be at least 2000ms"
        assert metrics['mean'] < 7000, f"Mean latency {metrics['mean']:.2f}ms exceeds threshold"
        
    
    def test_order_with_longer_delay_no_fault(self):
        """
        Test Case 3: POST /order with delay=4, fault=false
        Expected: Successful order but with longer delay from inventory service
        """
        params = {
            'delay': 4,
            'fault': 'false'
        }
        
        latencies = []
        success_count = 0
        failures = []
        
        print(f"\n[Test 3] Sending {NUM_REQUESTS} requests with delay=4, fault=false...")
        
        for i in range(NUM_REQUESTS):
            try:
                start_time = time.time()
                
                response = requests.post(
                    f'{ORDER_SERVICE_URL}/order',
                    params=params,
                    timeout=15
                )
                
                end_time = time.time()
                latency_ms = (end_time - start_time) * 1000
                latencies.append(latency_ms)
                
                # Validate response
                if response.status_code == 200:
                    data = response.json()
                    if (data.get('inventory_service') == 'okay' and 
                        data.get('notification_service') == 'okay' and 
                        data.get('order_service') == 'okay'):
                        success_count += 1
                    else:
                        failures.append(f"Request {i+1}: Services not all okay")
                else:
                    failures.append(f"Request {i+1}: Status {response.status_code}")
                    
                print(f"  Request {i+1}/{NUM_REQUESTS}: {latency_ms:.2f}ms - Status: {response.status_code}")
                
            except Exception as e:
                failures.append(f"Request {i+1}: {str(e)}")
                print(f"  Request {i+1}/{NUM_REQUESTS}: FAILED - {str(e)}")
        
        # Calculate metrics
        metrics = self.calculate_metrics(latencies)
        self.print_metrics_table("2-Second Delay, No Fault", metrics, params)


        



if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, '-v', '-s'])
