# data-generator/producer.py
"""
Real-time data generator that produces events to Kafka
Simulates e-commerce/retail transactions for Central Co-op scenario
"""

import json
import random
import time
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataGenerator:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = 'retail-transactions'
        self.producer = None
        self.init_producer()
        
        # Sample data for realistic events
        self.stores = [
            'London-Central', 'Manchester-North', 'Birmingham-East', 
            'Leeds-West', 'Sheffield-South', 'Newcastle-Central',
            'Liverpool-Docks', 'Bristol-Harbor', 'Edinburgh-Royal',
            'Glasgow-Central'
        ]
        
        self.products = [
            {'id': 'P001', 'name': 'Organic Milk', 'category': 'Dairy', 'base_price': 2.49},
            {'id': 'P002', 'name': 'Whole Wheat Bread', 'category': 'Bakery', 'base_price': 1.89},
            {'id': 'P003', 'name': 'Free Range Eggs', 'category': 'Dairy', 'base_price': 3.29},
            {'id': 'P004', 'name': 'Chicken Breast', 'category': 'Meat', 'base_price': 7.99},
            {'id': 'P005', 'name': 'Bananas', 'category': 'Produce', 'base_price': 1.29},
            {'id': 'P006', 'name': 'Tomatoes', 'category': 'Produce', 'base_price': 2.49},
            {'id': 'P007', 'name': 'Coffee Beans', 'category': 'Beverages', 'base_price': 8.99},
            {'id': 'P008', 'name': 'Orange Juice', 'category': 'Beverages', 'base_price': 3.49},
            {'id': 'P009', 'name': 'Pasta', 'category': 'Grocery', 'base_price': 1.59},
            {'id': 'P010', 'name': 'Rice', 'category': 'Grocery', 'base_price': 2.99},
        ]
        
        self.payment_methods = ['card', 'cash', 'mobile', 'online']
        self.customer_types = ['member', 'guest', 'premium', 'staff']
        
    def init_producer(self):
        """Initialize Kafka producer with retry logic"""
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda v: v.encode('utf-8') if v else None
                )
                logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
                return
            except NoBrokersAvailable:
                retry_count += 1
                logger.warning(f"Kafka not available, retry {retry_count}/{max_retries}")
                time.sleep(5)
        
        raise Exception("Could not connect to Kafka after maximum retries")
    
    def generate_transaction(self):
        """Generate a realistic retail transaction event"""
        
        # Select random products for the transaction
        num_items = random.randint(1, 8)
        selected_products = random.sample(self.products, num_items)
        
        items = []
        total_amount = 0
        
        for product in selected_products:
            quantity = random.randint(1, 5)
            price = product['base_price'] * (1 + random.uniform(-0.1, 0.1))  # ±10% price variation
            item_total = price * quantity
            
            items.append({
                'product_id': product['id'],
                'product_name': product['name'],
                'category': product['category'],
                'quantity': quantity,
                'unit_price': round(price, 2),
                'total': round(item_total, 2)
            })
            total_amount += item_total
        
        transaction = {
            'transaction_id': f"TXN-{datetime.now().strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}",
            'timestamp': datetime.now().isoformat(),
            'store_id': random.choice(self.stores),
            'customer_id': f"CUST-{random.randint(10000, 99999)}",
            'customer_type': random.choice(self.customer_types),
            'items': items,
            'total_amount': round(total_amount, 2),
            'payment_method': random.choice(self.payment_methods),
            'discount_applied': round(random.uniform(0, total_amount * 0.2), 2),
            'final_amount': round(total_amount * (1 - random.uniform(0, 0.2)), 2)
        }
        
        return transaction
    
    def generate_inventory_event(self):
        """Generate inventory update events"""
        product = random.choice(self.products)
        
        event = {
            'event_id': f"INV-{datetime.now().strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}",
            'timestamp': datetime.now().isoformat(),
            'store_id': random.choice(self.stores),
            'product_id': product['id'],
            'product_name': product['name'],
            'event_type': random.choice(['restock', 'sale', 'damage', 'return']),
            'quantity_change': random.randint(-50, 200),
            'current_stock': random.randint(0, 500),
            'reorder_level': 50,
            'alert': random.choice([True, False])
        }
        
        return event
    
    def generate_customer_event(self):
        """Generate customer behavior events"""
        event = {
            'event_id': f"CUST-{datetime.now().strftime('%Y%m%d%H%M%S')}-{random.randint(1000, 9999)}",
            'timestamp': datetime.now().isoformat(),
            'customer_id': f"CUST-{random.randint(10000, 99999)}",
            'event_type': random.choice(['login', 'search', 'add_to_cart', 'checkout', 'abandon_cart']),
            'store_id': random.choice(self.stores),
            'session_duration': random.randint(30, 1800),  # seconds
            'page_views': random.randint(1, 50),
            'device': random.choice(['mobile', 'desktop', 'tablet']),
            'channel': random.choice(['app', 'web', 'in-store'])
        }
        
        return event
    
    def run(self):
        """Main loop to generate and send events"""
        logger.info("Starting data generation...")
        interval = float(os.getenv('GENERATION_INTERVAL', '1'))
        
        event_count = 0
        
        try:
            while True:
                # Generate different types of events with different frequencies
                event_type = random.choices(
                    ['transaction', 'inventory', 'customer'],
                    weights=[60, 20, 20]  # 60% transactions, 20% inventory, 20% customer
                )[0]
                
                if event_type == 'transaction':
                    event = self.generate_transaction()
                    topic = 'retail-transactions'
                elif event_type == 'inventory':
                    event = self.generate_inventory_event()
                    topic = 'inventory-updates'
                else:
                    event = self.generate_customer_event()
                    topic = 'customer-events'
                
                # Send to Kafka
                future = self.producer.send(
                    topic,
                    key=event.get('transaction_id') or event.get('event_id'),
                    value=event
                )
                
                # Wait for confirmation
                record_metadata = future.get(timeout=10)
                event_count += 1
                
                logger.info(f"Sent {event_type} event #{event_count} to {topic} partition {record_metadata.partition}")
                
                # Log sample event every 10 events
                if event_count % 10 == 0:
                    logger.info(f"Sample event: {json.dumps(event, indent=2)}")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Shutting down data generator...")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info(f"Generated {event_count} events total")

if __name__ == "__main__":
    generator = DataGenerator()
    generator.run()