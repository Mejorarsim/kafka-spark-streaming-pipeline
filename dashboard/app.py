# dashboard/app.py
"""
Real-time dashboard for monitoring data pipeline
"""

from flask import Flask, render_template, jsonify, Response
import psycopg2
from psycopg2.extras import RealDictCursor
import redis
import json
import os
from datetime import datetime, timedelta
from decimal import Decimal
import csv
import io

app = Flask(__name__)

# Configuration
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'database': os.getenv('POSTGRES_DB', 'warehouse'),
    'user': os.getenv('POSTGRES_USER', 'admin'),
    'password': os.getenv('POSTGRES_PASSWORD', 'admin123'),
    'port': 5432
}

REDIS_CONFIG = {
    'host': os.getenv('REDIS_HOST', 'redis'),
    'port': 6379,
    'db': 0,
    'decode_responses': True
}

def get_postgres_connection():
    """Create PostgreSQL connection"""
    return psycopg2.connect(**POSTGRES_CONFIG)

def get_redis_client():
    """Create Redis client"""
    return redis.Redis(**REDIS_CONFIG)

@app.route('/')
def index():
    """Render main dashboard page"""
    return render_template('index.html')

@app.route('/api/metrics')
def get_metrics():
    """Get real-time metrics from PostgreSQL"""
    try:
        conn = get_postgres_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get summary metrics from transactions_summary (actual table)
        cur.execute("""
            SELECT 
                COALESCE(SUM(total_transactions), 0) as total_transactions,
                COALESCE(SUM(total_revenue), 0) as total_revenue,
                COALESCE(AVG(avg_transaction_value), 0) as avg_transaction_value,
                COALESCE(SUM(unique_customers), 0) as total_customers
            FROM transactions_summary
            WHERE window_start >= NOW() - INTERVAL '1 hour'
        """)
        summary = cur.fetchone()
        
        # Get store-level metrics
        cur.execute("""
            SELECT 
                store_id,
                SUM(total_transactions) as transactions,
                SUM(total_revenue) as revenue,
                AVG(avg_transaction_value) as avg_transaction
            FROM transactions_summary
            WHERE window_start >= NOW() - INTERVAL '1 hour'
            GROUP BY store_id
            ORDER BY revenue DESC
            LIMIT 10
        """)
        stores = cur.fetchall()
        
        # Get top products
        cur.execute("""
            SELECT 
                product_name,
                category,
                SUM(quantity_sold) as quantity,
                SUM(revenue) as revenue
            FROM product_performance
            WHERE window_start >= NOW() - INTERVAL '1 hour'
            GROUP BY product_name, category
            ORDER BY revenue DESC
            LIMIT 10
        """)
        products = cur.fetchall()
        
        # Get recent transactions count for activity indicator
        cur.execute("""
            SELECT COUNT(*) as recent_count
            FROM transactions_summary
            WHERE window_start >= NOW() - INTERVAL '5 minutes'
        """)
        recent_activity = cur.fetchone()
        
        cur.close()
        conn.close()
        
        # Format the response
        response = {
            'summary': {
                'total_transactions': int(summary['total_transactions']) if summary else 0,
                'total_revenue': float(summary['total_revenue']) if summary else 0,
                'avg_transaction_value': float(summary['avg_transaction_value']) if summary else 0,
                'total_customers': int(summary['total_customers']) if summary else 0
            },
            'stores': [
                {
                    'store_id': store['store_id'],
                    'transactions': int(store['transactions']),
                    'revenue': float(store['revenue']),
                    'avg_transaction': float(store['avg_transaction'])
                }
                for store in stores
            ] if stores else [],
            'products': [
                {
                    'product_name': product['product_name'],
                    'category': product['category'],
                    'quantity': str(product['quantity']),
                    'revenue': str(product['revenue'])
                }
                for product in products
            ] if products else [],
            'recent_activity': recent_activity['recent_count'] if recent_activity else 0
        }
        
        # Cache in Redis for performance
        redis_client = get_redis_client()
        redis_client.setex('dashboard_metrics', 30, json.dumps(response))
        
        return jsonify(response)
        
    except Exception as e:
        app.logger.error(f"Error fetching metrics: {e}")
        # Try to return cached data from Redis
        try:
            redis_client = get_redis_client()
            cached = redis_client.get('dashboard_metrics')
            if cached:
                return json.loads(cached)
        except:
            pass
        
        return jsonify({
            'summary': {
                'total_transactions': 0,
                'total_revenue': 0,
                'avg_transaction_value': 0,
                'total_customers': 0
            },
            'stores': [],
            'products': [],
            'error': str(e)
        })

@app.route('/api/health')
def health_check():
    """Check health status of connected services"""
    health_status = {
        'timestamp': datetime.now().isoformat(),
        'services': {}
    }
    
    # Check PostgreSQL
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute('SELECT 1')
        cur.close()
        conn.close()
        health_status['services']['postgresql'] = True
    except:
        health_status['services']['postgresql'] = False
    
    # Check Redis
    try:
        redis_client = get_redis_client()
        redis_client.ping()
        health_status['services']['redis'] = True
    except:
        health_status['services']['redis'] = False
    
    # Check if data is flowing (recent transactions)
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) as count 
            FROM transactions_summary 
            WHERE window_start >= NOW() - INTERVAL '5 minutes'
        """)
        result = cur.fetchone()
        health_status['services']['data_flow'] = result[0] > 0
        cur.close()
        conn.close()
    except:
        health_status['services']['data_flow'] = False
    
    return jsonify(health_status)

@app.route('/api/stream-stats')
def stream_stats():
    """Get streaming statistics"""
    try:
        conn = get_postgres_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get processing stats
        cur.execute("""
            SELECT 
                COUNT(DISTINCT window_start) as windows_processed,
                MIN(window_start) as earliest_window,
                MAX(window_start) as latest_window,
                COUNT(DISTINCT store_id) as active_stores
            FROM transactions_summary
            WHERE window_start >= NOW() - INTERVAL '1 hour'
        """)
        stats = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return jsonify(stats)
        
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/api/export')
@app.route('/api/export/<format>')
def export_data(format='json'):
    """Export dashboard data as JSON or CSV"""
    try:
        conn = get_postgres_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get all metrics for export
        cur.execute("""
            SELECT * FROM transactions_summary 
            WHERE window_start >= NOW() - INTERVAL '24 hours'
            ORDER BY window_start DESC
        """)
        transactions = cur.fetchall()
        
        cur.execute("""
            SELECT * FROM product_performance 
            WHERE window_start >= NOW() - INTERVAL '24 hours'
            ORDER BY revenue DESC
        """)
        products = cur.fetchall()
        
        cur.close()
        conn.close()
        
        # Convert Decimal and datetime objects
        for t in transactions:
            for key, value in t.items():
                if isinstance(value, datetime):
                    t[key] = str(value)
                elif isinstance(value, Decimal):
                    t[key] = float(value)
        
        for p in products:
            for key, value in p.items():
                if isinstance(value, datetime):
                    p[key] = str(value)
                elif isinstance(value, Decimal):
                    p[key] = float(value)
        
        # Export based on format
        if format == 'csv':
            output = io.StringIO()
            
            # Write transactions to CSV
            if transactions:
                writer = csv.DictWriter(output, fieldnames=transactions[0].keys())
                writer.writeheader()
                writer.writerows(transactions)
            
            return Response(
                output.getvalue(),
                mimetype='text/csv',
                headers={'Content-Disposition': 'attachment;filename=transactions_export.csv'}
            )
            
        else:  # Default to JSON
            export_data = {
                'export_timestamp': datetime.now().isoformat(),
                'transactions_summary': transactions,
                'product_performance': products,
                'total_records': len(transactions) + len(products)
            }
            
            return Response(
                json.dumps(export_data, indent=2),
                mimetype='application/json',
                headers={'Content-Disposition': 'attachment;filename=pipeline_export.json'}
            )
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)