from flask import Flask, jsonify, request
import logging
import os
from services.wildberries.wb_api import WildberriesAPI
from services.ozon.ozon_api import OzonAPI
from utils.db_utils import PostgresConnector
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/api/marketplace/summary', methods=['GET'])
def get_marketplace_summary():
    """Get summary of marketplace data"""
    try:
        # Connect to database
        db = PostgresConnector()
        if not db.connect():
            return jsonify({"error": "Could not connect to database"}), 500
        
        # Get latest sales data
        query = """
        WITH wb_sales AS (
            SELECT 
                DATE(date) as sale_date,
                SUM(finishedPrice) as revenue,
                COUNT(*) as orders
            FROM stg.wb_sales
            WHERE DATE(date) >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY DATE(date)
        ),
        ozon_sales AS (
            SELECT 
                DATE(created_at) as sale_date,
                SUM(price) as revenue,
                COUNT(*) as orders
            FROM stg.ozon_orders
            WHERE DATE(created_at) >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY DATE(created_at)
        )
        SELECT 
            COALESCE(wb.sale_date, oz.sale_date) as date,
            COALESCE(wb.revenue, 0) as wb_revenue,
            COALESCE(wb.orders, 0) as wb_orders,
            COALESCE(oz.revenue, 0) as ozon_revenue,
            COALESCE(oz.orders, 0) as ozon_orders
        FROM wb_sales wb
        FULL OUTER JOIN ozon_sales oz ON wb.sale_date = oz.sale_date
        ORDER BY date DESC
        LIMIT 30
        """
        
        results = db.fetch_all(query)
        db.disconnect()
        
        # Format results
        formatted_data = []
        for row in results:
            formatted_data.append({
                "date": row[0].strftime("%Y-%m-%d") if row[0] else None,
                "wildberries": {
                    "revenue": float(row[1]) if row[1] else 0,
                    "orders": int(row[2]) if row[2] else 0
                },
                "ozon": {
                    "revenue": float(row[3]) if row[3] else 0,
                    "orders": int(row[4]) if row[4] else 0
                }
            })
        
        return jsonify({
            "success": True,
            "data": formatted_data
        })
        
    except Exception as e:
        logger.error(f"Error retrieving marketplace summary: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/marketplace/stocks', methods=['GET'])
def get_marketplace_stocks():
    """Get current stock information"""
    try:
        # Connect to database
        db = PostgresConnector()
        if not db.connect():
            return jsonify({"error": "Could not connect to database"}), 500
        
        # Get latest stock data
        query = """
        WITH wb_stock AS (
            SELECT 
                barcode,
                SUM(quantity) as quantity
            FROM stg.wb_stocks
            WHERE DATE(lastChangeDate) = (SELECT MAX(DATE(lastChangeDate)) FROM stg.wb_stocks)
            GROUP BY barcode
        ),
        ozon_stock AS (
            SELECT 
                sku,
                SUM(present) as quantity
            FROM stg.ozon_stocks
            WHERE DATE(updated_at) = (SELECT MAX(DATE(updated_at)) FROM stg.ozon_stocks)
            GROUP BY sku
        )
        SELECT 
            'wildberries' as marketplace,
            barcode as product_id,
            quantity
        FROM wb_stock
        UNION ALL
        SELECT 
            'ozon' as marketplace,
            sku as product_id,
            quantity
        FROM ozon_stock
        """
        
        results = db.fetch_all(query)
        db.disconnect()
        
        # Format results
        formatted_data = {
            "wildberries": [],
            "ozon": []
        }
        
        for row in results:
            marketplace = row[0]
            product_data = {
                "product_id": row[1],
                "quantity": int(row[2]) if row[2] else 0
            }
            formatted_data[marketplace].append(product_data)
        
        return jsonify({
            "success": True,
            "data": formatted_data
        })
        
    except Exception as e:
        logger.error(f"Error retrieving stock information: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)