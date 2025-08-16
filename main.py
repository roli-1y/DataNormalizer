import json
import logging
from datetime import datetime
from statistics import mean
from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from bson import ObjectId
from bson.json_util import dumps

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger=logging.getLogger(__name__)

app=Flask(__name__)
CORS(app, resources={
    r"/stats": {"origins": "http://localhost:5173"},
    r"/machines": {"origins": "http://localhost:5173"}
})

# MongoDB Configuration
MONGO_URI="mongodb://localhost:27017/"
DB_NAME="machines"
COLLECTION_NAME="machine_data"

# Connect to MongoDB
try:
    client=MongoClient(MONGO_URI)
    db=client[DB_NAME]
    collection=db[COLLECTION_NAME]
    logger.info(f"Connected to MongoDB: {DB_NAME}.{COLLECTION_NAME}")

    # Create indexes for better performance
    collection.create_index([("os", 1)])
    collection.create_index([("cpu", 1)])
    collection.create_index([("timestamp", -1)])
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {str(e)}")
    raise

# Configuration
MAPPINGS_FILE='mappings.json'
MAX_LIMIT=100  # Maximum records per page

# Load mappings from JSON file
try:
    with open(MAPPINGS_FILE, 'r') as f:
        MAPPINGS=json.load(f)
except FileNotFoundError:
    logger.error(f"Could not find {MAPPINGS_FILE}. Starting with empty mappings.")
    MAPPINGS={}
except json.JSONDecodeError:
    logger.error(f"Invalid JSON in {MAPPINGS_FILE}. Starting with empty mappings.")
    MAPPINGS={}

# Helper Functions
def safe_lambda(data, lambda_str):
    """Safely evaluate lambda expressions for memory conversion."""
    if lambda_str == "lambda data: int(data['RAM'].split()[0])":
        try:
            return int(data['RAM'].split()[0])
        except (KeyError, IndexError, ValueError) as e:
            raise ValueError(f"Failed to parse RAM: {str(e)}")
    elif lambda_str == "lambda data: int(data['mem']) / 1024":
        try:
            return int(data['mem']) / 1024
        except (KeyError, ValueError) as e:
            raise ValueError(f"Failed to convert mem to GB: {str(e)}")
    else:
        raise ValueError(f"Unsupported lambda expression: {lambda_str}")

def get_field(data, mapping):
    """Get field value based on mapping configuration."""
    if not mapping:
        return None

    if isinstance(mapping, list):
        # Try case-insensitive matching for list of possible field names
        for key in mapping:
            for k in data:
                if k.lower() == key.lower():
                    return data[k]
        return None
    elif isinstance(mapping, str):
        if mapping.startswith('lambda '):
            try:
                return safe_lambda(data, mapping)
            except Exception as e:
                raise ValueError(f"Lambda execution error: {str(e)}")
        else:
            # Case-insensitive key lookup
            for k in data:
                if k.lower() == mapping.lower():
                    return data[k]
            return data.get(mapping)
    else:
        raise ValueError("Invalid mapping type")

def insert_record(record):
    """Insert a record into MongoDB."""
    try:
        result=collection.insert_one(record)
        logger.info(f"Inserted record with ID: {result.inserted_id}")
        return str(result.inserted_id)
    except Exception as e:
        logger.error(f"Failed to insert record: {str(e)}")
        raise

# API Endpoints
@app.route('/machines', methods=['POST'])
def post_machines():
    """Endpoint to add new machine data."""
    source=request.headers.get('X-Source')
    if not source or source not in MAPPINGS:
        return jsonify({
            "status": "error",
            "message": f"Invalid or missing X-Source header. Valid sources: {list(MAPPINGS.keys())}"
        }), 400

    try:
        data=request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": "Invalid JSON payload"}), 400

    items=[data] if isinstance(data, dict) else data
    inserted=0
    errors=[]

    for item in items:
        if not isinstance(item, dict):
            errors.append("Item is not a valid JSON object")
            continue

        try:
            maps=MAPPINGS[source]
            record={
                "os": get_field(item, maps.get("os")) or "",
                "cpu": get_field(item, maps.get("cpu")) or "",
                "memory_gb": get_field(item, maps.get("memory_gb"))
            }

            # Validate required fields
            if not record['os'] or not record['cpu']:
                raise ValueError("Missing required fields (os or cpu)")

            # Convert memory to float if present
            if record['memory_gb'] is not None:
                try:
                    record['memory_gb']=float(record['memory_gb'])
                except ValueError:
                    raise ValueError("memory_gb must be a number")

            insert_record(record)
            inserted += 1
        except Exception as e:
            errors.append(str(e))
            logger.error(f"Error processing item: {str(e)}")

    return jsonify({
        "status": "success",
        "inserted": inserted,
        "errors": errors,
        "message": f"Inserted {inserted} records"
    })

@app.route('/machines', methods=['GET'])
def get_machines():
    """Endpoint to retrieve machine data with pagination and filtering."""
    try:
        # Pagination parameters - support both limit/offset and page/per_page styles
        limit=request.args.get('limit', default=10, type=int)
        offset=request.args.get('offset', default=0, type=int)

        # Convert to page/per_page style if needed
        if limit and offset:
            per_page=min(limit, MAX_LIMIT)
            page=(offset // per_page) + 1
        else:
            # Fallback to page/per_page if limit/offset not provided
            page=request.args.get('page', 1, type=int)
            per_page=min(request.args.get('per_page', 10, type=int), MAX_LIMIT)

        # Filter parameters
        os_filter=request.args.get('os')
        cpu_filter=request.args.get('cpu')

        # Build query
        query={}
        if os_filter:
            query['os']=os_filter
        if cpu_filter:
            query['cpu']=cpu_filter

        # Get total count
        total=collection.count_documents(query)

        # Get paginated results - exclude _id, source, and timestamp
        projection={
            '_id': 0,
            'source': 0,
            'timestamp': 0
        }
        cursor=collection.find(query, projection).skip(offset).limit(limit)
        results=list(cursor)

        return jsonify(results)
    except Exception as e:
        logger.error(f"Database query failed: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Failed to retrieve data"
        }), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    """Endpoint to get statistics about machine data."""
    try:
        # Total records
        total=collection.count_documents({})

        # OS distribution (handle null/missing values)
        os_distribution=list(collection.aggregate([
            {"$match": {"os": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$os", "count": {"$sum": 1}}}
        ]))

        # CPU distribution (handle null/missing values)
        cpu_distribution=list(collection.aggregate([
            {"$match": {"cpu": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$cpu", "count": {"$sum": 1}}}
        ]))

        # Memory statistics (only for numeric values)
        memory_stats=list(collection.aggregate([
            {"$match": {
                "memory_gb": {
                    "$exists": True,
                    "$ne": None,
                    "$type": ["number", "int", "double", "decimal"]
                }
            }},
            {"$group": {
                "_id": None,
                "avg": {"$avg": "$memory_gb"},
                "min": {"$min": "$memory_gb"},
                "max": {"$max": "$memory_gb"},
                "count": {"$sum": 1}
            }}
        ]))

        # Prepare response
        response={
            "total_records": total,
            "os_distribution": {item["_id"]: item["count"] for item in os_distribution},
            "cpu_distribution": {item["_id"]: item["count"] for item in cpu_distribution},
        }

        # Add memory stats if available
        if memory_stats and memory_stats[0]['count'] > 0:
            response.update({
                "memory_stats": {
                    "average_gb": round(memory_stats[0]['avg'], 2),
                    "minimum_gb": memory_stats[0]['min'],
                    "maximum_gb": memory_stats[0]['max'],
                    "count": memory_stats[0]['count']
                }
            })
        else:
            response["memory_stats"]={
                "message": "No valid memory data available",
                "count": 0
            }

        return jsonify(response)

    except Exception as e:
        logger.error(f"Stats calculation failed: {str(e)}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": f"Failed to calculate statistics: {str(e)}"
        }), 500

@app.route('/machines/sources', methods=['GET'])
def get_sources():
    """Endpoint to get list of available data sources."""
    try:
        sources=list(collection.distinct("source"))
        return jsonify({
            "sources": sources,
            "mappings": list(MAPPINGS.keys())
        })
    except Exception as e:
        logger.error(f"Failed to get sources: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Failed to retrieve sources"
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)