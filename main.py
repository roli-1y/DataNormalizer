import json
import logging
from datetime import datetime
from statistics import mean
from flask import Flask, request, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from bson import ObjectId
from bson.json_util import dumps
import os
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app, resources={
    r"/stats": {"origins": "http://localhost:5173"},
    r"/machines": {"origins": "http://localhost:5173"},
    r"/mappings": {"origins": "http://localhost:5173"}
})

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "machines"
COLLECTION_NAME = "machine_data"

# Connect to MongoDB
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    logger.info(f"Connected to MongoDB: {DB_NAME}.{COLLECTION_NAME}")

    # Create indexes for better performance
    collection.create_index([("os", 1)])
    collection.create_index([("cpu", 1)])
    collection.create_index([("timestamp", -1)])
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {str(e)}")
    raise

# Configuration
MAPPINGS_FILE = 'mappings.json'
MAX_LIMIT = 100  # Maximum records per page
MAPPINGS_LAST_MODIFIED = 0
MAPPINGS = {}


def load_mappings(force=False):
    """Load mappings from JSON file with caching and auto-reload."""
    global MAPPINGS, MAPPINGS_LAST_MODIFIED

    try:
        if not os.path.exists(MAPPINGS_FILE):
            if force or not MAPPINGS:
                logger.warning(f"Could not find {MAPPINGS_FILE}. Using empty mappings.")
                MAPPINGS = {}
                MAPPINGS_LAST_MODIFIED = 0
            return MAPPINGS

        current_mtime = os.path.getmtime(MAPPINGS_FILE)

        # Only reload if file has changed or forced
        if force or current_mtime > MAPPINGS_LAST_MODIFIED:
            try:
                with open(MAPPINGS_FILE, 'r') as f:
                    new_mappings = json.load(f)

                MAPPINGS = new_mappings
                MAPPINGS_LAST_MODIFIED = current_mtime
                logger.info(f"Loaded mappings from {MAPPINGS_FILE}. Sources: {list(MAPPINGS.keys())}")

            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in {MAPPINGS_FILE}: {str(e)}")
                if not MAPPINGS:  # Only fail if we don't have existing mappings
                    MAPPINGS = {}
            except Exception as e:
                logger.error(f"Error loading mappings: {str(e)}")
                if not MAPPINGS:  # Only fail if we don't have existing mappings
                    MAPPINGS = {}

    except Exception as e:
        logger.error(f"Unexpected error loading mappings: {str(e)}")
        if not MAPPINGS:  # Ensure we always have a mappings dict
            MAPPINGS = {}

    return MAPPINGS


# Initial load of mappings
load_mappings(force=True)


# Helper Functions
def safe_lambda(data, lambda_str):
    """Safely evaluate lambda expressions for memory conversion with support for multiple formats."""
    try:
        if lambda_str == "lambda data: int(data['RAM'].split()[0])":
            # Try standard RAM format first
            if 'RAM' in data:
                if isinstance(data['RAM'], (int, float)):
                    return float(data['RAM'])
                return float(data['RAM'].split()[0])
            # Fallback to other possible field names
            for field in ['memory', 'mem', 'ram', 'Memory', 'memory_gb']:
                if field in data:
                    if isinstance(data[field], (int, float)):
                        return float(data[field])
                    return float(data[field].split()[0])
            raise KeyError("No valid memory field found")

        elif lambda_str == "lambda data: int(data['mem']) / 1024":
            # Handle memory in KB converting to GB
            if 'mem' in data:
                if isinstance(data['mem'], (int, float)):
                    return float(data['mem']) / 1024
                return float(data['mem']) / 1024
            # Fallback to memory_gb if available
            if 'memory_gb' in data:
                if isinstance(data['memory_gb'], (int, float)):
                    return float(data['memory_gb'])
                return float(data['memory_gb'])

        elif lambda_str == "lambda data: int(data['memory_gb'])":
            # Direct memory_gb field access
            if 'memory_gb' in data:
                if isinstance(data['memory_gb'], (int, float)):
                    return float(data['memory_gb'])
                return float(data['memory_gb'])
            raise KeyError("No valid memory_gb field found")

        else:
            raise ValueError(f"Unsupported lambda expression: {lambda_str}")
    except (KeyError, IndexError, ValueError, AttributeError) as e:
        logger.error(f"Failed to parse memory: {str(e)}, data: {data}")
        raise ValueError(f"Failed to parse memory: {str(e)}")


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


def normalize_cpu(cpu_value):
    """Normalize CPU field to handle lists or strings."""
    if isinstance(cpu_value, list):
        return ", ".join(str(cpu) for cpu in cpu_value)
    return str(cpu_value)


def insert_record(record):
    """Insert a record into MongoDB."""
    try:
        result = collection.insert_one(record)
        logger.info(f"Inserted record with ID: {result.inserted_id}")
        return str(result.inserted_id)
    except Exception as e:
        logger.error(f"Failed to insert record: {str(e)}")
        raise


# API Endpoints
@app.route('/machines', methods=['POST'])
def post_machines():
    """Endpoint to add new machine data."""
    # Reload mappings to ensure we have the latest version
    current_mappings = load_mappings()

    source = request.headers.get('X-Source')
    if not source or source not in current_mappings:
        return jsonify({
            "status": "error",
            "message": f"Invalid or missing X-Source header. Valid sources: {list(current_mappings.keys())}"
        }), 400

    try:
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No data provided"}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": "Invalid JSON payload"}), 400

    items = [data] if isinstance(data, dict) else data
    inserted = 0
    errors = []

    for item in items:
        if not isinstance(item, dict):
            errors.append("Item is not a valid JSON object")
            continue

        try:
            maps = current_mappings[source]
            cpu_value = get_field(item, maps.get("cpu")) or ""
            memory_value = get_field(item, maps.get("memory_gb"))
            if memory_value is None:
                logger.warning(f"Skipping item due to missing memory_gb: {item}")
                errors.append("Missing memory_gb field")
                continue

            record = {
                "os": get_field(item, maps.get("os")) or "",
                "cpu": normalize_cpu(cpu_value),
                "memory_gb": float(memory_value),
            }

            # Validate required fields
            if not record['os'] or not record['cpu']:
                logger.error(
                    f"Validation failed for item: {item}, mappings: {maps}, extracted os: {record['os']}, cpu: {record['cpu']}")
                errors.append("Missing required fields (os or cpu)")
                continue

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
        # Reload mappings to ensure we have the latest version
        load_mappings()

        # Pagination parameters with safety limits
        DEFAULT_LIMIT = 100
        MAX_LIMIT = 1000
        ABSOLUTE_MAX_LIMIT = 5000

        # Get and validate limit
        requested_limit = request.args.get('limit', default=DEFAULT_LIMIT, type=int)
        limit = min(max(1, requested_limit), MAX_LIMIT)

        # Enforce absolute maximum for privileged users if needed
        if requested_limit > MAX_LIMIT:
            if not request.headers.get('X-High-Limit-Access'):
                limit = MAX_LIMIT
            else:
                limit = min(requested_limit, ABSOLUTE_MAX_LIMIT)
                logger.warning(f"High limit request: {requested_limit}, granted: {limit}")

        # Get and validate offset
        offset = max(0, request.args.get('offset', default=0, type=int))

        # Filter parameters
        os_filter = request.args.get('os')
        cpu_filter = request.args.get('cpu')
        source_filter = request.args.get('source')

        # Build query
        query = {}
        if os_filter:
            query['os'] = {"$regex": f"^{os_filter}$", "$options": "i"}
        if cpu_filter:
            query['cpu'] = {"$regex": f".*{cpu_filter}.*", "$options": "i"}
        if source_filter:
            query['source'] = {"$regex": f"^{source_filter}$", "$options": "i"}

        # Get total count (with same filters)
        total = collection.count_documents(query)

        # Validate offset isn't beyond total
        if offset > total:
            offset = max(0, total - limit)

        # Get paginated results
        projection = {'_id': 0}
        cursor = collection.find(query, projection).skip(offset).limit(limit)

        # Add performance hint for large limits
        if limit > 500:
            cursor = cursor.batch_size(100)

        results = list(cursor)

        return jsonify({
            "data": results,
            "pagination": {
                "total": total,
                "returned": len(results),
                "has_more": (offset + limit) < total
            }
        })
    except Exception as e:
        logger.error(f"Database query failed: {str(e)}", exc_info=True)
        return jsonify({
            "status": "error",
            "message": "Failed to retrieve data",
            "error": str(e)
        }), 500


@app.route('/stats', methods=['GET'])
def get_stats():
    """Endpoint to get statistics about machine data."""
    try:
        # Reload mappings to ensure we have the latest version
        load_mappings()

        # Total records
        total = collection.count_documents({})

        # OS distribution
        os_distribution = list(collection.aggregate([
            {"$match": {"os": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$os", "count": {"$sum": 1}}}
        ]))

        # CPU distribution
        cpu_distribution = list(collection.aggregate([
            {"$match": {"cpu": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$cpu", "count": {"$sum": 1}}}
        ]))

        # Source distribution
        source_distribution = list(collection.aggregate([
            {"$match": {"source": {"$exists": True, "$ne": None}}},
            {"$group": {"_id": "$source", "count": {"$sum": 1}}}
        ]))

        # Memory statistics
        memory_stats = list(collection.aggregate([
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
        response = {
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
            response["memory_stats"] = {
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


@app.route('/mappings', methods=['GET'])
def get_mappings():
    """Endpoint to get current mappings configuration."""
    try:
        current_mappings = load_mappings()
        return jsonify({
            "mappings": current_mappings,
            "last_modified": MAPPINGS_LAST_MODIFIED,
            "file_path": MAPPINGS_FILE
        })
    except Exception as e:
        logger.error(f"Failed to get mappings: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Failed to retrieve mappings"
        }), 500


@app.route('/mappings/reload', methods=['POST'])
def reload_mappings():
    """Endpoint to force reload of mappings configuration."""
    try:
        previous_sources = list(MAPPINGS.keys())
        load_mappings(force=True)
        current_sources = list(MAPPINGS.keys())

        return jsonify({
            "status": "success",
            "message": "Mappings reloaded successfully",
            "previous_sources": previous_sources,
            "current_sources": current_sources,
            "last_modified": MAPPINGS_LAST_MODIFIED
        })
    except Exception as e:
        logger.error(f"Failed to reload mappings: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Failed to reload mappings: {str(e)}"
        }), 500


@app.route('/mappings/sources', methods=['GET'])
def get_sources():
    """Endpoint to get list of available data sources."""
    try:
        # Reload mappings to ensure we have the latest version
        current_mappings = load_mappings()

        db_sources = list(collection.distinct("source"))
        return jsonify({
            "database_sources": db_sources,
            "mapping_sources": list(current_mappings.keys())
        })
    except Exception as e:
        logger.error(f"Failed to get sources: {str(e)}")
        return jsonify({
            "status": "error",
            "message": "Failed to retrieve sources"
        }), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)