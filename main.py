import json
import logging
from flask import Flask, request, jsonify
from statistics import mean

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger=logging.getLogger(__name__)

app=Flask(__name__)
MAPPINGS_FILE='mappings.json'
MACHINES=[]  # In-memory storage
MAX_LIMIT=100  # Maximum pagination limit

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

# Sample data to populate on startup
SAMPLE_DATA={
    "team_a": [
        {"os": "Ubuntu 20.04", "cpu_model": "Xeon E5", "memory_gb": 64},
        {"os": "Ubuntu 22.04", "cpu_model": "Xeon Gold", "memory_gb": "128"},
    ],

    "team_b": {
        "OperatingSystem": "Debian 12",
        "CPU": "Intel i7",
        "RAM": "16 GB"
    },
    "team_c": {
        "OSName": "Windows Server 2022",
        "processor": ["Xeon Platinum", "Xeon Platinum"],
        "mem": 262144
    }
}

# Add test entries (test1 to test50) to team_a using a loop
for i in range(1, 50):
    SAMPLE_DATA["team_a"].append({
        "os": "Ubuntu 20.04",
        "cpu_model": f"Xeon E5 test{i}",
        "memory_gb": 64
    })

# Helper to safely evaluate lambda expressions for memory conversion
def safe_lambda(data, lambda_str):
    """Safely evaluate lambda expressions for memory conversion."""
    if lambda_str=="lambda data: int(data['RAM'].split()[0])":
        try:
            return int(data['RAM'].split()[0])
        except (KeyError, IndexError, ValueError) as e:
            raise ValueError(f"Failed to parse RAM: {str(e)}")
    elif lambda_str=="lambda data: int(data['mem']) / 1024":
        try:
            return int(data['mem']) / 1024
        except (KeyError, ValueError) as e:
            raise ValueError(f"Failed to convert mem to GB: {str(e)}")
    else:
        raise ValueError(f"Unsupported lambda expression: {lambda_str}")

# Helper to get field value based on mapping
def get_field(data, mapping):
    if isinstance(mapping, list):
        # Try case-insensitive matching
        for key in mapping:
            for k in data:
                if k.lower()==key.lower():
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
                if k.lower()==mapping.lower():
                    return data[k]
            return data.get(mapping)
    else:
        raise ValueError("Invalid mapping type")

# Insert normalized record into in-memory storage
def insert_record(record):
    MACHINES.append(record)
    logger.info(f"Inserted record: {record}")

# Function to populate sample data
def populate_sample_data():
    """Populate MACHINES with sample data on startup."""
    for source, data in SAMPLE_DATA.items():
        if source not in MAPPINGS:
            logger.error(f"Skipping sample data for {source}: not in mappings")
            continue

        items=[data] if isinstance(data, dict) else data
        inserted=0
        errors=[]

        for i, item in enumerate(items):
            if not isinstance(item, dict):
                errors.append(f"Sample item {i} for {source} is not a valid JSON object")
                logger.error(f"Sample item {i} for {source} is not a valid JSON object")
                continue

            try:
                maps=MAPPINGS[source]

                def extract_field(field_name):
                    mapping=maps.get(field_name)
                    if mapping is None:
                        return "" if field_name!='memory_gb' else None

                    val=get_field(item, mapping)

                    if field_name=='cpu' and isinstance(val, list):
                        val=', '.join(map(str, val))

                    if field_name=='memory_gb' and val is not None:
                        try:
                            val=float(val)
                        except (ValueError, TypeError):
                            raise ValueError(f"Invalid memory_gb value for item {i}: {val}")

                    if field_name in ('os', 'cpu') and val is not None and not isinstance(val, str):
                        raise ValueError(f"Invalid {field_name} type for item {i}: expected string, got {type(val)}")

                    return val

                record={
                    "os": extract_field("os") or "",
                    "cpu": extract_field("cpu") or "",
                    "memory_gb": extract_field("memory_gb")
                }

                insert_record(record)
                inserted+=1
            except Exception as e:
                error_msg=f"Error processing sample item {i} for {source}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)

        logger.info(f"Populated sample data for {source}: inserted {inserted}, errors {errors}")

# Clear in-memory storage and populate sample data on startup
MACHINES.clear()
logger.info("Cleared in-memory storage on startup.")
populate_sample_data()

@app.route('/machines', methods=['POST'])
def post_machines():
    source=request.headers.get('X-Source')
    if not source or source not in MAPPINGS:
        logger.error(f"Invalid or missing X-Source header: {source}")
        return jsonify({"status": "error", "message": f"Invalid or missing X-Source header: {source}"}), 400

    try:
        data=request.json
    except:
        logger.error("Invalid JSON payload received")
        return jsonify({"status": "error", "message": "Invalid JSON payload"}), 400

    if not isinstance(data, (dict, list)):
        logger.error(f"Payload must be a JSON object or array, got: {type(data)}")
        return jsonify({"status": "error", "message": "Payload must be a JSON object or array"}), 400

    items=[data] if isinstance(data, dict) else data
    inserted=0
    errors=[]

    for i, item in enumerate(items):
        if not isinstance(item, dict):
            errors.append(f"Item {i} is not a valid JSON object")
            logger.error(f"Item {i} is not a valid JSON object")
            continue

        try:
            maps=MAPPINGS[source]

            def extract_field(field_name):
                mapping=maps.get(field_name)
                if mapping is None:
                    return "" if field_name!='memory_gb' else None

                val=get_field(item, mapping)

                if field_name=='cpu' and isinstance(val, list):
                    val=', '.join(map(str, val))

                if field_name=='memory_gb' and val is not None:
                    try:
                        val=float(val)
                    except (ValueError, TypeError):
                        raise ValueError(f"Invalid memory_gb value for item {i}: {val}")

                if field_name in ('os', 'cpu') and val is not None and not isinstance(val, str):
                    raise ValueError(f"Invalid {field_name} type for item {i}: expected string, got {type(val)}")

                return val

            record={
                "os": extract_field("os") or "",
                "cpu": extract_field("cpu") or "",
                "memory_gb": extract_field("memory_gb")
            }

            insert_record(record)
            inserted+=1
        except Exception as e:
            error_msg=f"Error processing item {i}: {str(e)}"
            errors.append(error_msg)
            logger.error(error_msg)

    response={"status": "ok", "inserted": inserted, "errors": errors}
    logger.info(f"POST /machines response: {response}")
    return jsonify(response)

@app.route('/machines', methods=['GET'])
def get_machines():
    os_filter=request.args.get('os')
    cpu_filter=request.args.get('cpu')
    limit=request.args.get('limit', type=int)
    offset=request.args.get('offset', type=int, default=0)

    if limit is not None and limit>MAX_LIMIT:
        logger.warning(f"Requested limit {limit} exceeds max {MAX_LIMIT}, using max.")
        limit=MAX_LIMIT

    filtered=MACHINES

    if os_filter:
        filtered=[r for r in filtered if r['os']==os_filter]
    if cpu_filter:
        filtered=[r for r in filtered if r['cpu']==cpu_filter]

    if limit is not None:
        filtered=filtered[offset:offset + limit]

    logger.info(f"GET /machines: Returned {len(filtered)} records")
    return jsonify(filtered)

@app.route('/stats', methods=['GET'])
def get_stats():
    total_records=len(MACHINES)

    os_distribution={}
    for record in MACHINES:
        os=record['os']
        if os:  # Exclude empty os
            os_distribution[os]=os_distribution.get(os, 0) + 1

    memory_values=[r['memory_gb'] for r in MACHINES if r['memory_gb'] is not None]
    avg_memory_gb=float(mean(memory_values)) if memory_values else 0.0

    response={
        "total_records": total_records,
        "os_distribution": os_distribution,
        "avg_memory_gb": avg_memory_gb
    }
    logger.info(f"GET /stats: {response}")
    return jsonify(response)

if __name__=='__main__':
    app.run(debug=True, port=5000)