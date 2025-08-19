A Flask-based REST API for ingesting, storing, and querying machine specification data from multiple sources with different field naming conventions.

*Overview*

	This API provides endpoints to:
	
	Ingest machine data from various sources with different field naming conventions
	
	Store data in MongoDB with normalized field names
	
	Query and retrieve machine data with pagination and filtering
	
	Generate statistics about the collected data
	
	Manage field mapping configurations dynamically

*Key Features*

	Flexible Data Ingestion: Handles multiple data sources with different field naming conventions
	
	Dynamic Field Mapping: JSON-based configuration for field name mappings and conversions
	
	MongoDB Integration: Efficient storage and querying of machine data
	
	RESTful API: Clean endpoints for all operations
	
	CORS Support: Frontend-friendly API design

	Comprehensive Logging: Detailed logging for debugging and monitoring

*API Endpoints*

	POST /machines
	Ingest machine data from various sources.

	Headers:
	
	X-Source: Required - specifies which mapping configuration to use (e.g., "team_a", "team_b", "team_c")
	
	Body: JSON object or array of objects containing machine data
 

	GET /machines
		Retrieve machine data with pagination and filtering.
	
		*Query Parameters:*
		
		limit: Number of records to return (default: 100, max: 1000)
		
		offset: Pagination offset (default: 0)
		
		os: Filter by operating system
		
		cpu: Filter by CPU model
		
		source: Filter by data source
	
	GET /stats
		Get statistics about the collected machine data.
	
	GET /mappings
		Get current field mapping configuration.
	
	POST /mappings/reload
		Force reload of mappings configuration.
	
	GET /mappings/sources
Get list of available data sources.

Configuration
Mappings File (mappings.json)
The system uses a JSON configuration file to define how to map source-specific field names to normalized field names. Each source can define:

	os: Array of possible field names for operating system
 
	cpu: Array of possible field names for CPU model
	
	memory_gb: Object with:
	
	fields: Array of possible field names for memory
	
	convert: Lambda function string for value conversion
