import unittest
import mongomock
from flask import Flask
from unittest.mock import patch
import json
from main import app, collection, MAPPINGS

class TestMachinesEndpoint(unittest.TestCase):
    def setUp(self):
        """Set up the Flask test client and mock MongoDB collection."""
        self.app=app
        self.client=self.app.test_client()
        self.app.config['TESTING']=True

        # Patch the MongoDB collection with a mongomock collection
        self.mock_db=mongomock.MongoClient().db
        self.mock_collection=self.mock_db['machine_data']
        self.patcher_collection=patch('main.collection', self.mock_collection)
        self.patcher_collection.start()

        # Mock MAPPINGS to simulate mappings.json for POST test
        self.mock_mappings={
            "source1": {
                "os": "operating_system",
                "cpu": "processor",
                "memory_gb": "lambda data: int(data['RAM'].split()[0])"
            }
        }
        self.patcher_mappings=patch('main.MAPPINGS', self.mock_mappings)
        self.patcher_mappings.start()

    def tearDown(self):
        """Clean up after the test."""
        self.patcher_collection.stop()
        self.patcher_mappings.stop()

    def test_get_machines_with_pagination_and_filter(self):
        """Test /machines GET endpoint with pagination and OS filter."""
        # Insert mock data
        mock_data=[
            {"os": "Windows", "cpu": "Intel i7", "memory_gb": 16.0, "source": "source1", "timestamp": "2023-10-01"},
            {"os": "Linux", "cpu": "AMD Ryzen", "memory_gb": 32.0, "source": "source1", "timestamp": "2023-10-02"},
            {"os": "Windows", "cpu": "Intel i5", "memory_gb": 8.0, "source": "source2", "timestamp": "2023-10-03"},
            {"os": "MacOS", "cpu": "Apple M1", "memory_gb": 16.0, "source": "source2", "timestamp": "2023-10-04"}
        ]
        self.mock_collection.insert_many(mock_data)

        # Make GET request to /machines with pagination (limit=2, offset=0) and OS filter (os=Windows)
        response=self.client.get('/machines?limit=2&offset=0&os=Windows')


        # Check status code
        self.assertEqual(response.status_code, 200, f"Expected status code 200, got {response.status_code}")

        # Parse response
        data=json.loads(response.data)

        # Expected response (only Windows machines, first 2 records, excluding _id, source, timestamp)
        expected_data=[
            {"os": "Windows", "cpu": "Intel i7", "memory_gb": 16.0},
            {"os": "Windows", "cpu": "Intel i5", "memory_gb": 8.0}
        ]

        # Assertions
        self.assertEqual(len(data), 2, "Expected 2 records in response")
        self.assertEqual(data, expected_data, "Response data mismatch")

    def test_post_machines_valid_data(self):
        """Test /machines POST endpoint with valid data and X-Source header."""
        # Clear the collection to ensure no residual data
        self.mock_collection.delete_many({})

        # Test data
        post_data={
            "operating_system": "Windows",
            "processor": "Intel i7",
            "RAM": "16 GB"
        }

        # Make POST request to /machines with X-Source header
        response=self.client.post(
            '/machines',
            data=json.dumps(post_data),
            content_type='application/json',
            headers={'X-Source': 'source1'}
        )


        # Check status code
        self.assertEqual(response.status_code, 200, f"Expected status code 200, got {response.status_code}")

        # Parse response
        data=json.loads(response.data)

        # Expected response
        expected_response={
            "status": "success",
            "inserted": 1,
            "errors": [],
            "message": "Inserted 1 records"
        }

        # Assertions for response
        self.assertEqual(data, expected_response, "Response data mismatch")

        # Verify data was inserted into the collection
        inserted_docs=list(self.mock_collection.find())
        self.assertEqual(len(inserted_docs), 1, "Expected 1 document in collection")
        expected_doc={
            "os": "Windows",
            "cpu": "Intel i7",
            "memory_gb": 16.0
        }
        # Remove MongoDB-specific fields for comparison
        inserted_doc={
            "os": inserted_docs[0]["os"],
            "cpu": inserted_docs[0]["cpu"],
            "memory_gb": inserted_docs[0]["memory_gb"]
        }
        self.assertEqual(inserted_doc, expected_doc, "Inserted document mismatch")

    def test_get_all_machines(self):
        """Test /machines GET endpoint to retrieve all machine data without pagination or filters."""
        # Clear the collection to ensure no residual data
        self.mock_collection.delete_many({})

        # Insert mock data
        mock_data=[
            {"os": "Windows", "cpu": "Intel i7", "memory_gb": 16.0, "source": "source1", "timestamp": "2023-10-01"},
            {"os": "Linux", "cpu": "AMD Ryzen", "memory_gb": 32.0, "source": "source1", "timestamp": "2023-10-02"},
            {"os": "Windows", "cpu": "Intel i5", "memory_gb": 8.0, "source": "source2", "timestamp": "2023-10-03"},
            {"os": "MacOS", "cpu": "Apple M1", "memory_gb": 16.0, "source": "source2", "timestamp": "2023-10-04"}
        ]
        self.mock_collection.insert_many(mock_data)

        # Verify data was inserted
        self.assertEqual(self.mock_collection.count_documents({}), 4, "Failed to insert mock data")

        # Make GET request to /machines without query parameters
        response=self.client.get('/machines')



        # Check status code
        self.assertEqual(response.status_code, 200, f"Expected status code 200, got {response.status_code}")

        # Parse response
        data=json.loads(response.data)

        # Expected response (all machines, excluding _id, source, timestamp)
        expected_data=[
            {"os": "Windows", "cpu": "Intel i7", "memory_gb": 16.0},
            {"os": "Linux", "cpu": "AMD Ryzen", "memory_gb": 32.0},
            {"os": "Windows", "cpu": "Intel i5", "memory_gb": 8.0},
            {"os": "MacOS", "cpu": "Apple M1", "memory_gb": 16.0}
        ]

        # Assertions
        self.assertEqual(len(data), 4, "Expected 4 records in response")
        self.assertEqual(data, expected_data, "Response data mismatch")



if __name__ == '__main__':
    unittest.main()