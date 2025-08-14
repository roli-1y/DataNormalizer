import unittest
import json
from flask import Flask
from main import app
from main import MACHINES

class MachineServiceTestCase(unittest.TestCase):
    def setUp(self):
        """Set up the test client and clear in-memory storage."""
        self.app=app
        self.client=self.app.test_client()
        MACHINES.clear()  # Reset in-memory storage before each test

    def test_post_machines_team_a_valid_single(self):
        """Test POST /machines with a valid single payload for team_a."""
        payload={
            "os": "Ubuntu 20.04",
            "cpu_model": "Xeon E5",
            "memory_gb": 64
        }
        headers={"X-Source": "team_a", "Content-Type": "application/json"}
        response=self.client.post('/machines', headers=headers, json=payload)

        self.assertEqual(response.status_code, 200)
        data=response.get_json()
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["inserted"], 1)
        self.assertEqual(len(data["errors"]), 0)
        self.assertEqual(len(MACHINES), 1)
        self.assertEqual(MACHINES[0], {"os": "Ubuntu 20.04", "cpu": "Xeon E5", "memory_gb": 64.0})

    def test_post_machines_team_b_valid_array(self):
        """Test POST /machines with a valid array payload for team_b."""
        payload=[
            {"OperatingSystem": "Debian 12", "CPU": "Ryzen 7", "RAM": "32 GB"},
            {"OperatingSystem": "Ubuntu 22.04", "CPU": "Intel i9", "RAM": "16 GB"}
        ]
        headers={"X-Source": "team_b", "Content-Type": "application/json"}
        response=self.client.post('/machines', headers=headers, json=payload)

        self.assertEqual(response.status_code, 200)
        data=response.get_json()
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["inserted"], 2)
        self.assertEqual(len(data["errors"]), 0)
        self.assertEqual(len(MACHINES), 2)
        self.assertEqual(MACHINES[0]["os"], "Debian 12")
        self.assertEqual(MACHINES[0]["memory_gb"], 32)
        self.assertEqual(MACHINES[1]["os"], "Ubuntu 22.04")
        self.assertEqual(MACHINES[1]["memory_gb"], 16)

    def test_post_machines_team_c_os_inconsistent(self):
        """Test POST /machines with team_c's inconsistent os field names."""
        payload=[
            {"osName": "CentOS 7", "processor": "Xeon", "mem": 32768},
            {"OSName": "Fedora 34", "processor": "Core i5", "mem": 16384}
        ]
        headers={"X-Source": "team_c", "Content-Type": "application/json"}
        response=self.client.post('/machines', headers=headers, json=payload)

        self.assertEqual(response.status_code, 200)
        data=response.get_json()
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["inserted"], 2)
        self.assertEqual(len(data["errors"]), 0)
        self.assertEqual(len(MACHINES), 2)
        self.assertEqual(MACHINES[0]["os"], "CentOS 7")
        self.assertEqual(MACHINES[0]["memory_gb"], 32)  # 32768 MB / 1024
        self.assertEqual(MACHINES[1]["os"], "Fedora 34")
        self.assertEqual(MACHINES[1]["memory_gb"], 16)  # 16384 MB / 1024

    def test_post_machines_invalid_source(self):
        """Test POST /machines with an invalid X-Source header."""
        payload={"os": "Ubuntu 20.04", "cpu_model": "Xeon E5", "memory_gb": 64}
        headers={"X-Source": "invalid_team", "Content-Type": "application/json"}
        response=self.client.post('/machines', headers=headers, json=payload)

        self.assertEqual(response.status_code, 400)
        data=response.get_json()
        self.assertEqual(data["status"], "error")
        self.assertIn("Invalid or missing X-Source header", data["message"])
        self.assertEqual(len(MACHINES), 0)

    def test_post_machines_invalid_payload(self):
        """Test POST /machines with an invalid JSON payload."""
        headers={"X-Source": "team_a", "Content-Type": "application/json"}
        response=self.client.post('/machines', headers=headers, data="not_json")

        self.assertEqual(response.status_code, 400)
        data=response.get_json()
        self.assertEqual(data["status"], "error")
        self.assertIn("Invalid JSON payload", data["message"])
        self.assertEqual(len(MACHINES), 0)

    def test_post_machines_partial_invalid_array(self):
        """Test POST /machines with a partially invalid array payload."""
        payload=[
            {"os": "Ubuntu 20.04", "cpu_model": "Xeon E5", "memory_gb": 64},
            "invalid_item",
            {"os": "Debian 12", "cpu_model": "Ryzen 5", "memory_gb": 32}
        ]
        headers={"X-Source": "team_a", "Content-Type": "application/json"}
        response=self.client.post('/machines', headers=headers, json=payload)

        self.assertEqual(response.status_code, 200)
        data=response.get_json()
        self.assertEqual(data["status"], "ok")
        self.assertEqual(data["inserted"], 2)
        self.assertEqual(len(data["errors"]), 1)
        self.assertIn("Item 1 is not a valid JSON object", data["errors"])
        self.assertEqual(len(MACHINES), 2)

    def test_get_machines_no_filters(self):
        """Test GET /machines without filters."""
        MACHINES.extend([
            {"os": "Ubuntu 20.04", "cpu": "Xeon E5", "memory_gb": 64.0},
            {"os": "Debian 12", "cpu": "Ryzen 7", "memory_gb": 32.0}
        ])
        response=self.client.get('/machines')

        self.assertEqual(response.status_code, 200)
        data=response.get_json()
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["os"], "Ubuntu 20.04")
        self.assertEqual(data[1]["os"], "Debian 12")

    def test_get_machines_with_filters_and_pagination(self):
        """Test GET /machines with os filter, limit, and offset."""
        MACHINES.extend([
            {"os": "Ubuntu 20.04", "cpu": "Xeon E5", "memory_gb": 64.0},
            {"os": "Ubuntu 20.04", "cpu": "Ryzen 7", "memory_gb": 32.0},
            {"os": "Debian 12", "cpu": "Core i5", "memory_gb": 16.0}
        ])
        response=self.client.get('/machines?os=Ubuntu 20.04&limit=1&offset=1')

        self.assertEqual(response.status_code, 200)
        data=response.get_json()
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["cpu"], "Ryzen 7")
        self.assertEqual(data[0]["memory_gb"], 32.0)

    def test_get_stats(self):
        """Test GET /stats with sample data."""
        MACHINES.extend([
            {"os": "Ubuntu 20.04", "cpu": "Xeon E5", "memory_gb": 64.0},
            {"os": "Ubuntu 20.04", "cpu": "Ryzen 7", "memory_gb": None},
            {"os": "Debian 12", "cpu": "Core i5", "memory_gb": 16.0},
            {"os": "", "cpu": "Core i7", "memory_gb": 32.0}
        ])
        response=self.client.get('/stats')

        self.assertEqual(response.status_code, 200)
        data=response.get_json()
        self.assertEqual(data["total_records"], 4)
        self.assertEqual(data["os_distribution"], {
            "Ubuntu 20.04": 2,
            "Debian 12": 1
        })
        self.assertAlmostEqual(data["avg_memory_gb"], (64.0 + 16.0 + 32.0) / 3)

    def test_get_stats_empty(self):
        """Test GET /stats with no data."""
        response=self.client.get('/stats')

        self.assertEqual(response.status_code, 200)
        data=response.get_json()
        self.assertEqual(data["total_records"], 0)
        self.assertEqual(data["os_distribution"], {})
        self.assertEqual(data["avg_memory_gb"], 0.0)


if __name__=='__main__':
    unittest.main()