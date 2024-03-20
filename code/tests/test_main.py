import sys
import os
from fastapi.testclient import TestClient

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src'))
from src.main import app  # noqa: E402


with TestClient(app) as client:
    def test_read_heartbeat():
        response = client.get("/api/heartbeat/Callisto")
        assert response.status_code == 200
        assert response.json()['data']['error_free'] is True