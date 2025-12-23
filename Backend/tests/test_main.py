import pytest
from fastapi.testclient import TestClient
from main import app
from unittest.mock import patch
from core.security import create_access_token

client = TestClient(app)

# Test the root endpoint
def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Hello World"}

# Test the /caixas endpoint with mocked authentication
def test_caixas():
    token = create_access_token({"sub": "test_user", "role": "admin"})
    headers = {"Authorization": f"Bearer {token}"}

    with patch("core.security.get_current_user", return_value={"role": "admin", "username": "test_user"}):
        response = client.get("/caixas/", params={"data_inicio": "2025-01-01", "data_fim": "2025-01-31"}, headers=headers)
        assert response.status_code == 200
        assert "motoristas" in response.json()
        assert "ajudantes" in response.json()