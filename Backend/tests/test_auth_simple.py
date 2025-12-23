import os
import sys
from unittest.mock import MagicMock, patch

# Add parent directory to path so we can import app
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock create_client BEFORE importing main
with patch('supabase.create_client') as mock_create_client:
    mock_client_instance = MagicMock()
    mock_create_client.return_value = mock_client_instance
    from main import app
    from routers import auth

from fastapi.testclient import TestClient

client = TestClient(app)

def test_admin_login_success():
    # Mock environment variables if needed, but we set them in .env
    # For this test, we'll assume the default or what's in .env
    # Let's override the dependency to be sure or just test the logic
    
    # We can mock the environment variables by patching os.environ or just relying on the fact that we just wrote .env
    # But for safety, let's use the values we know we wrote.
    
    response = client.post(
        "/token",
        data={"username": "admin", "password": "secure_password_change_me"}
    )
    assert response.status_code == 200
    assert response.json()["role"] == "admin"
    print("Admin login success test passed!")

def test_admin_login_failure():
    response = client.post(
        "/token",
        data={"username": "admin", "password": "wrong_password"}
    )
    # Should fail or fall through to collaborator check
    # Since "admin" is likely not a valid CPF, it should eventually fail with 400 or 401
    # Depending on how get_cadastro_sincrono behaves with mocks.
    # To make this unit test run without a real Supabase connection, we should mock get_cadastro_sincrono
    
    # However, for a quick check, we expect 401 or 400 (if DB fails)
    assert response.status_code in [400, 401]
    print("Admin login failure test passed!")

if __name__ == "__main__":
    # Mocking Supabase and Database calls to avoid real network requests
    # We only want to test the Admin logic we changed
    
    # Mock get_cadastro_sincrono to return empty so it doesn't crash but fails auth
    auth.get_cadastro_sincrono = MagicMock(return_value=(None, "Mocked Error"))
    
    try:
        test_admin_login_success()
        test_admin_login_failure()
        print("All tests passed.")
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Test failed: {e}")
        exit(1)
