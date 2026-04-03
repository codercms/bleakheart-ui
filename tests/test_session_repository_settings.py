import shutil
import uuid
from pathlib import Path

from bleakheart_ui.infra.session_repository import SessionIndexRepository


def _make_repo_root() -> Path:
    root = Path("tests") / "_tmp_repo" / str(uuid.uuid4())
    root.mkdir(parents=True, exist_ok=True)
    return root


def test_app_settings_roundtrip():
    root = _make_repo_root()
    repo = SessionIndexRepository(root)
    payload = {
        "last_device_address": "AA:BB:CC:DD:EE:FF",
        "auto_connect_on_startup": True,
        "recording_disconnect_grace_ms": 45000,
        "live_measurements": {"ECG": True, "ACC": False},
    }
    repo.save_app_settings(payload)
    loaded = repo.load_app_settings()
    assert loaded["last_device_address"] == "AA:BB:CC:DD:EE:FF"
    assert loaded["auto_connect_on_startup"] is True
    assert int(loaded["recording_disconnect_grace_ms"]) == 45000
    assert loaded["live_measurements"]["ECG"] is True
    shutil.rmtree(root, ignore_errors=True)


def test_user_profiles_roundtrip():
    root = _make_repo_root()
    repo = SessionIndexRepository(root)
    profiles = {
        "participant_001": {
            "name": "Participant 001",
            "sex": "male",
            "age_years": 30,
            "weight_kg": 80.0,
            "height_cm": 180.0,
            "hr_rest": 60,
            "hr_max": 190,
        }
    }
    repo.save_user_profiles(profiles, "participant_001")
    loaded_profiles, selected = repo.load_user_profiles()
    assert selected == "participant_001"
    assert "participant_001" in loaded_profiles
    assert loaded_profiles["participant_001"]["name"] == "Participant 001"
    shutil.rmtree(root, ignore_errors=True)
