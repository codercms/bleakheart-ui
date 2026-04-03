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


def test_user_profiles_create_multiple_and_select():
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
        },
        "participant_002": {
            "name": "Participant 002",
            "sex": "female",
            "age_years": 29,
            "weight_kg": 62.5,
            "height_cm": 168.0,
            "hr_rest": 58,
            "hr_max": 184,
        },
    }
    repo.save_user_profiles(profiles, "participant_002")
    loaded_profiles, selected = repo.load_user_profiles()
    assert selected == "participant_002"
    assert sorted(loaded_profiles.keys()) == ["participant_001", "participant_002"]
    assert loaded_profiles["participant_002"]["sex"] == "female"
    assert int(loaded_profiles["participant_002"]["hr_max"]) == 184
    shutil.rmtree(root, ignore_errors=True)


def test_user_profiles_update_overwrites_existing_rows():
    root = _make_repo_root()
    repo = SessionIndexRepository(root)
    initial = {
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
    repo.save_user_profiles(initial, "participant_001")

    updated = {
        "participant_001": {
            "name": "Updated Name",
            "sex": "male",
            "age_years": 31,
            "weight_kg": 82.2,
            "height_cm": 181.0,
            "hr_rest": 59,
            "hr_max": 188,
        }
    }
    repo.save_user_profiles(updated, "participant_001")
    loaded_profiles, selected = repo.load_user_profiles()
    p = loaded_profiles["participant_001"]
    assert selected == "participant_001"
    assert p["name"] == "Updated Name"
    assert int(p["age_years"]) == 31
    assert float(p["weight_kg"]) == 82.2
    assert int(p["hr_rest"]) == 59
    assert int(p["hr_max"]) == 188
    shutil.rmtree(root, ignore_errors=True)
