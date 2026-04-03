from bleakheart_ui.features.main.app_settings import AppSettings, RuntimeSettings, UiSettings


def test_runtime_settings_defaults_and_normalization():
    settings = RuntimeSettings.from_dict(
        {
            "auto_reconnect_interval_ms": 999999,
            "recording_disconnect_mode": "invalid",
            "recording_disconnect_grace_ms": 1,
        }
    )
    assert settings.auto_connect_on_startup is True
    assert settings.auto_reconnect_interval_ms == 30000
    assert settings.recording_disconnect_mode == "pause_then_stop"
    assert settings.recording_disconnect_grace_ms == 10000


def test_ui_settings_defaults_keep_hr_rr_split():
    settings = UiSettings.from_dict({})
    assert settings.combine_hr_rr_chart is False
    assert settings.render_fps_mode == "manual"
    assert settings.focus_chart_preference == "ECG"


def test_app_settings_dialog_payload_contains_both_groups():
    app = AppSettings(
        runtime=RuntimeSettings.from_dict({"auto_connect_on_startup": False}),
        ui=UiSettings.from_dict({"combine_hr_rr_chart": True}),
    )
    payload = app.to_dialog_payload()
    assert payload["auto_connect_on_startup"] is False
    assert payload["combine_hr_rr_chart"] is True
