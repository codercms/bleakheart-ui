class ConnectionManager:
    def __init__(self, *, auto_reconnect_enabled: bool = True, auto_reconnect_interval_ms: int = 5000):
        self.auto_reconnect_enabled = bool(auto_reconnect_enabled)
        self.auto_reconnect_interval_ms = int(auto_reconnect_interval_ms)

        self.connect_attempt_inflight = False
        self._suppress_disconnect_autoreconnect = False
        self._auto_reconnect_inflight = False
        self._auto_reconnect_address = None

    def should_enable_connect_button(self, connected: bool) -> bool:
        return bool(connected or (not self.connect_attempt_inflight))

    def begin_connect_attempt(self, *, connected: bool) -> bool:
        if connected or self.connect_attempt_inflight:
            return False
        self.connect_attempt_inflight = True
        return True

    def finish_connect_success(self):
        self.connect_attempt_inflight = False
        self._suppress_disconnect_autoreconnect = False
        self.stop_auto_reconnect()

    def finish_connect_failure(self):
        self.connect_attempt_inflight = False
        self._auto_reconnect_inflight = False

    def request_user_disconnect(self):
        self._suppress_disconnect_autoreconnect = True
        self.stop_auto_reconnect()

    def consume_disconnect_request(self) -> bool:
        requested = bool(self._suppress_disconnect_autoreconnect)
        self._suppress_disconnect_autoreconnect = False
        return requested

    def stop_auto_reconnect(self):
        self._auto_reconnect_inflight = False
        self._auto_reconnect_address = None

    def schedule_auto_reconnect(self, address: str | None) -> bool:
        if not self.auto_reconnect_enabled:
            return False
        if not address:
            return False
        self._auto_reconnect_address = str(address)
        self._auto_reconnect_inflight = False
        return True

    def next_auto_reconnect_address(
        self,
        *,
        connected: bool,
        is_shutting_down: bool,
        fallback_address: str | None,
    ) -> str | None:
        if connected or is_shutting_down:
            self.stop_auto_reconnect()
            return None
        if self._auto_reconnect_inflight or self.connect_attempt_inflight:
            return None
        address = self._auto_reconnect_address or fallback_address
        if not address:
            self.stop_auto_reconnect()
            return None
        self._auto_reconnect_inflight = True
        self.connect_attempt_inflight = True
        return str(address)

    def finish_auto_reconnect_failure(self):
        self._auto_reconnect_inflight = False
        self.connect_attempt_inflight = False

    def handle_disconnect_event(
        self,
        *,
        was_connected: bool,
        is_shutting_down: bool,
        last_address: str | None,
    ) -> str | None:
        user_requested = self.consume_disconnect_request()
        self.connect_attempt_inflight = False
        self._auto_reconnect_inflight = False
        if (not was_connected) or user_requested or is_shutting_down:
            return None
        ok = self.schedule_auto_reconnect(last_address)
        if not ok:
            return None
        return self._auto_reconnect_address

    def begin_shutdown(self):
        self._suppress_disconnect_autoreconnect = True
        self.stop_auto_reconnect()
