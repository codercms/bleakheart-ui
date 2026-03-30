import queue

from PySide6 import QtCore, QtGui, QtWidgets


class EngineEventPump(QtCore.QObject):
    event_received = QtCore.Signal(object)
    finished = QtCore.Signal()

    def __init__(self, events_queue):
        super().__init__()
        self._events = events_queue
        self._running = False

    @QtCore.Slot()
    def run(self):
        self._running = True
        while self._running:
            try:
                event = self._events.get(timeout=0.05)
            except queue.Empty:
                continue
            self.event_received.emit(event)
        self.finished.emit()

    @QtCore.Slot()
    def stop(self):
        self._running = False


class WideClickCheckBox(QtWidgets.QCheckBox):
    def hitButton(self, pos: QtCore.QPoint) -> bool:
        return self.rect().contains(pos)


class GuardedComboBox(QtWidgets.QComboBox):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFocusPolicy(QtCore.Qt.StrongFocus)

    # Prevent accidental value changes while wheel-scrolling the sidebar.
    def wheelEvent(self, event: QtGui.QWheelEvent):
        # Accept wheel only when user intentionally opened the dropdown.
        if self.view().isVisible():
            super().wheelEvent(event)
            return
        event.ignore()


class ContainedScrollArea(QtWidgets.QScrollArea):
    # Wheel events are fully handled here to avoid scroll-chain to parent containers.
    def wheelEvent(self, event: QtGui.QWheelEvent):
        bar = self.verticalScrollBar()
        if bar is None:
            event.accept()
            return

        pixel_delta = event.pixelDelta().y()
        angle_delta = event.angleDelta().y()

        if pixel_delta != 0:
            new_value = bar.value() - int(pixel_delta)
        elif angle_delta != 0:
            line_steps = float(angle_delta) / 120.0
            step_px = max(1, int(bar.singleStep())) * 3
            new_value = bar.value() - int(round(line_steps * step_px))
        else:
            event.accept()
            return

        new_value = max(bar.minimum(), min(bar.maximum(), int(new_value)))
        bar.setValue(new_value)
        event.accept()
