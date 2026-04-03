from PySide6 import QtGui, QtWidgets


def scaled_font(
    base_font: QtGui.QFont,
    factor: float = 1.0,
    *,
    weight: QtGui.QFont.Weight | None = None,
) -> QtGui.QFont:
    out = QtGui.QFont(base_font)
    size = out.pointSizeF()
    if size <= 0:
        app_font = QtWidgets.QApplication.font()
        size = app_font.pointSizeF() if app_font.pointSizeF() > 0 else 10.0
    out.setPointSizeF(max(8.0, float(size) * float(factor)))
    if weight is not None:
        out.setWeight(weight)
    return out


def format_duration(total_s: float) -> str:
    total = max(0, int(round(float(total_s))))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def format_elapsed_tick_label(value_s: float, spacing_s: float = 1.0) -> str:
    total = max(0.0, float(value_s))
    spacing = max(0.0, float(spacing_s))
    hours = int(total // 3600.0)
    minutes = int((total % 3600.0) // 60.0)
    seconds = total % 60.0
    show_ms = spacing < 1.0

    if show_ms:
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{seconds:06.3f}"
        return f"{minutes:02d}:{seconds:06.3f}"

    sec_int = int(round(seconds))
    if sec_int == 60:
        sec_int = 0
        minutes += 1
        if minutes == 60:
            minutes = 0
            hours += 1
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{sec_int:02d}"
    return f"{minutes:02d}:{sec_int:02d}"
