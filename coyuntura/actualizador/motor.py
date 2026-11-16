"""Elige con qué programa se edita el libro: Excel (Windows) o LibreOffice.

``COYUNTURA_MOTOR=excel`` o ``libreoffice`` fuerza uno; si no, Excel donde está
``pywin32`` y LibreOffice en el resto.
"""

from __future__ import annotations

import os
import sys


def _elegir():
    pedido = os.environ.get("COYUNTURA_MOTOR", "").strip().lower()
    if pedido not in {"", "excel", "libreoffice"}:
        raise RuntimeError(f"COYUNTURA_MOTOR desconocido: {pedido!r} (excel o libreoffice)")
    if pedido == "excel" or (not pedido and sys.platform == "win32"):
        from .excel_com import Excel
        return Excel
    from .libreoffice import LibreOffice
    return LibreOffice


def Excel():
    """El editor del libro, con la interfaz de ``excel_com.Excel``."""
    return _elegir()()
