"""Lo que hace que una corrida de ``main.py`` sobreviva a un corte y no se trabe.

- **Avance**: ``.corrida/estado.json`` (ignorado por git) guarda el resultado de
  cada scraper apenas su lote se escribió. Si la corrida se corta (un reinicio,
  el proceso muerto), la siguiente que arranca dentro de ``HORAS_RETOMAR`` corre
  sólo los scrapers que faltaron; ``--de-cero`` lo evita. Al terminar, el estado
  pasa a ``.corrida/ultima.json``.
- **Base entera**: ``BD.xlsx`` e ``IndiceSeries.xlsx`` se escriben en un temporal
  que se renombra (``guardar_datos_preservando_formato``, ``escribir_indice``):
  un corte deja el archivo anterior o el nuevo, nunca uno a medias. Si el corte
  cae entre los dos renombres, el lote no quedó anotado y se vuelve a correr.
- **Tiempo máximo**: cada lote corre en su propio grupo de procesos y, si pasa su
  tiempo, se mata el grupo entero (también el Chrome de Ámbito).
- **Candado**: ``.corrida/candado`` impide dos corridas a la vez; se toma como
  libre si el proceso que lo dejó ya no existe.

No importa pandas: ``main.py`` sólo coordina y tiene que ocupar poca memoria.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
CARPETA = RAIZ / ".corrida"
HORAS_RETOMAR = 20
FORMATO_HORA = "%Y-%m-%d %H:%M:%S"
AGOTADO = -1000   # código de salida de un lote que pasó su tiempo máximo


class CorridaEnCurso(RuntimeError):
    pass


def _arranque_linux(pid: int) -> str | None:
    """Momento de arranque del proceso (en ticks desde el inicio del sistema), para no confundir un pid reusado."""
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
    except (OSError, IndexError):
        return None


def _arranque_sistema() -> str | None:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        return None


def vive(pid: int) -> bool:
    """¿Existe el proceso? Sin señales en Windows, donde ``os.kill`` lo terminaría."""
    if pid <= 0:
        return False
    if sys.platform == "win32":
        import ctypes
        kernel32 = ctypes.windll.kernel32
        manejador = kernel32.OpenProcess(0x1000, False, pid)   # PROCESS_QUERY_LIMITED_INFORMATION
        if not manejador:
            return False
        try:
            codigo = ctypes.c_ulong()
            return bool(kernel32.GetExitCodeProcess(manejador, ctypes.byref(codigo))) and codigo.value == 259
        finally:
            kernel32.CloseHandle(manejador)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _proceso_vivo(pid: object, arranque: object) -> bool:
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    return vive(pid) and (arranque is None or arranque == _arranque_linux(pid))


def _dueno_vivo(datos: dict) -> bool:
    """Vive quien tomó el candado o el lote que lanzó (que sigue escribiendo aunque su coordinador muera)."""
    if datos.get("maquina") != socket.gethostname():
        return True   # otra máquina comparte la carpeta: no se puede saber, se respeta
    if datos.get("sistema") and datos.get("sistema") != _arranque_sistema():
        return False  # la máquina se reinició desde que se tomó
    return (_proceso_vivo(datos.get("pid"), datos.get("arranque"))
            or _proceso_vivo(datos.get("lote"), datos.get("lote_arranque")))


def tomar_candado(carpeta: Path | None = None) -> Path:
    """Crea el candado o falla con ``CorridaEnCurso``; un candado de un proceso muerto se reemplaza."""
    carpeta = carpeta or CARPETA
    carpeta.mkdir(parents=True, exist_ok=True)
    ruta = carpeta / "candado"
    datos = {"pid": os.getpid(), "maquina": socket.gethostname(), "desde": time.strftime(FORMATO_HORA),
             "sistema": _arranque_sistema(), "arranque": _arranque_linux(os.getpid())}
    for _ in range(2):
        try:
            descriptor = os.open(ruta, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                previo = json.loads(ruta.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                previo = {}
            if previo and _dueno_vivo(previo):
                raise CorridaEnCurso(f"hay otra corrida en curso (pid {previo.get('pid')}, desde {previo.get('desde')})")
            print(f"Candado viejo de una corrida que ya no existe ({previo or 'ilegible'}): se libera", flush=True)
            ruta.unlink(missing_ok=True)
            continue
        with os.fdopen(descriptor, "w", encoding="utf-8") as archivo:
            json.dump(datos, archivo)
        return ruta
    raise CorridaEnCurso(f"no se pudo tomar {ruta}")


def anotar_lote(ruta: Path, pid: int) -> None:
    """Anota en el candado el proceso del lote en curso."""
    datos = leer(ruta)
    if datos.get("pid") == os.getpid():
        anotar(ruta, {**datos, "lote": pid, "lote_arranque": _arranque_linux(pid)})


def soltar_candado(ruta: Path) -> None:
    try:
        if json.loads(ruta.read_text(encoding="utf-8")).get("pid") == os.getpid():
            ruta.unlink()
    except (OSError, ValueError):
        pass


def leer(ruta: Path) -> dict:
    try:
        return json.loads(ruta.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def anotar(ruta: Path, estado: dict) -> None:
    temporal = ruta.with_name(ruta.name + ".tmp")
    temporal.write_text(json.dumps(estado, ensure_ascii=False), encoding="utf-8")
    temporal.replace(ruta)


def retomable(ruta: Path, horas: float = HORAS_RETOMAR, ahora: float | None = None) -> dict:
    """Estado de una corrida cortada hace menos de ``horas``, sin lo que no llegó a escribirse; si no, ``{}``."""
    estado = leer(ruta)
    try:
        inicio = time.mktime(time.strptime(estado["Inicio"], FORMATO_HORA))
    except (KeyError, ValueError, TypeError):
        return {}
    if (ahora or time.time()) - inicio > horas * 3600:
        return {}
    for nombre in [n.strip() for n in str(estado.pop("_sin_escribir", "")).split(",") if n.strip()]:
        estado.pop(nombre, None)
    return estado


def cerrar(ruta: Path) -> None:
    """La corrida terminó: su estado queda como ``ultima.json`` y ya no se retoma."""
    if ruta.is_file():
        ruta.replace(ruta.with_name("ultima.json"))


def limpiar_lotes_viejos(carpeta_lote: Path) -> None:
    """Borra las carpetas de ``.lote/`` de procesos que ya no existen (quedan si se corta un lote)."""
    import shutil
    if not carpeta_lote.is_dir():
        return
    for carpeta in carpeta_lote.iterdir():
        if carpeta.is_dir() and carpeta.name.isdigit() and not vive(int(carpeta.name)):
            shutil.rmtree(carpeta, ignore_errors=True)


def correr_con_tiempo(orden: list[str], segundos: float | None, cwd: Path = RAIZ, candado: Path | None = None) -> int:
    """Corre ``orden`` en un grupo de procesos propio; si pasa ``segundos``, lo mata entero y devuelve ``AGOTADO``."""
    opciones = ({"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if sys.platform == "win32"
                else {"start_new_session": True})
    proceso = subprocess.Popen(orden, cwd=cwd, **opciones)
    if candado is not None:
        anotar_lote(candado, proceso.pid)
    try:
        return proceso.wait(timeout=segundos)
    except subprocess.TimeoutExpired:
        _matar_grupo(proceso)
        return AGOTADO
    except BaseException:
        # Si se corta quien coordina (Ctrl+C), el lote no sigue corriendo solo.
        _matar_grupo(proceso)
        raise


def _matar_grupo(proceso: subprocess.Popen) -> None:
    if sys.platform == "win32":
        subprocess.run(["taskkill", "/T", "/F", "/PID", str(proceso.pid)], capture_output=True)
    else:
        try:
            os.killpg(proceso.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    proceso.wait()
