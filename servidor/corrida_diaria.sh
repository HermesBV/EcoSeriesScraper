#!/bin/sh
# Corrida diaria del servidor: dependencias, scrapers, Excel de coyuntura y copia a SeriesMacro.
#
# Uso (crontab del usuario que corre los scrapers):
#   0 3 * * * SERIESMACRO_DIR=/ruta/a/SeriesMacro sh /ruta/a/SeriesScraper/servidor/corrida_diaria.sh >> /ruta/a/SeriesScraper/logs/cron.log 2>&1
#
# La copia se hace recién cuando main.py terminó, aunque algún scraper haya fallado:
# BD.xlsx e IndiceSeries.xlsx siempre viajan juntos y el Excel de coyuntura sólo
# cambia si el actualizador lo verificó. Cada archivo se copia a un temporal y se
# renombra, así la web nunca lee un archivo a medio copiar.
set -u
cd "$(dirname "$0")/.."

: "${SERIESMACRO_DIR:?Falta SERIESMACRO_DIR (carpeta de SeriesMacro)}"
if [ -z "${PYTHON:-}" ]; then
    for candidato in venv/bin/python .venv/bin/python; do
        [ -x "$candidato" ] && PYTHON=$candidato && break
    done
fi
: "${PYTHON:?No se encontró el Python del entorno (venv/ o .venv/); indicarlo con PYTHON=}"

echo "=== $(date '+%F %T') inicio"
# Instala lo que falte de requirements.txt (si ya está todo, no baja nada).
"$PYTHON" -m pip install -q -r requirements.txt || echo "*** no se pudieron instalar las dependencias"

# Scrapers, calendario y, en Linux con LibreOffice, el Excel de coyuntura. Si la corrida
# anterior se cortó hace menos de 20 h, main.py la retoma: corre sólo los scrapers que
# faltaron (con --de-cero, todos). Los argumentos de este script pasan a main.py.
"$PYTHON" main.py "$@"
estado=$?
if [ $estado -eq 75 ]; then
    echo "*** hay otra corrida en curso (candado en .corrida/); ésta no copia nada"
    exit 75
fi
[ $estado -ne 0 ] && echo "*** main.py terminó con código $estado (detalle arriba y en la hoja «Estado corrida» de BD.xlsx)"

copiar() {
    origen=$1
    destino=$2
    if [ ! -s "$origen" ]; then
        echo "*** no existe $origen; no se copió"
        estado=1
        return
    fi
    mkdir -p "$(dirname "$destino")"
    cp "$origen" "$destino.tmp" && mv -f "$destino.tmp" "$destino" && echo "Copiado: $destino" \
        || { echo "*** falló la copia a $destino"; estado=1; }
}
copiar BD.xlsx "$SERIESMACRO_DIR/bds/BD.xlsx"
copiar IndiceSeries.xlsx "$SERIESMACRO_DIR/bds/IndiceSeries.xlsx"
copiar "coyuntura/coyuntura iiep reemplazar.xlsx" "$SERIESMACRO_DIR/coyuntura/coyuntura iiep reemplazar.xlsx"

echo "=== $(date '+%F %T') fin (estado $estado)"
exit $estado
