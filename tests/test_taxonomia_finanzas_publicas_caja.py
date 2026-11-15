"""Pruebas de las reglas de finanzas públicas en base caja (cuenta AIF e IMIG)."""

from __future__ import annotations

import unittest

from tools.taxonomia import Clasificacion, Serie
from tools.taxonomia.finanzas_publicas_caja import clasificar

AIF = "Cuenta ahorro-inversión-financiamiento (base caja)"
IMIG = "Informe mensual de ingresos y gastos (IMIG)"
HIST = "Series históricas"
TEMA = "Finanzas públicas"
UNIDAD = "Millones de pesos"

TITULOS = {
    "SPN": "Sector Público Nacional",
    "APN": "Administración Pública Nacional",
    "TN": "Tesoro Nacional",
    "OD": "Organismos Descentralizados",
    "ISS": "Instituciones de la Seguridad Social",
    "RA": "Recursos Afectados",
    "EP_PAMI_FF": "Empresas Públicas, PAMI, Fondos Fiduciarios y Otros",
    "Ex_CP": "Ex Cajas Provinciales",
}
FRECUENCIAS = {"A": "anuales", "T": "trimestrales", "M": "mensuales"}


def aif(hoja: str, descripcion: str, periodo: str = "2017", frecuencia: str = "A") -> Serie:
    titulo = f"{TITULOS[hoja]}. Valores {FRECUENCIAS[frecuencia]} {periodo}"
    return Serie(
        fuente="datos.gob.ar", id=f"379.1_PRUEBA_{hoja}_{frecuencia}", nombre=titulo, variable="x",
        descripcion=descripcion, unidad="", frecuencia=frecuencia, archivo="finanzas_publicas.xlsx",
        hoja=hoja, columna="c",
        dataset=f"Esquema Ahorro - Inversión - Financiamiento. {TITULOS[hoja]}. Base Caja.",
        estado="VIGENTE" if periodo == "2017" else "CERRADA",
    )


def imig_datos(descripcion: str, frecuencia: str = "M") -> Serie:
    return Serie(
        fuente="datos.gob.ar", id="452.3_PRUEBA", nombre="Informe Mensual de Ingresos y Gastos. Sector Público "
        "Nacional No Financiero.  Valores mensuales 2015-2018", variable="x", descripcion=descripcion, unidad="",
        frecuencia=frecuencia, archivo="finanzas_publicas.xlsx", hoja="IMIG", columna="c",
        dataset="Informe Mensual de Ingresos y Gastos", estado="VIGENTE",
    )


def imig_mecon(titulo: str) -> Serie:
    return Serie(
        fuente="mecon-imig", id=titulo.lower().replace(" ", "-"), nombre=titulo, variable="x",
        descripcion=f"{titulo}. Informe Mensual de Ingresos y Gastos, base caja, en millones de pesos.",
        unidad="millones de pesos", frecuencia="M", archivo="116 informes mensuales de Hacienda",
        hoja="IMIG", columna="c", dataset="Informe Mensual de Ingresos y Gastos (IMIG)", estado="VIGENTE",
    )


def vigente(subsector: str, bloque: str, nombre: str) -> Clasificacion:
    return Clasificacion(tema=TEMA, indicador=AIF, cobertura=subsector, apertura=bloque, nombre=nombre,
                         unidad=UNIDAD)


def historica(subsector: str, periodo: str, bloque: str, nombre: str) -> Clasificacion:
    return Clasificacion(tema=TEMA, indicador=AIF, cobertura=HIST,
                         apertura=f"{subsector} · metodología {periodo}", medida=bloque, nombre=nombre,
                         unidad=UNIDAD)


def imig(bloque: str, apertura: str, nombre: str) -> Clasificacion:
    return Clasificacion(tema=TEMA, indicador=IMIG, cobertura=bloque, apertura=apertura, nombre=nombre,
                         unidad=UNIDAD)


SPN = "Sector Público Nacional no financiero"
APN = "Administración Pública Nacional"
TN = "Tesoro Nacional"
OD = "Organismos descentralizados"
ISS = "Instituciones de la seguridad social"
RA = "Recursos afectados"
EP = "Empresas públicas, PAMI y fondos fiduciarios"
EXCP = "Ex cajas provinciales"

CASOS_AIF = (
    # SPN: la cobertura natural no lleva el subsector en el nombre
    (aif("SPN", "Total ingresos corrientes . Metodología 2017", "2017", "T"),
     vigente(SPN, "Ingresos", "Ingresos corrientes")),
    (aif("SPN", "Ingresos corrientes ingresos tributarios . Metodología 2017", "2017", "M"),
     vigente(SPN, "Ingresos", "Ingresos tributarios")),
    (aif("SPN", "Ingresos corrientes ventas de bs. y serv. de las adm. pub. . Metodología 2017", "2017", "M"),
     vigente(SPN, "Ingresos",
             "Ingresos corrientes · Ventas de bienes y servicios de las administraciones públicas")),
    (aif("SPN", "Ingresos corrientes aportes y contrib. a la seg. social . Metodología 2007 - 2014",
         "2007-2014"),
     historica(SPN, "2007-2014", "Ingresos", "Ingresos corrientes · Aportes y contribuciones a la seguridad social")),
    (aif("SPN", "Gastos corrientes prestaciones de la seguridad social . Metodología 1993 - 2006", "1993-2006"),
     historica(SPN, "1993-2006", "Gastos", "Gastos corrientes · Prestaciones de la seguridad social")),
    (aif("SPN", "Gastos corrientes  intereses y otras rentas de la prop. intereses deuda externa . Metodología 1993 - 2006",
         "1993-2006", "M"),
     historica(SPN, "1993-2006", "Gastos", "Gastos corrientes · Rentas de la propiedad · Intereses de la deuda externa")),
    (aif("SPN", "Gastos corrientes transferencias corrientes al sector público provincias y caba recursos coparticipados . Metodología 1993 - 2006",
         "1993-2006", "M"),
     historica(SPN, "1993-2006", "Gastos",
               "Gastos corrientes · Transferencias al sector público · Provincias y CABA · Recursos coparticipados")),
    (aif("SPN", "Gastos de capital inversion financieraa provincias y caba . Metodología 2007 - 2014", "2007-2014"),
     historica(SPN, "2007-2014", "Gastos", "Gastos de capital · Inversión financiera a provincias y CABA")),
    (aif("SPN", "Gastos corrientes", "1993-2006"),
     historica(SPN, "1993-2006", "Gastos", "Gastos corrientes")),
    (aif("SPN", "Recursos de capital privatizaciones . Metodología 2017", "2017", "M"),
     vigente(SPN, "Ingresos", "Recursos de capital · Privatizaciones")),
    (aif("SPN", "Superavit primario . Metodología 2007 - 2014", "2007-2014", "T"),
     historica(SPN, "2007-2014", "Resultados", "Resultado primario")),
    (aif("SPN", "Superavit primario -sin privatizaciones- . Metodología 2017", "2017", "A"),
     vigente(SPN, "Resultados", "Resultado primario · Sin privatizaciones")),
    # erratas de la fuente: el mismo concepto que sus hermanos
    (aif("SPN", "Resultado financiero sin renta. Metodología 1993 - 2006", "1993-2006"),
     historica(SPN, "1993-2006", "Resultados", "Resultado primario · Sin rentas")),
    (aif("SPN", "Resultado primario sin resnta. Metodología 1993 - 2006", "1993-2006", "M"),
     historica(SPN, "1993-2006", "Resultados", "Resultado primario · Sin rentas")),
    (aif("SPN", "Resultados financieros sin renta 2007 2014", "2007-2014", "M"),
     historica(SPN, "2007-2014", "Resultados", "Resultado primario · Sin rentas")),
    (aif("SPN", "Ingresos corrientes antes de figurativos . Metodología 2007 - 2014", "2007-2014", "T"),
     historica(SPN, "2007-2014", "Ingresos", "Ingresos primarios antes de figurativos")),
    (aif("SPN", "Aplicaciones financieras disminucion del patrimonio . Metodología 2007 - 2014", "2007-2014"),
     historica(SPN, "2007-2014", "Financiamiento", "Aplicaciones financieras · Disminución del patrimonio")),
    # otros subsectores: el subsector va al final del nombre
    (aif("TN", "Gastos corrientes prestaciones de la seguridad social . Metodología 1993 - 2006", "1993-2006"),
     historica(TN, "1993-2006", "Gastos", "Gastos corrientes · Prestaciones de la seguridad social · Tesoro Nacional")),
    (aif("OD", "Resultado financiero . Metodología 2017"),
     vigente(OD, "Resultados", "Resultado financiero · Organismos descentralizados")),
    (aif("APN", "Contribuciones figurativas de pami, fdos. fiduciarios y otros . Metodología 2017"),
     vigente(APN, "Figurativos", "Contribuciones figurativas · PAMI, fondos fiduciarios y otros · Administración Pública Nacional")),
    (aif("APN", "Contribuciones figurativas de empresas públicas y otros . Metodología 1993 - 2006", "1993-2006"),
     historica(APN, "1993-2006", "Figurativos", "Contribuciones figurativas · Empresas públicas y otros · Administración Pública Nacional")),
    (aif("OD", "Contribuciones figurativas de excajas provinciales . Metodología 2007 - 2014", "2007-2014", "M"),
     historica(OD, "2007-2014", "Figurativos", "Contribuciones figurativas · Ex cajas provinciales · Organismos descentralizados")),
    (aif("ISS", "Gastos corrientes  intereses y otras rentas de la prop. total rentas de la propiedad . Metodología 1993 - 2006",
         "1993-2006"),
     historica(ISS, "1993-2006", "Gastos", "Gastos corrientes · Rentas de la propiedad · Instituciones de la seguridad social")),
    (aif("ISS", "Ingresos corrientes rentas de la propiedad rentas de la propiedad netas . Metodología 2017"),
     vigente(ISS, "Ingresos", "Ingresos corrientes · Rentas de la propiedad netas · Instituciones de la seguridad social")),
    (aif("RA", "Gastos primarios despues de figurativos . Metodología 2017"),
     vigente(RA, "Gastos", "Gastos primarios después de figurativos · Recursos afectados")),
    (aif("RA", "Gastos de capital inversion real directa . Metodología 2007 - 2014", "2007-2014", "M"),
     historica(RA, "2007-2014", "Gastos", "Gastos de capital · Inversión real directa · Recursos afectados")),
    (aif("EP_PAMI_FF", "Fuentes financieras endeudamiento público e incremento de otros pasivos endeudamiento en moneda extranjera . Metodología 2017",
         "2017", "T"),
     vigente(EP, "Financiamiento", "Fuentes financieras · Endeudamiento en moneda extranjera · Empresas públicas, PAMI y fondos fiduciarios")),
    (aif("Ex_CP", "Resultado economico: ahorro/desahorro . Metodología 2007 - 2014", "2007-2014", "M"),
     historica(EXCP, "2007-2014", "Resultados", "Resultado económico · Ex cajas provinciales")),
    (aif("SPN", "Fuentes financieras endeudamiento publico e incremento de otros pasivos incremento otros pasivos . Metodología 1993 - 2006", "1993-2006"),
     historica(SPN, "1993-2006", "Financiamiento", "Fuentes financieras · Incremento de otros pasivos")),
    (aif("SPN", "Aplicaciones financieras amort.deudas y dism. otros pasivos amortización deuda externa . Metodología 1993 - 2006", "1993-2006"),
     historica(SPN, "1993-2006", "Financiamiento", "Aplicaciones financieras · Amortización de deuda externa")),
)

CASOS_IMIG = (
    # datos.gob.ar: jerarquía separada por puntos
    (imig_datos("IMIG. Ingresos totales. Ingresos tributarios. Ganancias"),
     imig("Ingresos", "", "Ingresos totales · Ingresos tributarios · Ganancias")),
    (imig_datos("IMIG. Ingresos totales. Ingresos tributarios. Aportes y contribuciones a la seguriad social"),
     imig("Ingresos", "", "Ingresos totales · Ingresos tributarios · Aportes y contribuciones a la seguridad social")),
    (imig_datos("IMIG. Ingresos totales. Ingresos de capital"),
     imig("Ingresos", "", "Ingresos totales · Recursos de capital")),
    (imig_datos("IMIG. Gastos primarios. Gastos corrientes primarios. Prestaciones sociales. Pensiones no contributivas"),
     imig("Gastos", "Corrientes", "Gastos primarios · Prestaciones sociales · Pensiones no contributivas")),
    (imig_datos("IMIG. Gastos primarios. Gastos corrientes primarios. Gastos de funcionamiento. Salarios"),
     imig("Gastos", "Corrientes", "Gastos primarios · Gastos de funcionamiento · Remuneraciones")),
    # las filas trimestrales de algunas series vienen separadas por guiones
    (imig_datos("Gastos primarios - Gastos corrientes primarios - Prestaciones sociales - Jubilaciones y pensiones contributivas", "T"),
     imig("Gastos", "Corrientes", "Gastos primarios · Prestaciones sociales · Jubilaciones y pensiones contributivas")),
    (imig_datos("Ingresos totales - Ingresos tributarios - Combustibles", "T"),
     imig("Ingresos", "", "Ingresos totales · Ingresos tributarios · Combustibles")),
    (imig_datos("IMIG. Gastos primarios. Gastos corrientes primarios. Otros gastos corrientes. Transferencias a universidades"),
     imig("Gastos", "Corrientes", "Gastos primarios · Transferencias a universidades")),
    (imig_datos("IMIG. Gastos de capital. Educación. Transferencias a provincias", "A"),
     imig("Gastos", "De capital", "Gastos primarios · Gastos de capital · Educación · Transferencias a provincias")),
    (imig_datos("IMIG. Gastos de capital. Fondo Federal Solidario"),
     imig("Gastos", "De capital", "Gastos primarios · Gastos de capital · Fondo Federal Solidario")),
    (imig_datos("IMIG. Intereses Netos"), imig("Gastos", "Corrientes", "Intereses netos")),
    (imig_datos("IMIG. Resultado primario con P.I.P. (Meta FMI)"),
     imig("Resultados", "", "Resultado primario con Programa de Inversiones Prioritarias")),
    (imig_datos("IMIG. Programa de Inversiones Prioritarias (P.I.P.)", "A"),
     imig("Partidas de ajuste", "", "Programa de Inversiones Prioritarias")),
    (imig_datos("IMIG. Ingresos totales. Rentas de la propiedad Netas. FGS cobradas al sector privado y público financiero"),
     imig("Ingresos", "", "Ingresos totales · Rentas de la propiedad netas · Rentas del FGS cobradas al sector privado y público financiero")),
    # mecon-imig: los mismos nombres, desde títulos en mayúsculas con guiones
    (imig_mecon("GASTOS PRIMARIOS"), imig("Gastos", "Totales", "Gastos primarios")),
    (imig_mecon("GASTOS PRIMARIOS - Prestaciones sociales - Pensiones no contributivas"),
     imig("Gastos", "Corrientes", "Gastos primarios · Prestaciones sociales · Pensiones no contributivas")),
    (imig_mecon("GASTOS PRIMARIOS - Prestaciones sociales"),
     imig("Gastos", "Corrientes", "Gastos primarios · Prestaciones sociales")),
    (imig_mecon("GASTOS PRIMARIOS - Gastos de funcionamiento y otros - Salarios"),
     imig("Gastos", "Corrientes", "Gastos primarios · Gastos de funcionamiento · Remuneraciones")),
    (imig_mecon("GASTOS PRIMARIOS - Educación - Nación"),
     imig("Gastos", "De capital", "Gastos primarios · Gastos de capital · Educación · Nación")),
    (imig_mecon("GASTOS PRIMARIOS - Fondo Federal Solidario"),
     imig("Gastos", "De capital", "Gastos primarios · Gastos de capital · Fondo Federal Solidario")),
    (imig_mecon("GASTOS PRIMARIOS - Subsidios económicos - Energía"),
     imig("Gastos", "Corrientes", "Gastos primarios · Subsidios económicos · Energía")),
    (imig_mecon("GASTOS PRIMARIOS - Otros - Transferencias a provincias"),
     imig("Gastos", "De capital", "Gastos primarios · Gastos de capital · Otros · Transferencias a provincias")),
    (imig_mecon("GASTOS PRIMARIOS - Otros gastos corrientes - Resultado Operativo de Empresas Públicas"),
     imig("Gastos", "Corrientes", "Gastos primarios · Otros gastos corrientes · Déficit operativo de empresas públicas")),
    (imig_mecon("GASTOS PRIMARIOS - Transferencias a universidades"),
     imig("Gastos", "Corrientes", "Gastos primarios · Transferencias a universidades")),
    (imig_mecon("GASTOS PRIMARIOS - Prestaciones sociales - Otros programas (Progresar, Emergencia Social, otros)"),
     imig("Gastos", "Corrientes", "Gastos primarios · Prestaciones sociales · Otros programas")),
    (imig_mecon("GASTOS PRIMARIOS - Prestaciones sociales - Asignaciones (familiares y por hijo)"),
     imig("Gastos", "Corrientes", "Gastos primarios · Prestaciones sociales · Asignaciones familiares y universales")),
    (imig_mecon("INGRESOS TOTALES - Tributarios - IVA neto de reintegros"),
     imig("Ingresos", "", "Ingresos totales · Ingresos tributarios · IVA neto de reintegros")),
    (imig_mecon("INGRESOS TOTALES - Tributarios - Contribuciones a la seguriad social"),
     imig("Ingresos", "", "Ingresos totales · Ingresos tributarios · Aportes y contribuciones a la seguridad social")),
    (imig_mecon("INGRESOS TOTALES - Otros ingresos corrientes - Resto ingresos corrientes"),
     imig("Ingresos", "", "Ingresos totales · Otros ingresos corrientes · Resto de ingresos corrientes")),
    (imig_mecon("INGRESOS TOTALES - Rentas de la propiedad - Rentas por colocaciones de emisiones primarias"),
     imig("Ingresos", "", "Ingresos totales · Rentas de la propiedad · Rentas por colocaciones de emisiones primarias")),
    (imig_mecon("Intereses"), imig("Gastos", "Corrientes", "Intereses")),
    (imig_mecon("RESULTADO PRIMARIO"), imig("Resultados", "", "Resultado primario")),
    (imig_mecon("RESULTADO PRIMARIO según Programa Facilidades Extendidas"),
     imig("Resultados", "", "Resultado primario según Programa de Facilidades Extendidas")),
    (imig_mecon("Ajustadores acuerdo FMI (1): ejecución del gasto social abarcado y del gasto de capital con fuente externa - Gasto social"),
     imig("Partidas de ajuste", "", "Ajustadores del acuerdo con el FMI · Gasto social")),
    (imig_mecon("Rentas por emisión primaria que excenden límite del PFE"),
     imig("Partidas de ajuste", "",
          "Rentas por emisión primaria que exceden el límite del Programa de Facilidades Extendidas")),
    (imig_mecon("Cumplimiento Medida Cautelar CSJN 1864/22"),
     imig("Partidas de ajuste", "", "Cumplimiento de la medida cautelar de la Corte Suprema 1864/22")),
)


class PruebasFinanzasPublicasCaja(unittest.TestCase):
    def test_casos_de_la_cuenta_aif(self):
        for serie, esperado in CASOS_AIF:
            with self.subTest(hoja=serie.hoja, descripcion=serie.descripcion, titulo=serie.nombre):
                self.assertEqual(clasificar(serie), esperado)

    def test_casos_del_informe_mensual(self):
        for serie, esperado in CASOS_IMIG:
            with self.subTest(fuente=serie.fuente, nombre=serie.nombre, descripcion=serie.descripcion):
                self.assertEqual(clasificar(serie), esperado)

    def test_hay_al_menos_25_casos_y_cubren_todas_las_hojas(self):
        self.assertGreaterEqual(len(CASOS_AIF) + len(CASOS_IMIG), 25)
        self.assertEqual({serie.hoja for serie, _ in CASOS_AIF},
                         {"SPN", "APN", "TN", "OD", "ISS", "RA", "EP_PAMI_FF", "Ex_CP"})
        self.assertEqual({serie.fuente for serie, _ in CASOS_IMIG}, {"datos.gob.ar", "mecon-imig"})

    def test_la_metodologia_vigente_no_va_a_series_historicas(self):
        vigente_ = clasificar(aif("TN", "Resultado financiero . Metodología 2017"))
        cerrada = clasificar(aif("TN", "Resultado financiero . Metodología 2007 - 2014", "2007-2014"))
        self.assertEqual(vigente_.nombre, cerrada.nombre)
        self.assertEqual(vigente_.cobertura, TN)
        self.assertEqual(cerrada.cobertura, HIST)
        self.assertEqual(cerrada.apertura, f"{TN} · metodología 2007-2014")

    def test_la_frecuencia_no_cambia_el_nombre(self):
        nombres = {clasificar(aif("APN", "Gastos antes de figurativos . Metodología 2017", frecuencia=f)).nombre
                   for f in "AMT"}
        self.assertEqual(nombres, {"Gastos antes de figurativos · Administración Pública Nacional"})

    def test_una_misma_partida_de_la_imig_se_llama_igual_en_las_dos_fuentes(self):
        datos = clasificar(imig_datos("IMIG. Gastos primarios. Gastos corrientes primarios. Subsidios económicos. Transporte"))
        mecon = clasificar(imig_mecon("GASTOS PRIMARIOS - Subsidios económicos - Transporte"))
        self.assertEqual(datos.nombre, mecon.nombre)
        self.assertEqual(datos.niveles(), mecon.niveles())

    def test_fuera_de_alcance_devuelve_none(self):
        base = aif("SPN", "Total ingresos corrientes . Metodología 2017")
        for cambio in (
            {"hoja": "Gasto Pol. Ing"}, {"hoja": "AC_Dev_61"}, {"archivo": "otro.xlsx"},
            {"fuente": "indec-ipc"}, {"fuente": "bcra"}, {"hoja": "spn"},
        ):
            with self.subTest(cambio=cambio):
                self.assertIsNone(clasificar(Serie(**{**base.__dict__, **cambio})))

    def test_una_hoja_de_alcance_sin_partida_conocida_queda_sin_regla(self):
        self.assertIsNone(clasificar(aif("SPN", "Una partida que no existe . Metodología 2017")))
        self.assertIsNone(clasificar(imig_mecon("Una partida nueva del informe")))

    def test_el_inventario_publicado_queda_clasificado_sin_colapsos(self):
        """Con todas las filas reales: cobertura total y un nombre distinto por partida en cada
        hoja, frecuencia y metodología (el mismo nombre entre metodologías es deliberado)."""
        try:
            from collections import Counter

            from tools.taxonomia import serie_de
            from tools.taxonomia.finanzas_publicas_caja import _metodologia_cerrada
            from tools.taxonomia.revisar import cargar
            inventario = cargar()
        except (OSError, ValueError, ImportError):
            self.skipTest("no está el inventario publicado")
        propias = inventario[
            (inventario["Código fuente"] == "mecon-imig")
            | ((inventario["Código fuente"] == "datos.gob.ar")
               & (inventario["Archivo origen"] == "finanzas_publicas.xlsx")
               & inventario["Hoja origen"].isin(list(TITULOS) + ["IMIG"]))
        ]
        if propias.empty:
            self.skipTest("el inventario no tiene series de finanzas públicas en base caja")
        sin_regla, cuenta = [], Counter()
        for fila in propias.to_dict("records"):
            serie = serie_de(fila)
            resultado = clasificar(serie)
            if resultado is None:
                sin_regla.append(serie.id)
                continue
            cuenta[(serie.fuente, serie.hoja, serie.frecuencia, _metodologia_cerrada(serie), resultado.nombre)] += 1
        self.assertEqual(sin_regla, [])
        # Dentro de una metodología, dos series de una hoja con el mismo nombre serían un colapso.
        # (mecon-imig renombra sus rótulos con el tiempo: sus variantes de una misma partida sí repiten.)
        colapsos = [clave for clave, n in cuenta.items() if n > 1 and clave[0] == "datos.gob.ar"]
        self.assertEqual(colapsos, [])

    def test_determinista(self):
        for serie, _ in (*CASOS_AIF, *CASOS_IMIG):
            self.assertEqual(clasificar(serie), clasificar(serie))


if __name__ == "__main__":
    unittest.main()
