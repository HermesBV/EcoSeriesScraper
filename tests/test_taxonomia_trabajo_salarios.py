"""Pruebas de las reglas de salarios, empleo registrado y cuenta de generación del ingreso."""

from __future__ import annotations

import unittest

from tools.taxonomia import Clasificacion, Serie
from tools.taxonomia.trabajo_salarios import clasificar

T = "Trabajo e ingresos"
DG = "datos.gob.ar"
XLSX = "empleo_ingresos.xlsx"


def serie(fuente, hoja, variable="", columna="", descripcion="", unidad="", frecuencia="M",
          archivo=XLSX):
    return Serie(fuente=fuente, id=f"{archivo}|{hoja}|{variable or columna}", nombre=columna or variable,
                 variable=variable, descripcion=descripcion, unidad=unidad, frecuencia=frecuencia,
                 archivo=archivo, hoja=hoja, columna=columna, dataset="", estado="VIGENTE")


def datos(hoja, variable, descripcion="", frecuencia="M"):
    return serie(DG, hoja, variable=variable, descripcion=descripcion, frecuencia=frecuencia)


def indec(hoja, columna):
    return serie("indec-salarios", hoja, columna=columna, archivo="salarios.xlsx")


def sipa(hoja, columna):
    return serie("mch-sipa", hoja, columna=columna, archivo="latest.xlsx")


def c(indicador, nombre, cobertura="", apertura="", medida="", unidad=""):
    return Clasificacion(tema=T, indicador=indicador, nombre=nombre, cobertura=cobertura,
                         apertura=apertura, medida=medida, unidad=unidad)


IS = "Índice de salarios"
EMP = "Empleo registrado (SIPA)"
REM = "Remuneraciones del empleo registrado"
PUE = "Puestos de trabajo registrados"
CGI = "Cuenta de generación del ingreso"
SAL = "Salarios industriales"
OBR = "Obreros ocupados y horas trabajadas en la industria"
IDX16 = "Índice oct-2016=100"
AP = "Empleo registrado · Asalariados privados"

CASOS = (
    # --- Índice de salarios (INDEC) -------------------------------------------------------------
    (indec("Cuadro 1", "Total índice de salarios - Total registrado (1) - Número índice - %"),
     c(IS, f"{IS} · Nivel general", "Nivel general", unidad=IDX16)),
    (indec("Cuadro 1", "Sector privado no registrado (2) - Total registrado (1) - Variación porcentual respecto del mes anterior - %"),
     c(IS, f"{IS} · Sector privado no registrado (var. % mensual)", "Sector privado no registrado", unidad="%")),
    (indec("Cuadro 1", "Sector registrado - Sector público - Número índice - %"),
     c(IS, f"{IS} · Sector público", "Sector público", unidad=IDX16)),
    (indec("Cuadro 2", "Total registrado (1) - Variación porcentual respecto a - igual mes del año anterior - %"),
     c(IS, f"{IS} · Sector registrado (var. % interanual)", "Sector registrado", unidad="%")),
    (indec("Cuadro 2", "Total registrado (1) - Variación porcentual respecto a - igual mes del año anterior - % (11)"),
     c(IS, f"{IS} · Nivel general (var. % interanual)", "Nivel general", unidad="%")),
    (indec("Cuadro 2", "Total registrado (1) - Variación porcentual respecto a - diciembre del año anterior - % (10)"),
     c(IS, f"{IS} · Sector privado no registrado (var. % acumulada)", "Sector privado no registrado", unidad="%")),
    (indec("Cuadro 3", "Público nacional (1) - Variación porcentual respecto a - mes anterior - %"),
     c(IS, f"{IS} · Sector público nacional (var. % mensual)", "Sector público nacional", unidad="%")),
    (datos("IS oct16=100", "indice_salarios_registrado_sector_privado"),
     c(IS, f"{IS} · Sector privado registrado", "Sector privado registrado", unidad=IDX16)),
    (datos("IS", "sector_priv_no_regis_sin_mod_decreto"),
     c(IS, f"{IS} · Sector privado no registrado (sin modificaciones del Decreto 381/07)",
       "Series históricas", unidad="Índice abr-2012=100")),
    (datos("IS", "nivel_gral_mod_decreto"),
     c(IS, f"{IS} · Nivel general", "Series históricas", unidad="Índice abr-2012=100")),
    # --- RIPTE: una sola serie, dos fuentes -------------------------------------------------------
    (serie("mch-ripte", "Serie histórica", variable="remuneracion-imponible-promedio-trabajadores-estables"),
     c("RIPTE", "RIPTE", unidad="Pesos")),
    (datos("RIPTE", "ripte"), c("RIPTE", "RIPTE", unidad="Pesos")),
    # --- Empleo registrado: MCH ------------------------------------------------------------------
    (sipa("A.1", "A.1. Personas con trabajo asalariado registrado en el sector privado. Total país1/ - En miles - Serie desestacionalizada 2/"),
     c(EMP, "Empleo registrado · Asalariados privados (desestacionalizado)", "Por modalidad ocupacional")),
    (sipa("T.1", "T.1. Personas con trabajo registrado. Total país - Nivel Indice base 100 = Ene-12 - Serie original"),
     c(EMP, "Empleo registrado · Total", "Por modalidad ocupacional")),
    (sipa("T.2.2", "T.2.2. Personas con trabajo registrado según modalidad ocupacional principal 1/. Sin estacionalidad 2/. Total país. En miles - Empleo en casas particulares"),
     c(EMP, "Empleo registrado · Asalariados de casas particulares (desestacionalizado)", "Por modalidad ocupacional")),
    (sipa("A.2.2", "A.2.2. Personas con empleo asalariado registrado del sector privado, según rama. Sin estacionalidad 1/. Total país 2/. En miles - Total"),
     c(EMP, "Empleo registrado · Asalariados privados (desestacionalizado)", "Por modalidad ocupacional")),
    (sipa("A.2.1", "A.2.1. Personas con empleo asalariado registrado en el sector privado. Con estacionalidad. Total país. En miles 1/. - Agricultura, ganaderÍa, caza y silvicultura"),
     c(EMP, f"{AP} · Agricultura, ganadería, caza y silvicultura", "Por rama de actividad")),
    (sipa("A.3.1", "A.3.1. Personas con empleo asalariado registrado en el sector privado. Con estacionalidad. Total país 1/. Índice base 100 = Ene-09 - Transporte, almacen. y comunicación"),
     c(EMP, f"{AP} · Transporte, almacenamiento y comunicaciones", "Por rama de actividad")),
    (sipa("A.5.2", "A.5.2. Personas con empleo asalariados en el sector privado 1/según provincia. Sin estacionalidad2/. En miles - Cdad. Autónoma de Buenos Aires"),
     c(EMP, f"{AP} · Ciudad Autónoma de Buenos Aires (desestacionalizado)", "Por provincia")),
    (sipa("A.5.1", "A.5.1.Personas con empleo asalariado registrado en el sector privado 1/ según provincia 2/. Con estacionalidad. En miles - CATAMARCA 3/"),
     c(EMP, f"{AP} · Catamarca", "Por provincia")),
    (sipa("A.6.1", "A.6.1. Personas con empleo asalariado registrado en el sector privado 1/ según ramas de actividad 2/. Con estacionalidad. Total país. En miles - Educación"),
     c(EMP, f"{AP} · Enseñanza", "Por rama de actividad")),
    (sipa("A.6.2", "A.6.2. Personas con empleo asalariado registrado del sector privado 1/según ramas de actividad2/. Sin estacionalidad. Total país 3/. En miles - Alimentos y tabaco"),
     c(EMP, f"{AP} · Alimentos y tabaco (desestacionalizado)", "Por agrupación de ramas")),
    # --- Empleo registrado: OEDE ------------------------------------------------------------------
    (datos("OEDE Total", "independientes_monotributo_social_sin_estac"),
     c(EMP, "Empleo registrado · Independientes monotributistas sociales (desestacionalizado)",
       "Por modalidad ocupacional", unidad="Miles de personas")),
    (datos("OEDE Asal prov", "asalariados_priv_sin_estac_tierra_fuego"),
     c(EMP, f"{AP} · Tierra del Fuego (desestacionalizado)", "Por provincia", unidad="Miles de personas")),
    (datos("OEDE Asal rama", "total_est"),
     c(EMP, "Empleo registrado · Asalariados privados", "Por modalidad ocupacional",
       unidad="Miles de personas")),
    (datos("OEDE Asal rama", "construccion_sin_est"),
     c(EMP, f"{AP} · Construcción (desestacionalizado)", "Por rama de actividad",
       unidad="Miles de personas")),
    # --- Remuneraciones -----------------------------------------------------------------------------
    (sipa("A.4", "A.4. Remuneración de las personas con empleo asalariado registrado en el sector privado. Por todo concepto (**), en pesos, a valores corrientes. Total país. - Remuneración mediana - sin estacionalidad"),
     c(REM, "Remuneración bruta mediana (desestacionalizado)", "Total", unidad="Pesos")),
    (datos("OEDE Remuneraciones", "asalariados_priv_remuneracion_promedio"),
     c(REM, "Remuneración bruta promedio", "Total", unidad="Pesos")),
    (datos("OEDE Remuneraciones Rama", "rem_bruta_asal_total"),
     c(REM, "Remuneración bruta promedio", "Total", unidad="Pesos")),
    (datos("OEDE Remuneraciones Rama", "rem_bruta_asal_sin_est_acti_inmo_empresariales_alquiler"),
     c(REM, "Remuneración bruta promedio · Servicios inmobiliarios, empresariales y de alquiler (desestacionalizado)",
       "Por rama de actividad", unidad="Pesos")),
    # --- Puestos de trabajo registrados -----------------------------------------------------------------
    (datos("OEDE Puestos Rama", "comercio_por_mayor_por_menor", frecuencia="T"),
     c(PUE, "Puestos de trabajo registrados · Comercio mayorista, minorista y reparaciones",
       "Por rama de actividad", unidad="Puestos de trabajo")),
    (datos("OEDE Puestos Sector", "empleo_registrado_priv_sin_estac_industria", frecuencia="T"),
     c(PUE, "Puestos de trabajo registrados · Industria manufacturera (desestacionalizado)",
       "Por rama de actividad", unidad="Puestos de trabajo")),
    (datos("OEDE Puestos Sector", "empleo_registrado_priv_servicios", frecuencia="T"),
     c(PUE, "Puestos de trabajo registrados · Servicios", "Por agrupación de ramas",
       unidad="Puestos de trabajo")),
    (datos("OEDE Puestos Sector", "empleo_registrado_priv_total", frecuencia="T"),
     c(PUE, "Puestos de trabajo registrados", "Total", unidad="Puestos de trabajo")),
    # --- Series históricas del INDEC (sectores agrupados, provincias, CGI antiguo) -------------------------
    (datos("RT x Sector", "metalicas_basicas_productos_metalicos", frecuencia="A"),
     c(REM, "Remuneración bruta promedio · Metálicas básicas y productos metálicos, excepto maquinaria y equipo",
       "Series históricas", "Ramas agrupadas", "Remuneración bruta promedio", "Pesos")),
    (datos("CS x Prov", "caba", frecuencia="T"),
     c(REM, "Costo salarial promedio · Ciudad Autónoma de Buenos Aires", "Series históricas",
       "Provincias", "Costo salarial promedio", "Pesos")),
    (datos("RN x Prov", "total_sector_privado", frecuencia="A"),
     c(REM, "Remuneración neta promedio", "Series históricas", "Provincias",
       "Remuneración neta promedio", "Pesos")),
    (datos("Puestos x Sector", "total_sistema", frecuencia="A"),
     c(PUE, "Puestos de trabajo registrados · Total del sistema", "Series históricas",
       "Ramas agrupadas", unidad="Miles de puestos")),
    (datos("Puestos x Prov", "santa_fe", frecuencia="T"),
     c(PUE, "Puestos de trabajo registrados · Santa Fe", "Series históricas", "Provincias",
       unidad="Miles de puestos")),
    (datos("CGI Remun", "extr_c_lig_turba_petro_crudo_gas_nat_act_rel_exc_act_prosp",
           "extracción de carbón y lignito; extracción de turba. extracción de petróleo crudo y gas natural; actividades de servicios relacionadas con la extracción de petróleo y gas, excepto las actividades de prospección.",
           "A"),
     c(REM, "Remuneración bruta promedio · Extracción de carbón, lignito, turba, petróleo crudo y gas natural",
       "Series históricas", "Ramas de la cuenta de generación del ingreso",
       "Remuneración bruta promedio", "Pesos")),
    (datos("CGI Puestos", "total", "total", "A"),
     c(PUE, "Puestos de trabajo registrados", "Series históricas",
       "Ramas de la cuenta de generación del ingreso", unidad="Puestos de trabajo")),
    (datos("CGI Remun netas", "ensenianza_privada", "enseñanza privada", "T"),
     c(REM, "Remuneración neta promedio · Enseñanza privada", "Series históricas",
       "Ramas de la cuenta de generación del ingreso", "Remuneración neta promedio", "Pesos")),
    # --- Cuenta de generación del ingreso -----------------------------------------------------------------------
    (datos("CGI VABpb", "total_general_vab_pb", "TOTAL GENERAL Valor Agregado Bruto pb", "T"),
     c(CGI, "CGI · Valor agregado bruto", "Total de la economía", unidad="Millones de pesos")),
    (datos("CGI VABpb", "l_adm_pub_def_plan_seg_soc_oblig_remun_trabajo_asalariado",
           "L -Administracion publica y defensa; planes de seguridad social de afiliacion obligatoria Remuneracion al trabajo asalariado", "T"),
     c(CGI, "CGI · Remuneración del trabajo asalariado · Administración pública, defensa y seguridad social obligatoria",
       "Por rama de actividad", "Remuneración del trabajo asalariado", unidad="Millones de pesos")),
    (datos("CGI VABpb", "a_agricultura_ganaderia_caza_silvicultura_ingreso_bruto",
           "A - Agricultura, ganaderia, caza y silvicultura Ingreso bruto mixto", "T"),
     c(CGI, "CGI · Ingreso mixto bruto · Agricultura, ganadería, caza y silvicultura",
       "Por rama de actividad", "Ingreso mixto bruto", unidad="Millones de pesos")),
    (datos("CGI-2016", "total_pib_cf", "PIB a costo de factores", "T"),
     c(CGI, "CGI · PIB a costo de factores (% del PIB a costo de factores)", "Total de la economía",
       unidad="Proporción (1 = 100%)")),
    (datos("CGI-2016", "excedente_bruto_explotacion", "", "T"),
     c(CGI, "CGI · Excedente bruto de explotación (% del PIB a costo de factores)", "Total de la economía",
       unidad="Proporción (1 = 100%)")),
    (datos("CGI ManodeObra", "g_comercio_puestos_trab_no_asal", "", "T"),
     c(CGI, "CGI · Puestos de trabajo no asalariados · Comercio mayorista, minorista y reparaciones",
       "Por rama de actividad", "Puestos de trabajo no asalariados", unidad="Miles de puestos")),
    (datos("CGI ManodeObra", "o_otras_act_serv_com_soc_personales_asal_no_reg", "", "T"),
     c(CGI, "CGI · Puestos de trabajo asalariados no registrados · Servicios comunitarios, sociales y personales",
       "Por rama de actividad", "Puestos de trabajo asalariados no registrados", unidad="Miles de puestos")),
    (datos("CGI ManodeObra", "total_general_puestos_trabajo_asalariado_registrado", "", "T"),
     c(CGI, "CGI · Puestos de trabajo asalariados registrados", "Total de la economía",
       unidad="Miles de puestos")),
    # --- Industria ------------------------------------------------------------------------------------------------
    (datos("EIM", "indice_obreros_ocupados", frecuencia="A"),
     c(OBR, "Obreros ocupados en la industria", unidad="Índice 1997=100")),
    (datos("EIM Nuevo", "variaciones_trimestrales_horas_trabajadas", frecuencia="T"),
     c(OBR, "Horas trabajadas en la industria (var. % trimestral)", unidad="%")),
    (datos("EIM", "indice_salario_obrero", frecuencia="T"),
     c(SAL, f"{SAL} · Salario por obrero ocupado", "Industria manufacturera", unidad="Índice 1997=100")),
    (datos("Salario oficial60-75", "mecanicos_transporte_automotor", frecuencia="A"),
     c(SAL, f"{SAL} · Salario básico de convenio del oficial · Mecánicos, transporte y automotor",
       "Series históricas", "Salarios básicos de convenio por rama", "Oficial", "Índice 1960=100")),
    (datos("Salario peon60-75", "promedio", frecuencia="T"),
     c(SAL, f"{SAL} · Salario básico de convenio del peón · Promedio de las ramas",
       "Series históricas", "Salarios básicos de convenio por rama", "Peón", "Índice 1960=100")),
    (datos("SalariosInd-Indices 62-67", "oficiales", frecuencia="A"),
     c(SAL, f"{SAL} · Salario básico de convenio del oficial · Capital Federal", "Series históricas",
       "Convenios de la Capital Federal", unidad="Índice")),
    (datos("SalariosInd-var65-66", "real_sin_basico_convenio", frecuencia="T"),
     c(SAL, f"{SAL} · Salario real sin básico de convenio · Capital Federal (var. %)", "Series históricas",
       "Convenios de la Capital Federal", unidad="%")),
    (datos("Salario real Ind85-90", "variacion_salario_real_b", frecuencia="M"),
     c(SAL, f"{SAL} · Salario hora normal (precios constantes, var. % interanual)", "Series históricas",
       "Salario hora normal", unidad="%")),
    (datos("Salario real Ind85-90", "poder_adquisitivo_base_1986", frecuencia="M"),
     c(SAL, f"{SAL} · Poder adquisitivo del salario hora normal", "Series históricas",
       "Salario hora normal", unidad="Índice 1986=100")),
    (datos("Salario real Ind85-90", "variacion_salario_real_a", frecuencia="M"),
     c(SAL, f"{SAL} · Salario hora normal (precios constantes, var. % mensual)", "Series históricas",
       "Salario hora normal", unidad="%")),
)


class TestTrabajoSalarios(unittest.TestCase):
    def test_casos_representativos(self):
        self.assertGreaterEqual(len(CASOS), 25)
        for entrada, esperado in CASOS:
            with self.subTest(hoja=entrada.hoja, variable=entrada.variable or entrada.columna[-40:]):
                self.assertEqual(clasificar(entrada), esperado)

    def test_misma_serie_mismo_lugar_en_las_dos_fuentes(self):
        # Asalariados privados por modalidad: MCH y OEDE publican la misma serie.
        mch = clasificar(sipa("T.2.1", "T.2.1. Personas con trabajo registrado. Con estacionalidad. En miles - Empleo asalariado en el sector privado"))
        oede = clasificar(datos("OEDE Total", "asalariados_priv_estacionalidad"))
        self.assertEqual((mch.nombre, mch.cobertura), (oede.nombre, oede.cobertura))
        # Una rama de la agrupación del SIPA que coincide con una rama CIIU lleva el nombre de la rama.
        agrupada = clasificar(sipa("A.6.1", "A.6.1. ... Con estacionalidad. Total país. En miles - Construcción"))
        rama = clasificar(sipa("A.2.1", "A.2.1. ... Con estacionalidad. Total país. En miles 1/. - Construcción"))
        self.assertEqual((agrupada.nombre, agrupada.cobertura), (rama.nombre, rama.cobertura))

    def test_correcciones_explicitas(self):
        from tools.taxonomia.trabajo_salarios import EXCLUIDAS, UNIDADES
        # Salario real Ind85-90 anual: siete columnas copiadas de «Salario oficial60-75» (1960-64).
        anuales = {i for (f, i) in EXCLUIDAS if i.startswith("355.1_")}
        self.assertEqual(len(anuales), 7)
        self.assertNotIn((DG, "355.2_REAL_BASE_986__14"), EXCLUIDAS)
        # CGI Puestos anual: 61 series con los primeros trimestres; quedan el total y las tres públicas.
        cgi = {i for (f, i) in EXCLUIDAS if i.startswith("325.1_")}
        self.assertEqual(len(cgi), 61)
        self.assertEqual(len(EXCLUIDAS), 68)
        self.assertTrue(all(f == DG for (f, _) in EXCLUIDAS))
        self.assertTrue(all(motivo.strip() for motivo in EXCLUIDAS.values()))
        # Índice de salarios base 2012: vale 100 en abril de 2012.
        self.assertEqual(len(UNIDADES), 16)
        self.assertEqual(set(UNIDADES.values()), {"Índice abr-2012=100"})

    def test_cgi_antiguo_por_rama_no_comparte_nombre_con_rt_puestos_x_sector(self):
        cgi = clasificar(datos("CGI Puestos", "construccion", "construcción", "T"))
        sector = clasificar(datos("Puestos x Sector", "construccion", frecuencia="T"))
        self.assertNotEqual(cgi.nombre, sector.nombre)
        # Subrama propia de la cuenta: sin marca.
        sub = clasificar(datos("CGI Puestos", "ensenianza_privada", "enseñanza privada", "T"))
        self.assertEqual(sub.nombre, "Puestos de trabajo registrados · Enseñanza privada")
        total_cgi = clasificar(datos("CGI Puestos", "total", "total", "T"))
        total_sector = clasificar(datos("Puestos x Sector", "sector_privado", frecuencia="T"))
        self.assertEqual(total_cgi.nombre, total_sector.nombre)

    def test_fuera_de_alcance(self):
        self.assertIsNone(clasificar(serie("indec-ipc", "Cuadro 1", columna="Nivel general")))
        self.assertIsNone(clasificar(serie(DG, "EPH", variable="tasa_actividad")))
        self.assertIsNone(clasificar(serie(DG, "SMVM", variable="smvm")))
        self.assertIsNone(clasificar(serie(DG, "RIPTE", variable="ripte", archivo="otro.xlsx")))

    def test_es_determinista(self):
        for entrada, _ in CASOS:
            self.assertEqual(clasificar(entrada), clasificar(entrada))


if __name__ == "__main__":
    unittest.main()
