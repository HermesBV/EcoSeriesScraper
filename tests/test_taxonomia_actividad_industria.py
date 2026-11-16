"""Pruebas de las reglas de actividad agregada, industria y construcción (EMAE, IPI, EMI, ISAC…)."""

from __future__ import annotations

import unittest

from tools.taxonomia import Clasificacion, Serie
from tools.taxonomia.actividad_industria import EXCLUIDAS, clasificar

PROPORCION = "Proporción (1 = 100%)"


def indec(fuente: str, archivo: str, hoja: str, columna: str, descripcion: str = "", frecuencia: str = "M",
          id_: str = "") -> Serie:
    return Serie(
        fuente=fuente, id=id_ or f"{archivo}|{hoja}|{columna.lower().replace(' ', '-')}",
        nombre=f"{descripcion} - {columna}", variable=descripcion, descripcion=descripcion, unidad="",
        frecuencia=frecuencia, archivo=archivo, hoja=hoja, columna=columna, dataset=descripcion, estado="VIGENTE",
    )


def datos(hoja: str, variable: str, descripcion: str, id_: str, frecuencia: str = "M", dataset: str = "",
          unidad: str = "") -> Serie:
    return Serie(
        fuente="datos.gob.ar", id=id_, nombre=dataset, variable=variable, descripcion=descripcion, unidad=unidad,
        frecuencia=frecuencia, archivo="actividad.xlsx", hoja=hoja, columna=id_, dataset=dataset, estado="VIGENTE",
    )


def esperado(indicador, nombre, cobertura="", apertura="", medida="", unidad=""):
    return Clasificacion(tema="Actividad", indicador=indicador, nombre=nombre, cobertura=cobertura,
                         apertura=apertura, medida=medida, unidad=unidad)


EMAE_TABLA = "Estimador Mensual de Actividad Económica. Números índice, base 2004=100 y variaciones porcentuales"
IPI_C2 = "Cuadro 2. IPI manufacturero nivel general, divisiones y subclases. Serie original, base 2004=100, en números índice. Años 2016-2026"
IPI_C5 = "Cuadro 5. IPI manufacturero nivel general y divisiones. Serie desestacionalizada, base 2004=100, en números índice y variación porcentual intermensual. Años 2016-2026"
IPI = "IPI manufacturero"
ISAC_C2 = "Cuadro 2.1. Insumos para la construcción. Serie original, base 2004=100, en números índice"

CASOS = (
    # --- EMAE: libro del INDEC
    (indec("indec-emae", "emae_mensual.xls", "Tabla", "Índice Serie Original 2004=100", EMAE_TABLA),
     esperado("EMAE", "EMAE · Nivel general", "Nivel general", unidad="Índice 2004=100")),
    (indec("indec-emae", "emae_mensual.xls", "Tabla", "Índice Serie Desestacionalizada 2004=100", EMAE_TABLA),
     esperado("EMAE", "EMAE · Nivel general (desestacionalizado)", "Nivel general", unidad="Índice 2004=100")),
    (indec("indec-emae", "emae_mensual.xls", "Tabla", "Índice Serie Tendencia-Ciclo 2004=100", EMAE_TABLA),
     esperado("EMAE", "EMAE · Nivel general (tendencia-ciclo)", "Nivel general", unidad="Índice 2004=100")),
    (indec("indec-emae", "emae_mensual.xls", "Tabla", "Var % respecto a igual período del año anterior", EMAE_TABLA),
     esperado("EMAE", "EMAE · Nivel general (var. % interanual)", "Nivel general", unidad="%")),
    (indec("indec-emae", "emae_mensual.xls", "Tabla", "Var % respecto al mes anterior", EMAE_TABLA,
           id_="emae_mensual.xls|Tabla|var-respecto-al-mes-anterior"),
     esperado("EMAE", "EMAE · Nivel general (desestacionalizado, var. % mensual)", "Nivel general", unidad="%")),
    (indec("indec-emae", "emae_mensual.xls", "Tabla", "Var % respecto al mes anterior (2)", EMAE_TABLA,
           id_="emae_mensual.xls|Tabla|var-respecto-al-mes-anterior-2"),
     esperado("EMAE", "EMAE · Nivel general (tendencia-ciclo, var. % mensual)", "Nivel general", unidad="%")),
    (indec("indec-emae", "emae_actividad.xls", "Tabla Letras", "D - Industria manufacturera", EMAE_TABLA),
     esperado("EMAE", "EMAE · Industria manufacturera", "Sectores", unidad="Índice 2004=100")),
    (indec("indec-emae", "emae_actividad.xls", "Tabla Var Letras",
           "L - Administración pública y defensa; planes de seguridad social de afiliación obligatoria"),
     esperado("EMAE", "EMAE · Administración pública y defensa; planes de seguridad social de afiliación obligatoria (var. % interanual)",
              "Sectores", unidad="%")),
    # --- EMAE: datos.gob.ar, mismos grupos y nombres
    (datos("1.4.1 Emae BASE 2004", "emae_desestacionalizada", "EMAE desestacionalizado. Base 2004",
           "143.3_NO_PR_2004_A_31"),
     esperado("EMAE", "EMAE · Nivel general (desestacionalizado)", "Nivel general", unidad="Índice 2004=100")),
    (datos("1.4.1 Emae BASE 2004", "emae_original_via", "EMAE. Base 2004. Variación % interanual",
           "143.3_ICE_SERVIA_2004_A_25"),
     esperado("EMAE", "EMAE · Nivel general (var. % interanual)", "Nivel general", unidad=PROPORCION)),
    (datos("1.4.2 Emae BASE 2004 - Sectores", "ensenianza", "EMAE. Enseñanza", "11.3_CMMR_2004_M_10"),
     esperado("EMAE", "EMAE · Enseñanza", "Sectores", unidad="Índice 2004=100")),
    (datos("1.4.2 Emae BASE 2004 - Sectores", "impuestos_netos_subsidios", "EMAE. Subsidios netos", "11.3_IF_2004_M_25"),
     esperado("EMAE", "EMAE · Impuestos netos de subsidios", "Sectores", unidad="Índice 2004=100")),
    (datos("1.4 Emae BASE 1993", "var_mes_anterior_tendencia_suavizada",
           "Variación % del EMAE respecto mes anterior (para Mensuales), tendencia suavizada", "10.3_VMATS_1993_M_36"),
     esperado("EMAE", "EMAE · Nivel general (tendencia-ciclo, var. % mensual)", "Series históricas", "Base 1993",
              unidad="%")),
    (datos("1.4 Emae BASE 1993", "indice_serie_original_mensual", "Serie original EMAE en Mensual. Índice (base 1993)",
           "10.3_ISOM_1993_M_29"),
     esperado("EMAE", "EMAE · Nivel general", "Series históricas", "Base 1993", unidad="Índice 1993=100")),
    # --- IPI manufacturero: libro del INDEC
    (indec("indec-ipi-manufacturero", "ipi_manufacturero.xls", "Cuadro 1",
           "Serie desestacionalizada (1) - Variación porcentual respecto al mes anterior - acumulada del año respecto a igual acumulado del año anterior - %"),
     esperado(IPI, "IPI manufacturero · Nivel general (desestacionalizado, var. % mensual)", "Nivel general", unidad="%")),
    (indec("indec-ipi-manufacturero", "ipi_manufacturero.xls", "Cuadro 1",
           "Serie original - Variación porcentual - acumulada del año respecto a igual acumulado del año anterior - %"),
     esperado(IPI, "IPI manufacturero · Nivel general (var. % acumulada)", "Nivel general", unidad="%")),
    (indec("indec-ipi-manufacturero", "ipi_manufacturero.xls", "Cuadro 2", "D - IPI Manufacturero", IPI_C2),
     esperado(IPI, "IPI manufacturero · Nivel general", "Nivel general", unidad="Índice 2004=100")),
    (indec("indec-ipi-manufacturero", "ipi_manufacturero.xls", "Cuadro 2", "18-19 - Prendas de vestir, cuero y calzado", IPI_C2),
     esperado(IPI, "IPI manufacturero · Prendas de vestir, cuero y calzado", "Divisiones", unidad="Índice 2004=100")),
    (indec("indec-ipi-manufacturero", "ipi_manufacturero.xls", "Cuadro 2", "19201/2/3/4 - Calzado y sus partes", IPI_C2),
     esperado(IPI, "IPI manufacturero · Calzado y sus partes", "Subclases", "Prendas de vestir, cuero y calzado",
              unidad="Índice 2004=100")),
    (indec("indec-ipi-manufacturero", "ipi_manufacturero.xls", "Cuadro 2",
           "35110/120/200/300/990 - Otro equipo de transporte", IPI_C2),
     esperado(IPI, "IPI manufacturero · Otro equipo de transporte, excepto motocicletas", "Subclases",
              "Otro equipo de transporte", unidad="Índice 2004=100")),
    (indec("indec-ipi-manufacturero", "ipi_manufacturero.xls", "Cuadro 3",
           "2411, excepto 24111 - Productos químicos básicos - %"),
     esperado(IPI, "IPI manufacturero · Productos químicos básicos (var. % interanual)", "Subclases",
              "Sustancias y productos químicos", unidad="%")),
    (indec("indec-ipi-manufacturero", "ipi_manufacturero.xls", "Cuadro 4",
           "15510/29/30/41/42/49 - Gaseosas, aguas, sodas, cervezas, jugos para diluir, sidras y bebidas espirituosas - %"),
     esperado(IPI, "IPI manufacturero · Gaseosas, aguas, sodas, cervezas, jugos, sidras y bebidas espirituosas (var. % acumulada)",
              "Subclases", "Alimentos y bebidas", unidad="%")),
    (indec("indec-ipi-manufacturero", "ipi_manufacturero.xls", "Cuadro 5", "34 - Variación porcentual respecto al mes anterior - %", IPI_C5),
     esperado(IPI, "IPI manufacturero · Vehículos automotores, carrocerías, remolques y autopartes (desestacionalizado, var. % mensual)",
              "Divisiones", unidad="%")),
    (indec("indec-ipi-manufacturero", "ipi_manufacturero.xls", "Cuadro 6", "15 - Alimentos y bebidas - %"),
     esperado(IPI, "IPI manufacturero · Alimentos y bebidas (tendencia-ciclo)", "Divisiones", unidad="Índice 2004=100")),
    (indec("indec-ipi-manufacturero", "ipi_manufacturero.xls", "Cuadro 6", "D - Variación porcentual respecto al mes anterior - %"),
     esperado(IPI, "IPI manufacturero · Nivel general (tendencia-ciclo, var. % mensual)", "Nivel general", unidad="%")),
    (indec("indec-ipi-manufacturero", "ipi_manufacturero.xls", "Cuadro 7", "Serie desestacionalizada - Nivel general - Números índice"),
     esperado(IPI, "IPI manufacturero · Nivel general (desestacionalizado, método directo)", "Nivel general",
              unidad="Índice 2004=100")),
    (indec("indec-ipi-manufacturero", "ipi_manufacturero.xls", "Cuadro 2", IPI_C2 + " - Código (CLaNAE 2004) - Período"),
     esperado(IPI, "IPI manufacturero · Número de período", "Auxiliares")),
    # --- IPI manufacturero: datos.gob.ar
    (datos("1.14.d IPI Manuf. - NG", "serie_tendencia_ciclo", "IPI Nivel General Serie Tendencia Ciclo",
           "453.1_SERIE_TENDCLO_0_0_21_61"),
     esperado(IPI, "IPI manufacturero · Nivel general (tendencia-ciclo)", "Nivel general", unidad="Índice 2004=100")),
    (datos("1.14.d IPI Manuf. - sectores", "otro_equipo_transporte", "IPI Divisiones y Subclases Resto equipo de transporte",
           "453.2_OTRO_EQUIPRTE_0_0_22_9"),
     esperado(IPI, "IPI manufacturero · Otro equipo de transporte, excepto motocicletas", "Subclases",
              "Otro equipo de transporte", unidad="Índice 2004=100")),
    (datos("1.14.d IPI Manuf. - sectores", "otro_equipo_de_transporte", "IPI Divisiones y Subclases Otro equipo de transporte",
           "453.2_OTRO_EQUIPRTE_0_0_25_29"),
     esperado(IPI, "IPI manufacturero · Otro equipo de transporte", "Divisiones", unidad="Índice 2004=100")),
    (datos("1.14.d IPI Manuf. - sectores", "equipos_informatica", "IPI Divisiones y Subclases Equipos y aparatos de informática",
           "453.2_EQUIPOS_INCOS_0_0_70_37"),
     esperado(IPI, "IPI manufacturero · Equipos de informática, televisión y comunicaciones, y componentes electrónicos",
              "Subclases", "Otros equipos, aparatos e instrumentos", unidad="Índice 2004=100")),
    # --- EMI: antecesor del IPI, en «Series históricas»
    (datos("1.14 Emi", "emi_desestac", "Estimados Mensual Industrial desestacionalizado. Índice (Base 2004)", "12.3_ED_2004_M_12"),
     esperado(IPI, "IPI manufacturero · Nivel general (desestacionalizado)", "Series históricas", "EMI base 2004",
              "Nivel general", "Índice 2004=100")),
    (datos("1.14b Emi 2012", "metalmecanica_exc_automotor", "EMI. Metalmecánica (excluyendo automotores). Índice (Base 2012)",
           "13.3_MEA_M_2012_0_27"),
     esperado(IPI, "IPI manufacturero · Industria metalmecánica, excluida la automotriz", "Series históricas",
              "EMI base 2012", "Sectores", "Índice 2012=100")),
    (datos("1.14c Emi 2004", "minerales_metalicos", "EMI. Minerales metálicos. Variación Interanual (Base 2004), Mensuales",
           "14.2_MM_M_2004_0_19"),
     esperado(IPI, "IPI manufacturero · Productos minerales no metálicos (var. % interanual)", "Series históricas",
              "EMI base 2004", "Sectores", PROPORCION)),
    # --- Capacidad instalada
    (datos("1.16c UCII 2004", "ucii_nivel_general", "Utilización de la capacidad instalada en la industria como porcentaje del total",
           "31.3_UNG_2004_M_18"),
     esperado("Utilización de la capacidad instalada", "Utilización de la capacidad instalada · Nivel general",
              "Nivel general", unidad="%")),
    (datos("1.16c UCII 2004", "ucii_vehiculosautomotores", "Utilización de la capacidad instalada en la industria de Vehículos automotores",
           "31.3_UV_2004_M_25"),
     esperado("Utilización de la capacidad instalada", "Utilización de la capacidad instalada · Vehículos automotores",
              "Sectores", unidad="%")),
    (datos("1.16 UCII", "ucii_industrias_metalicas_basicas", "Utilización de la capacidad instalada en la industria de metales básicos",
           "29.3_UIMB_2006_M_33"),
     esperado("Utilización de la capacidad instalada", "Utilización de la capacidad instalada · Industrias metálicas básicas",
              "Series históricas", "Base 2006", "Sectores", "%")),
    (datos("1.16b UCII 2012", "ucii_nivel_general", "Utilización de la capacidad instalada en la industria como porcentaje del total",
           "30.3_UNG_2012_M_18"),
     esperado("Utilización de la capacidad instalada", "Utilización de la capacidad instalada · Nivel general",
              "Series históricas", "Base 2012", "Nivel general", "%")),
    # --- Encuesta industrial
    (datos("1.15 Enc. industrial", "nivel_general", "Índice de Volumen Físico (IVF). Índice General. Anuales",
           "15.1_NG_1997_A_13", "A", "Índice de Volumen Físico (IVF)"),
     esperado("Encuesta industrial", "Encuesta industrial · Volumen físico · Nivel general", "Volumen físico",
              unidad="Índice 1997=100")),
    (datos("1.15 Enc. industrial", "prod_madera_corcho_excepto_muebles_paja_trenzables",
           "Índice de Obreros Ocupados (IOO). Producción y fabricación de productos d emadera y corcho expto muebles y articulos de materiales trenzables. Anuales",
           "16.1_PMCEM_1997_A_17", "A", "Índices de Obreros Ocupados (IOO)"),
     esperado("Encuesta industrial",
              "Encuesta industrial · Obreros ocupados · Madera y corcho, excepto muebles; artículos de paja",
              "Obreros ocupados", unidad="Índice 1997=100")),
    (datos("1.15 Enc. industrial", "fabricacion_maquinaria_aparatos_electronicos",
           "Índice de Salarios por Obrero (ISO). Fabricación maquinaria y aparatos electronicos. Trimestrales",
           "18.2_FMAE_1997_T_13", "T", "Índice de Salario por Obrero (ISO)"),
     esperado("Encuesta industrial", "Encuesta industrial · Salarios por obrero · Maquinaria y aparatos eléctricos",
              "Salarios por obrero", unidad="Índice 1997=100")),
    (datos("1.15b Enc. industrial Rev.", "nivel_general",
           "Índice de Salarios por Obrero (ISO) (revisado). Índice General. Variaciones interanuales, Trimestrales.",
           "19.1_NG_1997_0_13", "T", "Índices de Obreros Ocupados (IOO) (Revisado)"),
     esperado("Encuesta industrial", "Encuesta industrial · Obreros ocupados · Nivel general (var. % interanual)",
              "Obreros ocupados", unidad=PROPORCION)),
    (datos("1.15b Enc. industrial Rev.", "fabricacion_equipos_aparatos_radio_television_comunicaciones",
           "Índice de Horas Trabajadas (IHT) (revisado). Fabricación equipos y aparatos (radios, televisión y equipos de comunicación) (revisado). Variaciones trimestrales, Trimestrales.",
           "20.2_FEAR_1997_0_13", "T", "Índices de Horas trabajadas (IHT) (Revisado)"),
     esperado("Encuesta industrial",
              "Encuesta industrial · Horas trabajadas · Equipos de radio, televisión y comunicaciones (var. % trimestral)",
              "Horas trabajadas", unidad=PROPORCION)),
    # --- Ventas y productos
    (datos("1.13 Ventas", "automot_nac_imp_total", "Ventas al mercado interno de automotores nacionales e importados total de unidades",
           "38.3_ANIT_1994_M_21"),
     esperado("Ventas al mercado interno", "Ventas al mercado interno · Automotores nacionales e importados",
              "Automotores", unidad="Unidades")),
    (datos("1.13 Ventas", "gasoil", "Ventas de gasoil de Producción Nacional en mestros cúbicos", "38.3_G_1994_M_6"),
     esperado("Ventas al mercado interno", "Ventas al mercado interno · Gasoil", "Derivados del petróleo",
              unidad="Miles de metros cúbicos")),
    (datos("1.13 Ventas", "cigarrillos_3er_cuartil",
           "Ventas al mercado interno de Producción Nacional de Cigarrillos en millones de paquetes del 3er cuartil",
           "38.3_CIGARRILLOTIL_1994_M_23_25"),
     esperado("Ventas al mercado interno", "Ventas al mercado interno · Cigarrillos, 3.er cuartil",
              "Alimentos, bebidas y tabaco", unidad="Millones de paquetes")),
    (datos("1.17 Productos ind.", "mercado_externo", "Mercado externo en Miles de Hectolitros", "41.3_ME_0_A_15"),
     esperado("Producción industrial por producto",
              "Producción industrial por producto · Despachos autorizados de vino en el mercado externo",
              "Alimentos, bebidas y tabaco")),
    (datos("1.17 Productos ind.", "petroleo_crudo", "Petróleo Crudo en Miles de M3", "41.3_PC_0_A_14"),
     esperado("Producción industrial por producto", "Producción industrial por producto · Petróleo crudo",
              "Petróleo, gas y derivados", unidad="Miles de metros cúbicos")),
    (datos("1.17 Productos ind.", "gas_natural", "Gas Natural en Millones de M3", "41.3_GN_0_A_11"),
     esperado("Producción industrial por producto", "Producción industrial por producto · Gas natural",
              "Petróleo, gas y derivados", unidad="Millones de metros cúbicos")),
    (datos("1.17 Productos ind.", "planos", "Planos en Miles de Toneladas", "41.3_P_0_A_6"),
     esperado("Producción industrial por producto", "Producción industrial por producto · Laminados en caliente planos",
              "Siderurgia y metales")),
    # --- ISAC: libro del INDEC
    (indec("indec-isac", "isac.xls", "Cuadro 1", "Serie original - ISAC nivel general - Números índice"),
     esperado("ISAC", "ISAC · Nivel general", "Nivel general", unidad="Índice 2004=100")),
    (indec("indec-isac", "isac.xls", "Cuadro 1",
           "Serie tendencia-ciclo - Variación porcentual respecto al mes anterior - acumulada del año respecto a igual acumulado del año anterior - %"),
     esperado("ISAC", "ISAC · Nivel general (tendencia-ciclo, var. % mensual)", "Nivel general", unidad="%")),
    (indec("indec-isac", "isac.xls", "Cuadro 2.1", "Cemento portland", ISAC_C2),
     esperado("ISAC", "ISAC · Insumos · Cemento portland", "Insumos", "Cemento portland", unidad="Índice 2004=100")),
    (indec("indec-isac", "isac.xls", "Cuadro 2.2", "Resto (1) - %"),
     esperado("ISAC", "ISAC · Insumos · Resto (var. % interanual)", "Insumos", "Resto", unidad="%")),
    (indec("indec-isac", "isac.xls", "Cuadro 3.2", "Yeso - %"),
     esperado("ISAC", "ISAC · Insumos · Yeso (desestacionalizado, var. % mensual)", "Insumos", "Yeso", unidad="%")),
    (indec("indec-isac", "isac.xls", "Cuadro 5", "Variación porcentual - respecto al mismo mes del año anterior"),
     esperado("ISAC", "ISAC · Puestos de trabajo (var. % interanual)", "Puestos de trabajo", unidad="%")),
    (indec("indec-isac", "isac.xls", "Cuadro 6.1", "Superficie autorizada - m2",
           "Cuadro 6.1. Superficie autorizada por los permisos de edificación de 246 municipios, en metros cuadrados"),
     esperado("ISAC", "ISAC · Permisos de edificación · Superficie autorizada", "Permisos de edificación",
              unidad="Metros cuadrados")),
    (indec("indec-isac", "isac.xls", "Cuadro 6,2",
           "Permisos otorgados - acumulada del año respecto a igual acumulado del año anterior - cantidad",
           "Cuadro 6.2. Superficie autorizada por los permisos de edificación de 176 municipios, en metros cuadrados"),
     esperado("ISAC", "ISAC · Permisos de edificación · Permisos otorgados", "Series históricas",
              "Permisos de edificación de 176 municipios", unidad="Permisos")),
    (indec("indec-isac", "isac.xls", "Cuadro 6.3",
           "Variación porcentual - acumulada del año respecto a igual acumulado del año anterior - m2",
           "Cuadro 6.3. Superficie autorizada por los permisos de edificación de 60 municipios, en metros cuadrados"),
     esperado("ISAC", "ISAC · Permisos de edificación · Superficie autorizada (var. % acumulada)", "Series históricas",
              "Permisos de edificación de 60 municipios", unidad="%")),
    # --- ISAC: datos.gob.ar
    (datos("1.18b ISAC rev.", "isac_variacion_interanual",
           "Indicador sintético de la actividad de la construcción. Nivel General. Variación interanual", "33.2_I_2004_M_4"),
     esperado("ISAC", "ISAC · Nivel general (var. % interanual)", "Nivel general", unidad=PROPORCION)),
    (datos("1.18b ISAC rev.", "isac_cemento_portland",
           "Indicador sintético de la actividad de la construcción. Insumos. Cemento portland. Serie tendencia ciclo",
           "33.5_ISAC_CEMENAND_0_0_21_37"),
     esperado("ISAC", "ISAC · Insumos · Cemento portland (tendencia-ciclo)", "Insumos", "Cemento portland",
              unidad="Índice 2004=100")),
    (datos("1.18 ISAC", "isac_obras_viales",
           "Indicador sintético de la actividad de la construcción de obras viales", "32.3_IOV_2004_M_17"),
     esperado("ISAC", "ISAC · Obras viales", "Series históricas", "Serie hasta 2015 (base 2004)", "Tipos de obra",
              "Índice 2004=100")),
    (datos("1.18 ISAC", "isac_sin_estacionalidad",
           "Indicador sintético de la actividad de la construcción sin estacionalidad", "32.3_ISE_2004_M_23"),
     esperado("ISAC", "ISAC · Nivel general (desestacionalizado)", "Series históricas", "Serie hasta 2015 (base 2004)",
              "Nivel general", "Índice 2004=100")),
)


class ActividadIndustriaTest(unittest.TestCase):
    def test_casos_representativos(self):
        for serie, esperada in CASOS:
            with self.subTest(hoja=serie.hoja, columna=serie.columna, variable=serie.variable):
                self.assertEqual(clasificar(serie), esperada)

    def test_hay_al_menos_25_casos_que_cubren_todo_el_alcance(self):
        self.assertGreaterEqual(len(CASOS), 25)
        hojas = {serie.hoja for serie, _ in CASOS}
        for hoja in ("Tabla", "Tabla Letras", "Tabla Var Letras", "Cuadro 1", "Cuadro 7", "Cuadro 6.1", "Cuadro 6,2",
                     "1.4 Emae BASE 1993", "1.4.1 Emae BASE 2004", "1.4.2 Emae BASE 2004 - Sectores", "1.13 Ventas",
                     "1.14 Emi", "1.14.d IPI Manuf. - NG", "1.14.d IPI Manuf. - sectores", "1.14b Emi 2012",
                     "1.14c Emi 2004", "1.15 Enc. industrial", "1.15b Enc. industrial Rev.", "1.16 UCII",
                     "1.16b UCII 2012", "1.16c UCII 2004", "1.17 Productos ind.", "1.18 ISAC", "1.18b ISAC rev."):
            self.assertIn(hoja, hojas)

    def test_mismo_nombre_y_grupo_entre_el_libro_del_indec_y_datos_gob_ar(self):
        indec_ng = indec("indec-emae", "emae_mensual.xls", "Tabla", "Índice Serie Desestacionalizada 2004=100")
        datos_ng = datos("1.4.1 Emae BASE 2004", "emae_desestacionalizada", "EMAE desestacionalizado. Base 2004",
                         "143.3_NO_PR_2004_A_31")
        self.assertEqual(clasificar(indec_ng), clasificar(datos_ng))
        indec_div = indec("indec-ipi-manufacturero", "ipi_manufacturero.xls", "Cuadro 2", "26 - Productos minerales no metálicos")
        datos_div = datos("1.14.d IPI Manuf. - sectores", "productos_minerales_no_metalicos",
                          "IPI Divisiones y Subclases Productos minerales no metálicos", "453.2_PRODUCTOS_COS_0_0_32_70")
        self.assertEqual(clasificar(indec_div), clasificar(datos_div))
        indec_isac = indec("indec-isac", "isac.xls", "Cuadro 2.1", "Hormigón elaborado")
        datos_isac = datos("1.18b ISAC rev.", "isac_hormigon_elaborado",
                           "Indicador sintético de la actividad de la construcción. Insumos. Hormigón elaborado",
                           "33.3_ISAC_HORMIADO_0_0_23_36")
        self.assertEqual(clasificar(indec_isac), clasificar(datos_isac))

    def test_serie_fuera_de_alcance(self):
        ajena = indec("indec-supermercados", "supermercados.xlsx", "Cuadro 1", "Ventas totales - Miles de pesos")
        self.assertIsNone(clasificar(ajena))
        otra_hoja = datos("1.1 OyD real", "pib", "PIB", "9.1_PIB_0_A_1")
        self.assertIsNone(clasificar(otra_hoja))
        otro_archivo = Serie(fuente="datos.gob.ar", id="x", nombre="x", variable="nivel_general", descripcion="",
                             unidad="", frecuencia="M", archivo="otro.xlsx", hoja="1.15 Enc. industrial", columna="x",
                             dataset="", estado="VIGENTE")
        self.assertIsNone(clasificar(otro_archivo))

    def test_exclusiones_fijadas(self):
        auxiliares = {clave for clave in EXCLUIDAS if clave[0] == "indec-ipi-manufacturero"}
        self.assertEqual({c[1].split("|")[1] for c in auxiliares},
                         {"Cuadro 2", "Cuadro 3", "Cuadro 4", "Cuadro 5", "Cuadro 6"})
        for _, id_ in auxiliares:
            self.assertTrue(id_.endswith("periodo") or "anos-2016-2026" in id_)
        productos = {clave[1] for clave in EXCLUIDAS if clave[0] == "datos.gob.ar"}
        self.assertEqual(productos, {
            "41.1_PPSOU_0_M_33", "41.2_PPSOU_0_T_33", "41.3_PPSOU_0_A_33",
            "41.1_CP_0_M_16", "41.2_CP_0_T_16", "41.3_CP_0_A_16",
            "41.3_NS_0_A_11", "41.2_NS_0_T_11", "41.3_NU_0_A_11", "41.2_NU_0_T_11",
            "41.1_NC_0_M_11", "41.2_NC_0_T_11", "41.3_NC_0_A_11"})
        self.assertEqual(len(EXCLUIDAS), 18)
        for motivo in EXCLUIDAS.values():
            self.assertTrue(motivo.strip())

    def test_cuadro_5_el_rotulo_con_porcentaje_es_el_indice(self):
        # «15 - Alimentos y bebidas - %» es el índice (valores ~140); «… Variación porcentual… - %», la variación.
        indice_ = clasificar(indec("indec-ipi-manufacturero", "ipi_manufacturero.xls", "Cuadro 5",
                                   "15 - Alimentos y bebidas - %", IPI_C5))
        self.assertEqual(indice_.unidad, "Índice 2004=100")
        self.assertNotIn("var.", indice_.nombre)

    def test_dos_llamadas_dan_lo_mismo(self):
        for serie, _ in CASOS:
            self.assertEqual(clasificar(serie), clasificar(serie))

    def test_ningun_nombre_pasa_el_limite(self):
        for serie, esperada in CASOS:
            self.assertLessEqual(len(esperada.nombre), 120)


if __name__ == "__main__":
    unittest.main()
