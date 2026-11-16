"""Pruebas de las reglas de servicios, comercio, agro, energía y confianza de ``actividad.xlsx``."""

from __future__ import annotations

import unittest

from tools.taxonomia import Clasificacion, Serie
from tools.taxonomia.actividad_otros import EXCLUIDAS, clasificar


def serie(hoja: str, variable: str, descripcion: str = "", frecuencia: str = "M", id_: str = "",
          fuente: str = "datos.gob.ar", archivo: str = "actividad.xlsx") -> Serie:
    return Serie(
        fuente=fuente, id=id_ or f"{hoja}|{variable}|{frecuencia}", nombre="Título del cuadro",
        variable=variable, descripcion=descripcion, unidad="", frecuencia=frecuencia, archivo=archivo,
        hoja=hoja, columna=id_ or variable, dataset="Título del cuadro", estado="VIGENTE",
    )


def esperado(indicador, nombre, cobertura="", apertura="", unidad="") -> Clasificacion:
    return Clasificacion(tema="Actividad", indicador=indicador, nombre=nombre, cobertura=cobertura,
                         apertura=apertura, unidad=unidad)


SP = "Servicios públicos"
SUPER = "Ventas en supermercados"
MAYO = "Ventas en autoservicios mayoristas"
CC = "Ventas en centros de compras"

CASOS = (
    # --- Servicios públicos, base 2017 (1.10.1)
    ("1.10.1 SSPP 2017", "s_original_nivel_gral", "Serie original. Nivel General",
     esperado(SP, "Servicios públicos · Nivel general", "Nivel general", "", "Índice 2004=100")),
    ("1.10.1 SSPP 2017", "s_desest_peajes", "Serie desestacionalizada. Peajes",
     esperado(SP, "Servicios públicos · Peajes (desestacionalizado)", "Por servicio", "Peajes", "Índice 2004=100")),
    ("1.10.1 SSPP 2017", "s_tendencia_ciclo_transp_carga", "Serie tendencia ciclo. Transporte de carga",
     esperado(SP, "Servicios públicos · Transporte de carga (tendencia-ciclo)", "Por servicio",
              "Transporte de carga", "Índice 2004=100")),
    ("1.10.1 SSPP 2017", "s_original_recoleccion_residuos", "Serie original. Recoleccion de residuos",
     esperado(SP, "Servicios públicos · Recolección de residuos", "Por servicio", "Recolección de residuos",
              "Índice 2004=100")),
    ("1.10.1 SSPP 2017", "peajes_accesos_caba_u", "Peajes. Accesos CABA en unidades",
     esperado(SP, "Servicios públicos · Peajes · Accesos a la Ciudad Autónoma de Buenos Aires", "Por servicio",
              "Peajes", "Vehículos")),
    ("1.10.1 SSPP 2017", "peajes_accesos_caba_uteqs", "Peajes. Accesos a CABA en uteqs",
     esperado(SP, "Servicios públicos · Peajes · Accesos a la Ciudad Autónoma de Buenos Aires (UTEq)",
              "Por servicio", "Peajes", "UTEq")),
    ("1.10.1 SSPP 2017", "serv_telef_movil_datos_accesos_bamtbytes", "Servicio de telefonía móvil. Datos y accesos en bambytes",
     esperado(SP, "Servicios públicos · Telefonía móvil · Tráfico de datos y accesos", "Por servicio", "Telefonía",
              "Terabytes")),
    # En la frecuencia mensual la descripción de estas dos series está desordenada: manda la variable.
    ("1.10.1 SSPP 2017", "serv_telef_fija_lineas_activas_minutos", "Servicio de telefonía fija. Líneas activas. Tráfico de minutos total",
     esperado(SP, "Servicios públicos · Telefonía fija · Tráfico de minutos", "Por servicio", "Telefonía",
              "Miles de minutos")),
    ("1.10.1 SSPP 2017", "transp_pasaj_subte_premetromiles_pasaj", "Tranporte de pasajeros. Subterraneo y premetro en miles de pasajeros",
     esperado(SP, "Servicios públicos · Transporte de pasajeros · Subterráneo y premetro", "Por servicio",
              "Transporte de pasajeros", "Miles de pasajeros")),
    ("1.10.1 SSPP 2017", "residuos_residuos_solidos_ingresados_tn", "Residuos solidos ingresados en m3",
     esperado(SP, "Servicios públicos · Recolección de residuos · Residuos sólidos ingresados", "Por servicio",
              "Recolección de residuos", "Toneladas")),
    # --- Servicios públicos, base 1993 (1.10)
    ("1.10 SS PP", "nivel_general_desestacionalizado", "Indicador sintético de servicios públicos, nivel general desestacionalizado. Base 2004=100",
     esperado(SP, "Servicios públicos · Nivel general (desestacionalizado)", "Series históricas", "Base 1993",
              "Índice 2004=100")),
    ("1.10 SS PP", "transporte_cargas_base_original", "Indicador sintético transporte de cargas. Base 2004=100",
     esperado(SP, "Servicios públicos · Transporte de carga", "Series históricas", "Base 1993", "Índice 2004=100")),
    ("1.10 SS PP", "serv_internacional_minutos_salida_2002", "Servicio telefónico internacional desde 2002 en miles de minutos de salida",
     esperado(SP, "Servicios públicos · Telefonía fija · Minutos internacionales salientes", "Series históricas",
              "Base 1993", "Miles de minutos")),
    ("1.10 SS PP", "serv_telefonico_internac_minutos_salidos", "Servicio telefónico internacional en miles de minutos salidos",
     esperado(SP, "Servicios públicos · Telefonía fija · Minutos internacionales salientes (serie hasta 2003)",
              "Series históricas", "Base 1993 (hasta 2003)", "Miles de minutos")),
    ("1.10 SS PP", "cabotaje_nacional_pasaj_serv_aeronavegacion_comercial", "Transporte de pasajeros dentro del servicio de cabotaje nacional de aeronavegacion comercial en miles de pasajes",
     esperado(SP, "Servicios públicos · Transporte de pasajeros · Servicio aéreo regular de cabotaje",
              "Series históricas", "Base 1993", "Pasajes")),
    ("1.10 SS PP", "pasaj_serv_subterraneos_metrovias_sa", "Transporte de pasajeros de servicios Subterráneos de Metrovías SA en miles de pasajes",
     esperado(SP, "Servicios públicos · Transporte de pasajeros · Subterráneo y premetro", "Series históricas",
              "Base 1993", "Miles de pasajes")),
    # --- Supermercados
    ("1.11 Super", "evolucion_mensual_ventas_grupos_articulostotal_2008", "Evolución mensual de las ventas por grupos de artículos. Total en miles de pesos (base 2008)",
     esperado(SUPER, "Ventas en supermercados · Total", "Series históricas", "Base 1996", "Miles de pesos")),
    ("1.11 Super", "evol_mensual_ventas_articulos_electronicos_articulos_hogar", "Evolución mensual de las ventas de artículos electrónicos para el hogar en miles de pesos",
     esperado(SUPER, "Ventas en supermercados · Electrónicos y artículos para el hogar", "Series históricas",
              "Base 1996", "Miles de pesos")),
    ("1.11.1 Super", "ventas_precios_corrientes", "Ventas supermercados. Ventas a precios corrientes",
     esperado(SUPER, "Ventas en supermercados · Total", "Total", "", "Millones de pesos")),
    ("1.11.1 Super", "ventas_precios_constantes_desestacionalizada", "Ventas supermercados. Ventas a precios constantes serie desestacionalizada",
     esperado(SUPER, "Ventas en supermercados · Total (precios constantes, desestacionalizado)", "Total", "",
              "Índice 2017=100")),
    ("1.11.1 Super", "almacen", "Ventas supermercados. Almacén",
     esperado(SUPER, "Ventas en supermercados · Almacén", "Por grupo de artículos", "Almacén", "Miles de pesos")),
    ("1.11.1 Super", "subtotal_ventas_alimentos_bebidas", "Ventas supermercados. Subtotal ventas alimentos y bebidas",
     esperado(SUPER, "Ventas en supermercados · Alimentos y bebidas", "Por grupo de artículos",
              "Alimentos y bebidas", "Miles de pesos")),
    ("1.11.1 Super", "ventas_totales_canal_venta", "Ventas supermercados. Ventas totales por canal de venta",
     esperado(SUPER, "Ventas en supermercados · Total", "Total", "", "Miles de pesos")),
    ("1.11.1 Super", "canales_on_line", "Ventas supermercados. Canales On-line",
     esperado(SUPER, "Ventas en supermercados · Canales online", "Por canal de venta", "", "Miles de pesos")),
    ("1.11.1 Super", "tarjetas_debito", "Ventas supermercados. Tarjetas de débito",
     esperado(SUPER, "Ventas en supermercados · Tarjetas de débito", "Por medio de pago", "", "Miles de pesos")),
    # --- Autoservicios mayoristas
    ("1.11.2 Mayoristas", "ventas_precios_constantes", "Autoservicios mayoristas. Ventas a precios constantes",
     esperado(MAYO, "Ventas en autoservicios mayoristas · Total (precios constantes)", "Total", "",
              "Millones de pesos")),
    ("1.11.2 Mayoristas", "carnes", "Autoservicios mayoristas. Carnes",
     esperado(MAYO, "Ventas en autoservicios mayoristas · Carnes", "Por grupo de artículos", "", "Miles de pesos")),
    ("1.11.2 Mayoristas", "salon_de_ventas", "Autoservicios mayoristas. Salón de ventas",
     esperado(MAYO, "Ventas en autoservicios mayoristas · Salón de ventas", "Por canal de venta", "",
              "Miles de pesos")),
    ("1.11.2 Mayoristas", "ventas_totales_grupos_articulos", "Autoservicios mayoristas. Ventas totales por grupos de artículos",
     esperado(MAYO, "Ventas en autoservicios mayoristas · Total", "Total", "", "Miles de pesos")),
    # --- Centros de compras
    ("1.12 Centros compras", "evol_mensual_ventas_mdp_electronicos_cd_elect_computacion", "Ventas por grupos de artículos de artículos electrónicos, cd, electrodomesticos y computacion",
     esperado(CC, "Ventas en centros de compras · Electrónicos, CD, electrodomésticos y computación",
              "Series históricas", "Base 2008", "Miles de pesos")),
    ("1.12 Centros compras", "evolucion_mensual_ventas_mdp_total", "Total de las ventas en centros de compras",
     esperado(CC, "Ventas en centros de compras · Total", "Series históricas", "Base 2008", "Miles de pesos")),
    ("1.12 Centros compras", "espectadores_prom_gba", "Espectadores por sala Gran Buenos Aires",
     esperado(CC, "Centros de compras · Espectadores de cine por sala · Gran Buenos Aires", "Series históricas",
              "Base 2008", "Personas por sala")),
    ("1.12 Centros compras", "salas_caba", "Salas de cine Ciudad de Buenos Aires",
     esperado(CC, "Centros de compras · Salas de cine · Ciudad Autónoma de Buenos Aires", "Series históricas",
              "Base 2008", "Salas")),
    ("1.12.1 Centros compras", "ventas_resto_precios_constantes", "Ventas centros de compras. Ventas resto del país a precios constantes",
     esperado(CC, "Ventas en centros de compras · Total · Resto del país (precios constantes)", "Por región", "",
              "Millones de pesos")),
    ("1.12.1 Centros compras", "ventas_totales_precios_constantes_tendencia_ciclo", "Ventas centros de compras. Ventas totales a precios constantes serie tendencia-ciclo",
     esperado(CC, "Ventas en centros de compras · Total (precios constantes, tendencia-ciclo)", "Total", "",
              "Índice 2017=100")),
    ("1.12.1 Centros compras", "total", "Ventas centros de compras. Total",
     esperado(CC, "Ventas en centros de compras · Total", "Total", "", "Pesos")),
    ("1.12.1 Centros compras", "patio_comidas", "Ventas centros de compras. Patio de comidas, alimentos y kioscos",
     esperado(CC, "Ventas en centros de compras · Patio de comidas, alimentos y kioscos", "Por grupo de artículos",
              "", "Pesos")),
    ("1.12.1 Centros compras", "espectadores", "Espectadores",
     esperado(CC, "Centros de compras · Espectadores de cine", "Cines", "", "Personas")),
    # --- Agro, faena y pesca
    ("1.19 Agro", "soja_hectareas", "Indicadores de evolución del sector agropecuario. Superficie sembrada de soja en hectáreas",
     esperado("Producción agrícola", "Producción agrícola · Soja (superficie sembrada)", unidad="Hectáreas")),
    ("1.19 Agro", "sorgogranifero_toneladadas", "Indicadores de evolución del sector agropecuario. Producción de sorgo granifero en toneladas",
     esperado("Producción agrícola", "Producción agrícola · Sorgo granífero", unidad="Toneladas")),
    ("1.20 Pecuarios", "aves_cabezas", "Cantidad de aves procesadas, Anual",
     esperado("Faena pecuaria", "Faena pecuaria · Aves", unidad="Miles de cabezas")),
    ("1.20 Pecuarios", "ovinos_cabezas", "Faena de cabezas vacunas, Anual",
     esperado("Faena pecuaria", "Faena pecuaria · Ovinos", unidad="Cabezas")),
    ("1.20 Pecuarios", "vacunos_toneladas", "Faena pecuaria, vacunos, Mensual en toneladas",
     esperado("Faena pecuaria", "Faena pecuaria · Vacunos (toneladas)", unidad="Toneladas")),
    ("1.21 Pesca", "total_crustaceos", "Captura pesquera, total crustáceos, Anual en toneladas",
     esperado("Captura pesquera", "Captura pesquera · Crustáceos", unidad="Toneladas")),
    # --- Automotriz y siderúrgica
    ("1.22 Historicos Automotriz", "produccion_categoria_b", "Históricos de la produccion categoria b",
     esperado("Industria automotriz", "Industria automotriz · Producción · Categoría B", "Producción", "",
              "Unidades")),
    ("1.22 Historicos Automotriz", "ventas_mercado_interno_vehiculos_importados_utilitarios", "Históricos de la ventas mercado interno de vehiculos importados utilitarios",
     esperado("Industria automotriz", "Industria automotriz · Ventas de vehículos importados · Utilitarios",
              "Ventas en el mercado interno", "Vehículos importados", "Unidades")),
    ("1.22 Historicos Automotriz", "ventas_mercado_interno_automoviles", "Históricos de la ventas mercado interno automoviles",
     esperado("Industria automotriz", "Industria automotriz · Ventas en el mercado interno · Automóviles",
              "Ventas en el mercado interno", "Total", "Unidades")),
    ("1.22 Historicos Automotriz", "ventas_mercado_interno_vehiculos_nacionales_total", "Históricos de la ventas mercado interno de vehiculos nacionales total",
     esperado("Industria automotriz", "Industria automotriz · Ventas de vehículos nacionales · Total",
              "Ventas en el mercado interno", "Vehículos nacionales", "Unidades")),
    ("1.23 Historicos Siderurgia", "hierro_primario_hierro_esponja", "Volúmen de produción de hierro primario hierro esponja",
     esperado("Industria siderúrgica", "Industria siderúrgica · Producción · Hierro esponja",
              "Acero crudo y hierro primario", "", "Miles de toneladas")),
    ("1.23 Historicos Siderurgia", "laminados_planos_en_frio", "Volúmen de produción de laminados planos en frio",
     esperado("Industria siderúrgica", "Industria siderúrgica · Producción · Laminados planos en frío",
              "Laminados", "", "Miles de toneladas")),
    # --- Energía
    ("1.24 Petroleo", "produccion_de_petroleo_crudo", "Producción de Petróleo Crudo",
     esperado("Petróleo y derivados", "Petróleo y derivados · Producción · Petróleo crudo", "Producción", "",
              "Miles de metros cúbicos")),
    ("1.24 Petroleo", "produccion_nafta_mayor97_ron", "Producción de Nafta >97 RON",
     esperado("Petróleo y derivados", "Petróleo y derivados · Producción · Nafta premium (>97 RON)", "Producción",
              "", "Miles de metros cúbicos")),
    ("1.24 Petroleo", "ventas_gasoil", "Ventas de Gasoil",
     esperado("Petróleo y derivados", "Petróleo y derivados · Ventas · Gasoil", "Ventas y despachos", "",
              "Miles de metros cúbicos")),
    ("1.24 Petroleo", "despachos_asfalto", "Despachos de Asfalto",
     esperado("Petróleo y derivados", "Petróleo y derivados · Despachos · Asfalto", "Ventas y despachos", "",
              "Toneladas")),
    ("1.24 Petroleo", "petroleo_crudo_procesado", "Petróleo Crudo Procesado",
     esperado("Petróleo y derivados", "Petróleo y derivados · Procesamiento · Petróleo crudo",
              "Procesamiento en refinerías", "", "Miles de metros cúbicos")),
    ("1.25 Gas", "produccion_gas_natural", "Producción de Gas Natural",
     esperado("Gas natural", "Gas natural · Producción", "Total", "", "Millones de metros cúbicos")),
    ("1.25 Gas", "total", "Total",
     esperado("Gas natural", "Gas natural · Consumo", "Total", "", "Millones de metros cúbicos")),
    ("1.25 Gas", "centrales_electricas", "Centrales Eléctricas",
     esperado("Gas natural", "Gas natural · Consumo de centrales eléctricas", "Por tipo de usuario", "",
              "Millones de metros cúbicos")),
    ("1.25 Gas", "distrib_gas_cuyana_ecogas", "Distrib. Gas Cuyana ECOGAS",
     esperado("Gas natural", "Gas natural · Consumo · Distribuidora de Gas Cuyana (Ecogas)", "Por distribuidora",
              "", "Millones de metros cúbicos")),
    ("1.26 Empresas", "produccion_petroleo_crudo_sinopec_argentina", "Producción de Petróleo Crudo SINOPEC ARGENTINA EXPLORATION AND PRODUCTION, INC. (ex OCCIDENTAL)",
     esperado("Hidrocarburos por empresa",
              "Hidrocarburos por empresa · Producción de petróleo crudo · Sinopec Argentina Exploration and Production",
              "Producción de petróleo crudo", "", "Metros cúbicos")),
    ("1.26 Empresas", "produccion_gas_natural_ypf_sa", "Producción de Gas Natural YPF S.A.",
     esperado("Hidrocarburos por empresa", "Hidrocarburos por empresa · Producción de gas natural · YPF",
              "Producción de gas natural", "", "Miles de metros cúbicos")),
    ("1.26 Empresas", "ventas_naftas_essoaxion", "Ventas de Naftas ESSO/AXION",
     esperado("Hidrocarburos por empresa", "Hidrocarburos por empresa · Ventas de naftas · Esso/Axion",
              "Ventas de naftas", "", "Metros cúbicos")),
    ("1.27 Pozos", "metros_perforados_total_general", "Metros perforados Total general",
     esperado("Pozos y metros perforados", "Pozos y metros perforados · Metros perforados · Total",
              "Metros perforados", "", "Metros")),
    ("1.27 Pozos", "pozos_terminados_exploracion", "Pozos termnados Exploración",
     esperado("Pozos y metros perforados", "Pozos y metros perforados · Pozos terminados · Exploración",
              "Pozos terminados", "", "Pozos")),
    ("1.28 Electricidad", "demanda_total", "Demanda Total",
     esperado("Electricidad", "Electricidad · Demanda", "Total", "", "GWh")),
    ("1.28 Electricidad", "grandes_usuarios", "Grandes Usuarios",
     esperado("Electricidad", "Electricidad · Demanda de grandes usuarios", "Por tipo de usuario", "", "GWh")),
    ("1.28 Electricidad", "edestesa_emp_dist__este", "Demanda por Distribuidora (Principales Empresas) EDESTESA(EMP.DIST.EL.DEL ESTE)",
     esperado("Electricidad", "Electricidad · Demanda · Edestesa", "Por distribuidora", "", "GWh")),
    ("1.28 Electricidad", "energia_san_juan_sa_exedessa", "Demanda por Distribuidora (Principales Empresas) ENERGIA SAN JUAN SA EX-EDESSA",
     esperado("Electricidad", "Electricidad · Demanda · Energía San Juan (ex Edessa)", "Por distribuidora", "",
              "GWh")),
    ("1.28 Electricidad", "potencia_instalada_turbina_a_gas", "Potencia Instalada Turbina a gas",
     esperado("Electricidad", "Electricidad · Potencia instalada · Turbina a gas", "Por tecnología", "", "MW")),
    ("1.28 Electricidad", "temperatura_promedio", "Temperatura Promedio",
     esperado("Electricidad", "Electricidad · Temperatura promedio", "Total", "", "°C")),
    ("1.29 Biocombustibles", "biodisel_ventas_al_corte", "Biodisel ventas al corte",
     esperado("Biocombustibles", "Biocombustibles · Ventas de biodiésel al corte", "Biodiésel", "", "Toneladas")),
    ("1.29 Biocombustibles", "bioetanol_base_cania_azucar_produccion", "Bioetanol en base a cania de azúcarproducción",
     esperado("Biocombustibles", "Biocombustibles · Producción de bioetanol · Caña de azúcar", "Bioetanol", "",
              "Metros cúbicos")),
    ("1.29 Biocombustibles", "bioetanol_total_ventas_corte", "Bioetanol total ventas al corte",
     esperado("Biocombustibles", "Biocombustibles · Ventas de bioetanol al corte", "Bioetanol", "",
              "Metros cúbicos")),
    # --- Confianza y ocupación hotelera
    ("1.30 ICC - UTDT", "icc_nacional", "ICC Nacional",
     esperado("Confianza del consumidor", "Confianza del consumidor · Nivel general", "Nivel general", "",
              "Índice (0-100)")),
    ("1.30 ICC - UTDT", "desagregacion_regiones_capital", "ICC: Desagregacion por regiones Capital",
     esperado("Confianza del consumidor", "Confianza del consumidor · Nivel general · Ciudad Autónoma de Buenos Aires",
              "Por región", "", "Índice (0-100)")),
    ("1.30 ICC - UTDT", "desagregacion_subindices_bienes_durables_inmuebles", "ICC: Desagregacion por subindices bienes durables e inmuebles",
     esperado("Confianza del consumidor", "Confianza del consumidor · Bienes durables e inmuebles",
              "Por subíndice", "", "Índice (0-100)")),
    ("1.31 EOH", "pernoctaciones_no_residentes", "EOH. Pernoctaciones no residentes",
     esperado("Ocupación hotelera", "Ocupación hotelera · Pernoctaciones · No residentes", "Pernoctaciones", "",
              "Noches")),
    ("1.31 EOH", "viajeros_totales", "EOH. Viajeros totales",
     esperado("Ocupación hotelera", "Ocupación hotelera · Viajeros", "Viajeros", "", "Personas")),
    ("1.32 ICG - UTDT", "icg_nivel_general", "ICG Nivel General",
     esperado("Confianza en el gobierno", "Confianza en el gobierno · Nivel general", "Nivel general", "",
              "Índice (0-5)")),
    ("1.32 ICG - UTDT", "evaluacion_general", "ICG. Desagregación Evaluación General",
     esperado("Confianza en el gobierno", "Confianza en el gobierno · Evaluación general", "Por componente", "",
              "Índice (0-5)")),
)


class ReglasActividadOtros(unittest.TestCase):
    def test_casos(self):
        for hoja, variable, descripcion, buscado in CASOS:
            with self.subTest(hoja=hoja, variable=variable):
                self.assertEqual(clasificar(serie(hoja, variable, descripcion)), buscado)

    def test_hay_al_menos_25_casos_y_cubren_todas_las_hojas(self):
        self.assertGreaterEqual(len(CASOS), 25)
        hojas = {hoja for hoja, *_ in CASOS}
        self.assertEqual(len(hojas), 21)

    def test_el_nombre_no_depende_de_la_frecuencia(self):
        anual = clasificar(serie("1.25 Gas", "metrogas", "Metrogas", frecuencia="A"))
        mensual = clasificar(serie("1.25 Gas", "metrogas", "Metrogas", frecuencia="M"))
        self.assertEqual(anual, mensual)

    def test_variantes_de_variable_entre_frecuencias(self):
        # 1.20: la descripción de ovinos dice «vacunas» en la mensual; la variable manda.
        self.assertEqual(clasificar(serie("1.20 Pecuarios", "ovinos_cabezas", "Faena de cabezas vacunas, Anual")).nombre,
                         "Faena pecuaria · Ovinos")
        # 1.10: la variable del subte cambia entre frecuencias; el resultado es el mismo.
        a = clasificar(serie("1.10 SS PP", "pasaj_serv_subterraneos_metrovias"))
        b = clasificar(serie("1.10 SS PP", "pasaj_serv_subterraneos_metrovias_sa"))
        self.assertEqual(a, b)

    def test_series_fuera_de_alcance(self):
        self.assertIsNone(clasificar(serie("1.25 Gas", "metrogas", fuente="indec-supermercados")))
        self.assertIsNone(clasificar(serie("1.25 Gas", "metrogas", archivo="otro.xlsx")))
        self.assertIsNone(clasificar(serie("1.13 Ventas", "ventas_totales")))
        self.assertIsNone(clasificar(serie("1.3 PIB real x sector", "pib")))
        # Hoja de mi alcance pero serie que no conozco: no se inventa una clasificación.
        self.assertIsNone(clasificar(serie("1.25 Gas", "variable_desconocida")))

    def test_dos_llamadas_dan_lo_mismo(self):
        for hoja, variable, descripcion, _ in CASOS:
            primero = clasificar(serie(hoja, variable, descripcion))
            segundo = clasificar(serie(hoja, variable, descripcion))
            self.assertEqual(primero, segundo)

    def test_siempre_tema_actividad_y_nombre_sin_restos(self):
        for hoja, variable, descripcion, _ in CASOS:
            resultado = clasificar(serie(hoja, variable, descripcion))
            self.assertEqual(resultado.tema, "Actividad")
            self.assertNotIn("_", resultado.nombre)
            self.assertFalse(resultado.nombre.endswith("."))


class CorreccionesActividadOtros(unittest.TestCase):
    def test_telefonia_dos_mediciones_no_comparten_nombre(self):
        # Se solapan 13 meses con valores distintos: no son dos tramos de una misma serie.
        pares = (("serv_telefonico_basico_llamadas_interurbanas", "serv_telefonico_basico_llamadas_interurbanas_2002"),
                 ("serv_telefonico_internac_minutos_salidos", "serv_internacional_minutos_salida_2002"),
                 ("serv_telefonico_internac_minutos_salida", "serv_internacional_minutos_salida_2002"))
        for vieja, nueva in pares:
            with self.subTest(vieja=vieja):
                a = clasificar(serie("1.10 SS PP", vieja))
                b = clasificar(serie("1.10 SS PP", nueva))
                self.assertNotEqual(a.nombre, b.nombre)
                self.assertTrue(a.nombre.endswith("(serie hasta 2003)"))
                self.assertEqual(a.nombre.replace(" (serie hasta 2003)", ""), b.nombre)
        # Las series hasta 2003 sin continuación conservan su nombre.
        self.assertEqual(clasificar(serie("1.10 SS PP", "serv_telefonico_internac_minutos_entrados")).nombre,
                         "Servicios públicos · Telefonía fija · Minutos internacionales entrantes")

    def test_trimestral_de_flujos_base_2017_es_promedio_mensual(self):
        t = clasificar(serie("1.10.1 SSPP 2017", "transp_pasaj_total_miles_pasaj", frecuencia="T"))
        m = clasificar(serie("1.10.1 SSPP 2017", "transp_pasaj_total_miles_pasaj", frecuencia="M"))
        a = clasificar(serie("1.10.1 SSPP 2017", "transp_pasaj_total_miles_pasaj", frecuencia="A"))
        self.assertEqual(t.unidad, "Miles de pasajeros (promedio mensual)")
        self.assertEqual((m.unidad, a.unidad), ("Miles de pasajeros", "Miles de pasajeros"))
        lineas = clasificar(serie("1.10.1 SSPP 2017", "serv_telef_fija_lineas_activas_miles_u", frecuencia="T"))
        self.assertEqual(lineas.unidad, "Miles de líneas")
        indice = clasificar(serie("1.10.1 SSPP 2017", "s_original_peajes", frecuencia="T"))
        self.assertEqual(indice.unidad, "Índice 2004=100")

    def test_totales_de_supermercados_y_mayoristas_son_el_mismo_total(self):
        for hoja in ("1.11.1 Super", "1.11.2 Mayoristas"):
            nombres = {clasificar(serie(hoja, v)).nombre
                       for v in ("ventas_totales_canal_venta", "ventas_totales_medio_pago", "ventas_totales_grupos_articulos")}
            self.assertEqual(len(nombres), 1)
            self.assertTrue(next(iter(nombres)).endswith(" · Total"))

    def test_excluidas(self):
        claves = {i for _, i in EXCLUIDAS}
        self.assertTrue(all(f == "datos.gob.ar" for f, _ in EXCLUIDAS))
        # Anual 1.10 con 2001-2013 cruzado con otra serie (2014 propio).
        self.assertTrue({"35.1_EGABO_1993_A_35", "35.1_PBO_1993_A_20", "35.1_TBO_1993_A_23",
                         "35.1_TCBO_1993_A_31", "35.1_TPBO_1993_A_34"} <= claves)
        # Las desestacionalizadas anuales y el nivel general anual son correctos y se publican.
        self.assertTrue(claves.isdisjoint({"35.1_NG_1993_A_13", "35.1_TBD_1993_A_21", "35.1_PBD_1993_A_18"}))
        # Stock de líneas sumado en anual y trimestral; la mensual se publica.
        self.assertTrue({"35.1_STBLS_1993_A_38", "35.2_STBLS_1993_T_38", "35.1_STMC_1993_A_29",
                         "35.2_STMC_1993_T_29"} <= claves)
        self.assertNotIn("35.3_STBLS_1993_M_38", claves)
        # Copias de «Total por canal de venta».
        self.assertTrue({"455.1_VENTAS_TOTAGO_0_M_25_99", "455.1_VENTAS_TOTLOS_0_M_30_43",
                         "456.1_VENTAS_TOTAGO_0_M_25_27", "456.1_VENTAS_TOTLOS_0_M_31_93"} <= claves)
        self.assertNotIn("455.1_VENTAS_TOTNTA_0_M_26_88", claves)
        self.assertEqual(len(EXCLUIDAS), 13)
        self.assertTrue(all(motivo.strip() for motivo in EXCLUIDAS.values()))


if __name__ == "__main__":
    unittest.main()
