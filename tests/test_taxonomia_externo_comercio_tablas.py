"""Pruebas de las reglas de las grandes tablas de comercio exterior (sector_externo.xlsx, datos.gob.ar)."""

from __future__ import annotations

import unittest
from collections import Counter

from tools.taxonomia import Clasificacion, Serie
from tools.taxonomia.externo_comercio_tablas import (
    CAPITULOS, _CAPITULOS, _PATRONES_CAPITULO, _SECCIONES, clasificar, seccion_de,
)

I = "Intercambio comercial argentino"
MILLONES = "Millones de dólares"
NCM = "Por sección y capítulo de la NCM"
PAIS = "Por país y región"
RUBRO = "Por provincia y rubro"
DESTINO = "Por provincia y destino"


def serie(hoja: str, variable: str, frecuencia: str = "A", fuente: str = "datos.gob.ar",
          archivo: str = "sector_externo.xlsx") -> Serie:
    return Serie(
        fuente=fuente, id=f"id-{variable}-{frecuencia}", nombre="Título del cuadro", variable=variable,
        descripcion="Descripción", unidad="", frecuencia=frecuencia, archivo=archivo, hoja=hoja,
        columna=f"col-{variable}", dataset="Título del dataset", estado="VIGENTE",
    )


def esperado(flujo: str, apertura: str, grupo: str, nombre: str) -> Clasificacion:
    return Clasificacion(tema="Sector externo", indicador=I, cobertura=flujo, apertura=apertura,
                         medida=grupo, nombre=nombre, unidad=MILLONES)


S1 = "Sección I · Animales vivos y productos del reino animal"
S2 = "Sección II · Productos del reino vegetal"
S4 = "Sección IV · Productos de las industrias alimentarias, bebidas y tabaco"
S5 = "Sección V · Productos minerales"
S11 = "Sección XI · Materias textiles y sus manufacturas"
S16 = "Sección XVI · Máquinas, aparatos y material eléctrico"
S17 = "Sección XVII · Material de transporte"
X, M, SC = "4. X NCM", "5. M NCM", "6. Saldo NCM"

CASOS = (
    # Hojas 4, 5 y 6: capítulos de la NCM
    (serie(X, "x_animales_vivos"), esperado("Exportaciones", NCM, S1, "Exportaciones · Animales vivos")),
    (serie(X, "x_cereales", "M"), esperado("Exportaciones", NCM, S2, "Exportaciones · Cereales")),
    (serie(X, "x_leche_productos_lacteos_huevos_ave_miel", "T"),
     esperado("Exportaciones", NCM, S1, "Exportaciones · Leche y productos lácteos; huevos de ave; miel natural")),
    (serie(M, "m_combustibles_minerales_aceites_minerales_prod_destilacion", "M"),
     esperado("Importaciones", NCM, S5,
              "Importaciones · Combustibles minerales, aceites minerales y productos de su destilación; "
              "materias bituminosas")),
    (serie(X, "x_comb_minerales_aceites_minerales_productos_destilacion"),
     esperado("Exportaciones", NCM, S5,
              "Exportaciones · Combustibles minerales, aceites minerales y productos de su destilación; "
              "materias bituminosas")),
    (serie(SC, "sc_vehiculos_automoviles_tractores_demas_vehiculos"),
     esperado("Saldo comercial", NCM, S17,
              "Saldo comercial · Vehículos automóviles, tractores, velocípedos y demás vehículos terrestres, "
              "sus partes y accesorios")),
    (serie(M, "m_prendas_complementos_accesorios_vestir_emcepto_punto"),
     esperado("Importaciones", NCM, S11,
              "Importaciones · Prendas y complementos (accesorios) de vestir, excepto los de punto")),
    (serie(X, "x_prendas_complementos_accesorios_vestir_punto"),
     esperado("Exportaciones", NCM, S11,
              "Exportaciones · Prendas y complementos (accesorios) de vestir, de punto")),
    (serie(M, "m_demas_fibras_temtiles_vegetales_hilados_papel", "T"),
     esperado("Importaciones", NCM, S11,
              "Importaciones · Las demás fibras textiles vegetales; hilados de papel y tejidos de hilados de papel")),
    (serie(SC, "sc_maquinas_aparatos_material_electrico_partes"),
     esperado("Saldo comercial", NCM, S16, "Saldo comercial · Máquinas, aparatos y material eléctrico y sus partes")),
    (serie(X, "x_resto"), esperado("Exportaciones", NCM, "Total y resto", "Exportaciones · Resto")),
    (serie(M, "m_total", "M"), esperado("Importaciones", NCM, "Total y resto", "Importaciones")),
    (serie(SC, "sc_total", "T"), esperado("Saldo comercial", NCM, "Total y resto", "Saldo comercial")),
    # Hojas 7, 8 y 9: países, bloques y regiones
    (serie("7. X Paises", "ica_exportaciones_brasil", "M"),
     esperado("Exportaciones", PAIS, "Países", "Exportaciones · Brasil")),
    (serie("7. X Paises", "ica_exportaciones_corea"),
     esperado("Exportaciones", PAIS, "Países", "Exportaciones · República de Corea")),
    (serie("7. X Paises", "ica_exportaciones_espana", "T"),
     esperado("Exportaciones", PAIS, "Países", "Exportaciones · España")),
    (serie("7. X Paises", "ica_exportaciones_total_mercosur"),
     esperado("Exportaciones", PAIS, "Bloques y regiones", "Exportaciones · MERCOSUR")),
    (serie("7. X Paises", "ica_exportaciones_total_ue_28"),
     esperado("Exportaciones", PAIS, "Bloques y regiones", "Exportaciones · Unión Europea (UE-28)")),
    (serie("7. X Paises", "ica_exportaciones_resto_mundo"),
     esperado("Exportaciones", PAIS, "Bloques y regiones", "Exportaciones · Resto del mundo")),
    (serie("7. X Paises", "ica_exportaciones_totales", "M"), esperado("Exportaciones", PAIS, "Total", "Exportaciones")),
    (serie("8. M Paises", "ica_importaciones_autria"),
     esperado("Importaciones", PAIS, "Países", "Importaciones · Austria")),
    (serie("8. M Paises", "ica_importaciones_taiwan", "M"),
     esperado("Importaciones", PAIS, "Países", "Importaciones · Taiwán")),
    (serie("8. M Paises", "ica_importaciones_total_pacifico", "M"),
     esperado("Importaciones", PAIS, "Bloques y regiones", "Importaciones · Asia Pacífico")),
    (serie("8. M Paises", "ica_participacion_importaciones_asia_pacifico", "T"),
     esperado("Importaciones", PAIS, "Bloques y regiones", "Importaciones · Asia Pacífico")),
    (serie("8. M Paises", "ica_participacion_importaciones_aladi_total", "M"),
     esperado("Importaciones", PAIS, "Bloques y regiones", "Importaciones · ALADI")),
    (serie("8. M Paises", "ica_importaciones_aladi"),
     esperado("Importaciones", PAIS, "Bloques y regiones", "Importaciones · ALADI")),
    (serie("8. M Paises", "ica_participacion_import_mercado_comun_centroamericano"),
     esperado("Importaciones", PAIS, "Bloques y regiones", "Importaciones · Mercado Común Centroamericano")),
    (serie("8. M Paises", "ica_importaciones_totales"), esperado("Importaciones", PAIS, "Total", "Importaciones")),
    (serie("9. Saldo Paises", "ica_saldo_comercial_paises_bajos"),
     esperado("Saldo comercial", PAIS, "Países", "Saldo comercial · Países Bajos")),
    (serie("9. Saldo Paises", "ica_saldo_comercial_asia_pacifico"),
     esperado("Saldo comercial", PAIS, "Bloques y regiones", "Saldo comercial · Asia Pacífico")),
    (serie("9. Saldo Paises", "ica_saldo_comercial_total_asia_pacifico", "M"),
     esperado("Saldo comercial", PAIS, "Bloques y regiones", "Saldo comercial · Asia Pacífico")),
    (serie("9. Saldo Paises", "ica_saldo_comercial_resto_america_latina"),
     esperado("Saldo comercial", PAIS, "Bloques y regiones", "Saldo comercial · Resto de América Latina")),
    (serie("9. Saldo Paises", "ica_saldo_comercial_totales", "T"),
     esperado("Saldo comercial", PAIS, "Total", "Saldo comercial")),
    # Hoja 36: provincia y rubro
    (serie("36. Expo. provincia y rubro", "misiones_pp"),
     esperado("Exportaciones", RUBRO, "Misiones", "Exportaciones · Productos primarios · Misiones")),
    (serie("36. Expo. provincia y rubro", "cordoba_moa"),
     esperado("Exportaciones", RUBRO, "Córdoba",
              "Exportaciones · Manufacturas de origen agropecuario (MOA) · Córdoba")),
    (serie("36. Expo. provincia y rubro", "ciudad_de_buenos_aires_moi"),
     esperado("Exportaciones", RUBRO, "Ciudad Autónoma de Buenos Aires",
              "Exportaciones · Manufacturas de origen industrial (MOI) · Ciudad Autónoma de Buenos Aires")),
    (serie("36. Expo. provincia y rubro", "neuquen_cye"),
     esperado("Exportaciones", RUBRO, "Neuquén", "Exportaciones · Combustibles y energía · Neuquén")),
    (serie("36. Expo. provincia y rubro", "plataforma_continental_total_plataforma_continental"),
     esperado("Exportaciones", RUBRO, "Plataforma continental", "Exportaciones · Total · Plataforma continental")),
    (serie("36. Expo. provincia y rubro", "total_exportaciones_pp"),
     esperado("Exportaciones", RUBRO, "Total del país", "Exportaciones · Productos primarios")),
    (serie("36. Expo. provincia y rubro", "total_exportaciones_total_exportaciones"),
     esperado("Exportaciones", RUBRO, "Total del país", "Exportaciones")),
    (serie("36. Expo. provincia y rubro", "santiago_del_estero_moi"),
     esperado("Exportaciones", RUBRO, "Santiago del Estero",
              "Exportaciones · Manufacturas de origen industrial (MOI) · Santiago del Estero")),
    # Hoja 37: provincia y destino
    (serie("37. Expo. provincia y destino", "pba_paises_bajos"),
     esperado("Exportaciones", DESTINO, "Buenos Aires", "Exportaciones · Países Bajos · Buenos Aires")),
    (serie("37. Expo. provincia y destino", "caba_hong_kong"),
     esperado("Exportaciones", DESTINO, "Ciudad Autónoma de Buenos Aires",
              "Exportaciones · Hong Kong · Ciudad Autónoma de Buenos Aires")),
    (serie("37. Expo. provincia y destino", "catamarca_republica_corea"),
     esperado("Exportaciones", DESTINO, "Catamarca", "Exportaciones · República de Corea · Catamarca")),
    (serie("37. Expo. provincia y destino", "san_juan_zf_zonamerica_ex_montevideo_uruguay"),
     esperado("Exportaciones", DESTINO, "San Juan", "Exportaciones · Zona Franca Zonamérica (Uruguay) · San Juan")),
    (serie("37. Expo. provincia y destino", "indeterminado_indet_continente"),
     esperado("Exportaciones", DESTINO, "Indeterminado", "Exportaciones · Destino indeterminado · Indeterminado")),
    (serie("37. Expo. provincia y destino", "chubut_resto"),
     esperado("Exportaciones", DESTINO, "Chubut", "Exportaciones · Resto de destinos · Chubut")),
    (serie("37. Expo. provincia y destino", "chubut_total_chubut"),
     esperado("Exportaciones", DESTINO, "Chubut", "Exportaciones · Total · Chubut")),
    (serie("37. Expo. provincia y destino", "exportaciones_totales_brasil"),
     esperado("Exportaciones", DESTINO, "Total del país", "Exportaciones · Brasil")),
    (serie("37. Expo. provincia y destino", "exportaciones_totales_total_general"),
     esperado("Exportaciones", DESTINO, "Total del país", "Exportaciones")),
    (serie("37. Expo. provincia y destino", "exportaciones_totales_resto"),
     esperado("Exportaciones", DESTINO, "Total del país", "Exportaciones · Resto de destinos")),
)

VARIABLES_NCM = {
    # Slugs reales de las tres hojas y las tres frecuencias que cambian de una a otra.
    "x_aceites_esenciales_preparaciones_perfumeria_tocador", "x_aceites_esenciales_resinoides_prepar_perfumeria_tocador",
    "x_demas_productos_origen_animal_no_expresados_ncp", "x_demas_productos_origen_animal_ncp",
    "x_frutas_frutos_comestibles_citricos_melones_sandias", "x_frutas_frutos_comestibles_cortezas_agrios",
    "x_jabon_preparaciones_lavar_preparaciones_lubricanteslogia", "x_jabon_agentes_superficie_organicos_preparaciones",
    "x_perlas_naturales_piedras_preciosas_semipreciosasa_monedas",
    "x_maq_aparatos_material_electrico_aparatos_reproduccion",
    "x_productos_lacteos_prod_comestibles_origen_animal_ncp", "x_hortalizas_raices_tuberculos_alimenticios",
    "x_prod_quim_inorganicos_compuestos_inorganicos_organicos", "x_polvoras_explosivos_articulos_pirotecnia_fosforos",
    "x_prep_carne_pescado_moluscos_demas_invertebrados", "x_pieles_excepto_peleteria_cueros",
}


class PruebasExternoComercioTablas(unittest.TestCase):
    def test_casos_representativos(self):
        for entrada, salida in CASOS:
            with self.subTest(hoja=entrada.hoja, variable=entrada.variable):
                self.assertEqual(clasificar(entrada), salida)

    def test_cantidad_de_casos(self):
        self.assertGreaterEqual(len(CASOS), 25)
        hojas = {entrada.hoja for entrada, _ in CASOS}
        self.assertEqual(len(hojas), 8)

    def test_fuera_de_alcance(self):
        self.assertIsNone(clasificar(serie("1. ICA", "ica_exportaciones_totales")))
        self.assertIsNone(clasificar(serie("2. X Rubro", "x_total")))
        self.assertIsNone(clasificar(serie("38. Precios FOB oficiales", "x_cereales", "D")))
        self.assertIsNone(clasificar(serie(X, "x_cereales", fuente="otra-fuente")))
        self.assertIsNone(clasificar(serie(X, "x_cereales", archivo="otro.xlsx")))

    def test_variable_desconocida_no_se_adivina(self):
        self.assertIsNone(clasificar(serie(X, "x_capitulo_inexistente")))
        self.assertIsNone(clasificar(serie("7. X Paises", "ica_exportaciones_atlantida")))
        self.assertIsNone(clasificar(serie("37. Expo. provincia y destino", "misiones_atlantida")))
        self.assertIsNone(clasificar(serie("36. Expo. provincia y rubro", "misiones_xyz")))

    def test_determinista(self):
        for entrada, _ in CASOS:
            self.assertEqual(clasificar(entrada), clasificar(entrada))

    def test_ignora_nombre_y_descripcion_de_la_fuente(self):
        original = serie("7. X Paises", "ica_exportaciones_brasil")
        otra = Serie(**{**original.__dict__, "nombre": "Cualquier otro título", "descripcion": "Exportaciones a Brasil . En  dólares FOB"})
        self.assertEqual(clasificar(original), clasificar(otra))

    def test_cada_variable_ncm_encaja_en_un_solo_capitulo(self):
        for variable in VARIABLES_NCM:
            slug = variable[2:]
            encajes = [n for n, patron in _PATRONES_CAPITULO if patron.match(slug)]
            self.assertEqual(len(encajes), 1, (variable, encajes))

    def test_capitulos_completos_y_sin_repetir(self):
        numeros = [numero for numero, _, _ in _CAPITULOS]
        self.assertEqual(numeros, [n for n in range(1, 98) if n != 77])
        self.assertEqual(len(set(CAPITULOS.values())), 96)
        self.assertLessEqual(max(map(len, CAPITULOS.values())), 100)

    def test_secciones_cubren_todos_los_capitulos(self):
        cubiertos = Counter(n for primero, ultimo, _ in _SECCIONES for n in range(primero, ultimo + 1))
        self.assertEqual(set(cubiertos), set(range(1, 98)))
        self.assertEqual(max(cubiertos.values()), 1)
        self.assertEqual(seccion_de(27), S5)
        with self.assertRaises(ValueError):
            seccion_de(98)

    def test_nombres_sin_restos_de_slug(self):
        for entrada, salida in CASOS:
            self.assertNotIn("_", salida.nombre)
            self.assertNotIn(" - ", salida.nombre)
            self.assertLessEqual(len(salida.nombre), 120)


if __name__ == "__main__":
    unittest.main()
