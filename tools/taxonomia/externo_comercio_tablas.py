"""Reglas de las grandes tablas de comercio exterior de bienes (``sector_externo.xlsx``, datos.gob.ar).

Árbol: Sector externo > Intercambio comercial argentino > <flujo> > <apertura> > <grupo>.

    Nivel 3 (flujo)       Exportaciones, Importaciones, Saldo comercial
    Nivel 4 (apertura)    Por sección y capítulo de la NCM        hojas 4, 5 y 6
                          Por país y región                       hojas 7, 8 y 9
                          Por provincia y rubro                   hoja 36 (sólo exportaciones)
                          Por provincia y destino                 hoja 37 (sólo exportaciones)
    Nivel 5 (grupo)       NCM: la sección (I a XXI) más «Total y resto»
                          País y región: «Total», «Bloques y regiones» y «Países»
                          Provincias: la jurisdicción (más «Total del país»)

Hojas del libro (el «Hoja origen» exacto) y qué dimensión sale de cada una:

* ``4. X NCM``, ``5. M NCM``, ``6. Saldo NCM``: un capítulo de la Nomenclatura Común del Mercosur
  (96 capítulos: del 1 al 97 sin el 77, reservado) más el total y un «resto» (que es exactamente el
  total menos la suma de los capítulos, comprobado con los datos). El capítulo se reconoce por el
  slug de ``Variable``; los slugs cambian de una frecuencia a otra y entre las tres hojas («x_»,
  «m_» y «sc_»), así que cada capítulo tiene una expresión regular y las pruebas verifican que
  cada variable encaje en una sola.
* ``7. X Paises``, ``8. M Paises``, ``9. Saldo Paises``: un país, un bloque comercial o una región.
  Las series «Participación de importaciones … en el total» de la hoja 8 (ALADI, CAN, Mercado
  Común Centroamericano, Asia Pacífico) no son participaciones: los valores son millones de
  dólares, del orden de las importaciones desde esos bloques. Se nombran como el resto.
* ``36. Expo. provincia y rubro``: jurisdicción × rubro (PP, MOA, MOI, CYE y total), sólo anual.
* ``37. Expo. provincia y destino``: jurisdicción × principales destinos (más «Resto» y «Total»),
  sólo anual. Las series del total del país (``exportaciones_totales_*``) son las mismas
  exportaciones a ese destino de la hoja 7 y llevan el mismo nombre.

Nombres: ``Exportaciones · <capítulo>``, ``Importaciones · <país>``, ``Saldo comercial · <región>``,
``Exportaciones · <rubro> · <provincia>`` y ``Exportaciones · <destino> · <provincia>``. El total
nacional se llama simplemente «Exportaciones» (o «Importaciones», «Saldo comercial»), igual que en
el resto del comercio exterior; el total de una jurisdicción lleva «Total» («Exportaciones · Total ·
Misiones»). Los capítulos llevan la descripción oficial de la NCM abreviada de manera uniforme (ver
``_abreviar``). Unidad: millones de dólares (valores FOB para las exportaciones y CIF para las
importaciones; el saldo es su diferencia).
"""

from __future__ import annotations

import re

from . import Clasificacion, Serie
from .texto import unir

FUENTE = "datos.gob.ar"
ARCHIVO = "sector_externo.xlsx"
TEMA = "Sector externo"
INDICADOR = "Intercambio comercial argentino"
UNIDAD = "Millones de dólares"

EXPORTACIONES = "Exportaciones"
IMPORTACIONES = "Importaciones"
SALDO = "Saldo comercial"

POR_NCM = "Por sección y capítulo de la NCM"
POR_PAIS = "Por país y región"
POR_PROVINCIA_RUBRO = "Por provincia y rubro"
POR_PROVINCIA_DESTINO = "Por provincia y destino"

TOTAL_Y_RESTO = "Total y resto"
TOTAL = "Total"
BLOQUES = "Bloques y regiones"
PAISES = "Países"
TOTAL_PAIS = "Total del país"

# --- Nomenclatura Común del Mercosur ------------------------------------------------------------

LIMITE_DESCRIPCION = 100   # con «Saldo comercial · » el nombre queda por debajo de los 120 caracteres


def _abreviar(texto: str) -> str:
    """Abrevia la descripción oficial de un capítulo, siempre con el mismo criterio.

    Se conservan las cláusulas oficiales (separadas por «;») mientras entren en el límite; si la
    primera sola lo supera, se corta en la última coma que entre.
    """
    clausulas = texto.split("; ")
    if len(clausulas[0]) > LIMITE_DESCRIPCION:
        corte = clausulas[0][:LIMITE_DESCRIPCION + 1].rsplit(", ", 1)[0]
        return corte
    resultado = clausulas[0]
    for clausula in clausulas[1:]:
        if len(resultado) + 2 + len(clausula) > LIMITE_DESCRIPCION:
            break
        resultado = f"{resultado}; {clausula}"
    return resultado


# (capítulo, expresión del slug de «Variable» sin el prefijo de la hoja, descripción oficial de la NCM)
_CAPITULOS = (
    (1, r"animales_vivos", "Animales vivos"),
    (2, r"carne_despojos", "Carne y despojos comestibles"),
    (3, r"pescados_crustaceos", "Pescados y crustáceos, moluscos y demás invertebrados acuáticos"),
    (4, r"leche_productos_lacteos|productos_lacteos",
     "Leche y productos lácteos; huevos de ave; miel natural; productos comestibles de origen animal, "
     "no expresados ni comprendidos en otra parte"),
    (5, r"demas_productos_origen_animal",
     "Los demás productos de origen animal, no expresados ni comprendidos en otra parte"),
    (6, r"plantas_vivas", "Plantas vivas y productos de la floricultura"),
    (7, r"hortalizas_(?:plantas_)?raices", "Hortalizas, plantas, raíces y tubérculos alimenticios"),
    (8, r"frutas_frutos_comestibles", "Frutas y frutos comestibles; cortezas de agrios (cítricos), melones o sandías"),
    (9, r"cafe_te_yerba", "Café, té, yerba mate y especias"),
    (10, r"cereales", "Cereales"),
    (11, r"productos_molineria", "Productos de la molinería; malta; almidón y fécula; inulina; gluten de trigo"),
    (12, r"semillas_frutos_oleaginosos",
     "Semillas y frutos oleaginosos; semillas y frutos diversos; plantas industriales o medicinales; paja y forraje"),
    (13, r"gomas_resinas", "Gomas, resinas y demás jugos y extractos vegetales"),
    (14, r"materias_trenzables",
     "Materias trenzables y demás productos de origen vegetal, no expresados ni comprendidos en otra parte"),
    (15, r"grasas_aceites",
     "Grasas y aceites animales o vegetales; productos de su desdoblamiento; grasas alimenticias elaboradas; "
     "ceras de origen animal o vegetal"),
    (16, r"prep(?:araciones)?_carne",
     "Preparaciones de carne, de pescado o de crustáceos, moluscos o demás invertebrados acuáticos"),
    (17, r"azucares", "Azúcares y artículos de confitería"),
    (18, r"cacao", "Cacao y sus preparaciones"),
    (19, r"preparaciones_base_cereales",
     "Preparaciones a base de cereales, harina, almidón, fécula o leche; productos de pastelería"),
    (20, r"preparaciones_hortalizas", "Preparaciones de hortalizas, frutas u otros frutos o de otras partes de plantas"),
    (21, r"preparaciones_alimenticias", "Preparaciones alimenticias diversas"),
    (22, r"bebidas", "Bebidas, líquidos alcohólicos y vinagre"),
    (23, r"residuos_desperdicios",
     "Residuos y desperdicios de las industrias alimentarias; alimentos preparados para animales"),
    (24, r"tabaco", "Tabaco y sucedáneos del tabaco, elaborados"),
    (25, r"sal_azufre", "Sal; azufre; tierras y piedras; yesos, cales y cementos"),
    (26, r"minerales_metaliferos", "Minerales metalíferos, escorias y cenizas"),
    (27, r"comb(?:ustibles)?_minerales",
     "Combustibles minerales, aceites minerales y productos de su destilación; materias bituminosas; ceras minerales"),
    (28, r"prod(?:uctos)?_quim(?:icos)?_inorganicos",
     "Productos químicos inorgánicos; compuestos inorgánicos u orgánicos de metales preciosos, de elementos "
     "radiactivos, de metales de las tierras raras o de isótopos"),
    (29, r"productos_quimicos_organicos", "Productos químicos orgánicos"),
    (30, r"productos_farmaceuticos", "Productos farmacéuticos"),
    (31, r"abonos", "Abonos"),
    (32, r"extractos_curtientes",
     "Extractos curtientes o tintóreos; taninos y sus derivados; pigmentos y demás materias colorantes; "
     "pinturas y barnices; mástiques; tintas"),
    (33, r"aceites_esenciales",
     "Aceites esenciales y resinoides; preparaciones de perfumería, de tocador o de cosmética"),
    (34, r"jabon",
     "Jabón, agentes de superficie orgánicos, preparaciones para lavar, preparaciones lubricantes, ceras "
     "artificiales, ceras preparadas, productos de limpieza, velas y artículos similares"),
    (35, r"materias_albuminoideas",
     "Materias albuminoideas; productos a base de almidón o de fécula modificados; colas; enzimas"),
    (36, r"polvoras",
     "Pólvoras y explosivos; artículos de pirotecnia; fósforos; aleaciones pirofóricas; materias inflamables"),
    (37, r"productos_fotograficos", "Productos fotográficos o cinematográficos"),
    (38, r"productos_diversos_industrias_quimicas", "Productos diversos de las industrias químicas"),
    (39, r"plastico", "Plástico y sus manufacturas"),
    (40, r"caucho", "Caucho y sus manufacturas"),
    (41, r"pieles", "Pieles (excepto la peletería) y cueros"),
    (42, r"manufacturas_cuero",
     "Manufacturas de cuero; artículos de talabartería o guarnicionería; artículos de viaje, bolsos de mano "
     "(carteras) y continentes similares; manufacturas de tripa"),
    (43, r"peleteria", "Peletería y confecciones de peletería; peletería facticia o artificial"),
    (44, r"madera", "Madera, carbón vegetal y manufacturas de madera"),
    (45, r"corcho", "Corcho y sus manufacturas"),
    (46, r"manufacturas_esparteria", "Manufacturas de espartería o cestería"),
    (47, r"pastas_madera",
     "Pastas de madera o de las demás materias fibrosas celulósicas; papel o cartón para reciclar "
     "(desperdicios y desechos)"),
    (48, r"papel_carton", "Papel y cartón; manufacturas de pasta de celulosa, de papel o de cartón"),
    (49, r"productos_editoriales",
     "Productos editoriales, de la prensa y de las demás industrias gráficas; textos manuscritos o "
     "mecanografiados, y planos"),
    (50, r"seda", "Seda"),
    (51, r"lana", "Lana y pelo fino u ordinario; hilados y tejidos de crin"),
    (52, r"algodon", "Algodón"),
    (53, r"demas_fibras_t(?:ex|em)tiles", "Las demás fibras textiles vegetales; hilados de papel y tejidos de hilados de papel"),
    (54, r"filamentos",
     "Filamentos sintéticos o artificiales; tiras y formas similares de materia textil sintética o artificial"),
    (55, r"fibras_sinteticas", "Fibras sintéticas o artificiales discontinuas"),
    (56, r"guata",
     "Guata, fieltro y tela sin tejer; hilados especiales; cordeles, cuerdas y cordajes; artículos de cordelería"),
    (57, r"alfombras", "Alfombras y demás revestimientos para el suelo, de materia textil"),
    (58, r"tejidos_especiales",
     "Tejidos especiales; superficies textiles con mechón insertado; encajes; tapicería; pasamanería; bordados"),
    (59, r"telas_impregnadas",
     "Telas impregnadas, recubiertas, revestidas o estratificadas; artículos técnicos de materia textil"),
    (60, r"tejido_punto", "Tejidos de punto"),
    (61, r"prendas_.*_vestir_punto", "Prendas y complementos (accesorios) de vestir, de punto"),
    (62, r"prendas_.*_vestir_(?:ex|em)cepto_punto", "Prendas y complementos (accesorios) de vestir, excepto los de punto"),
    (63, r"demas_articulos_textiles", "Los demás artículos textiles confeccionados; juegos; prendería y trapos"),
    (64, r"calzado", "Calzado, polainas y artículos análogos; partes de estos artículos"),
    (65, r"sombreros", "Sombreros, demás tocados y sus partes"),
    (66, r"paraguas", "Paraguas, sombrillas, quitasoles, bastones, bastones asiento, látigos, fustas y sus partes"),
    (67, r"plumas",
     "Plumas y plumón preparados y artículos de plumas o de plumón; flores artificiales; manufacturas de cabello"),
    (68, r"manufacturas_piedra",
     "Manufacturas de piedra, yeso fraguable, cemento, amianto (asbesto), mica o materias análogas"),
    (69, r"productos_ceramicos", "Productos cerámicos"),
    (70, r"vidrio", "Vidrio y sus manufacturas"),
    (71, r"perlas",
     "Perlas finas (naturales) o cultivadas, piedras preciosas o semipreciosas y similares, metales preciosos, "
     "chapados de metal precioso (plaqué) y manufacturas de estas materias; bisutería; monedas"),
    (72, r"fundicion_hierro_acero", "Fundición, hierro y acero"),
    (73, r"manufacturas_fundicion", "Manufacturas de fundición, de hierro o de acero"),
    (74, r"cobre", "Cobre y sus manufacturas"),
    (75, r"niquel", "Níquel y sus manufacturas"),
    (76, r"aluminio", "Aluminio y sus manufacturas"),
    (78, r"plomo", "Plomo y sus manufacturas"),
    (79, r"cinc", "Cinc y sus manufacturas"),
    (80, r"estano", "Estaño y sus manufacturas"),
    (81, r"demas_metales_comunes", "Los demás metales comunes; cermets; manufacturas de estas materias"),
    (82, r"herramientas",
     "Herramientas y útiles, artículos de cuchillería y cubiertos de mesa, de metales comunes; "
     "partes de estos artículos, de metal común"),
    (83, r"manufacturas_diversas_metal", "Manufacturas diversas de metales comunes"),
    (84, r"reactores",
     "Reactores nucleares, calderas, máquinas, aparatos y artefactos mecánicos; partes de estas máquinas o aparatos"),
    (85, r"maq(?:uinas)?_aparatos_material_electrico",
     "Máquinas, aparatos y material eléctrico y sus partes; aparatos de grabación o reproducción de sonido, "
     "aparatos de grabación o reproducción de imágenes y sonido en televisión, y las partes y accesorios "
     "de estos aparatos"),
    (86, r"vehiculos_material",
     "Vehículos y material para vías férreas o similares, y sus partes; aparatos mecánicos (incluso "
     "electromecánicos) de señalización para vías de comunicación"),
    (87, r"vehiculos_automoviles",
     "Vehículos automóviles, tractores, velocípedos y demás vehículos terrestres, sus partes y accesorios"),
    (88, r"aeronaves", "Aeronaves, vehículos espaciales y sus partes"),
    (89, r"barcos", "Barcos y demás artefactos flotantes"),
    (90, r"instrumentos_aparatos",
     "Instrumentos y aparatos de óptica, fotografía o cinematografía, de medida, control o precisión; "
     "instrumentos y aparatos médico-quirúrgicos; partes y accesorios de estos instrumentos o aparatos"),
    (91, r"aparatos_relojeria", "Aparatos de relojería y sus partes"),
    (92, r"instrumentos_musicales", "Instrumentos musicales; partes y accesorios de estos instrumentos"),
    (93, r"armas", "Armas, municiones, y sus partes y accesorios"),
    (94, r"muebles",
     "Muebles; mobiliario médico-quirúrgico; artículos de cama y similares; aparatos de alumbrado no expresados "
     "ni comprendidos en otra parte; anuncios, letreros y placas indicadoras luminosos y artículos similares; "
     "construcciones prefabricadas"),
    (95, r"juguetes", "Juguetes, juegos y artículos para recreo o deporte; sus partes y accesorios"),
    (96, r"manufacturas_diversas", "Manufacturas diversas"),
    (97, r"objetos_arte", "Objetos de arte o de colección y antigüedades"),
)
CAPITULOS = {numero: _abreviar(texto) for numero, _, texto in _CAPITULOS}
_PATRONES_CAPITULO = tuple((numero, re.compile(patron + "$" if patron == "manufacturas_diversas" else patron))
                           for numero, patron, _ in _CAPITULOS)

# Secciones de la NCM: (primer capítulo, último capítulo, nombre abreviado).
_SECCIONES = (
    (1, 5, "Sección I · Animales vivos y productos del reino animal"),
    (6, 14, "Sección II · Productos del reino vegetal"),
    (15, 15, "Sección III · Grasas y aceites animales o vegetales"),
    (16, 24, "Sección IV · Productos de las industrias alimentarias, bebidas y tabaco"),
    (25, 27, "Sección V · Productos minerales"),
    (28, 38, "Sección VI · Productos de las industrias químicas"),
    (39, 40, "Sección VII · Plástico y caucho y sus manufacturas"),
    (41, 43, "Sección VIII · Pieles, cueros, peletería y sus manufacturas"),
    (44, 46, "Sección IX · Madera, corcho, espartería y cestería"),
    (47, 49, "Sección X · Pastas de madera, papel y cartón"),
    (50, 63, "Sección XI · Materias textiles y sus manufacturas"),
    (64, 67, "Sección XII · Calzado, sombrerería, paraguas y plumas"),
    (68, 70, "Sección XIII · Manufacturas de piedra, cerámica y vidrio"),
    (71, 71, "Sección XIV · Perlas, piedras preciosas y metales preciosos"),
    (72, 83, "Sección XV · Metales comunes y sus manufacturas"),
    (84, 85, "Sección XVI · Máquinas, aparatos y material eléctrico"),
    (86, 89, "Sección XVII · Material de transporte"),
    (90, 92, "Sección XVIII · Instrumentos de óptica y de precisión, relojería e instrumentos musicales"),
    (93, 93, "Sección XIX · Armas y municiones"),
    (94, 96, "Sección XX · Mercancías y productos diversos"),
    (97, 97, "Sección XXI · Objetos de arte, de colección y antigüedades"),
)


def seccion_de(capitulo: int) -> str:
    for primero, ultimo, nombre in _SECCIONES:
        if primero <= capitulo <= ultimo:
            return nombre
    raise ValueError(f"capítulo fuera de la NCM: {capitulo}")


def _capitulo(slug: str) -> int | None:
    """Número de capítulo del slug (sin prefijo), o None si no encaja en uno solo."""
    candidatos = [numero for numero, patron in _PATRONES_CAPITULO if patron.match(slug)]
    return candidatos[0] if len(candidatos) == 1 else None


# --- Países, bloques y regiones --------------------------------------------------------------------

# slug (sin el prefijo del flujo) -> nombre. Los slugs de una misma entidad cambian entre hojas
# («autria», «total_pacifico», «total_resto_al»…): todos van a la misma entrada.
_PAISES = {
    "alemania": "Alemania", "angola": "Angola", "argelia": "Argelia", "australia": "Australia",
    "austria": "Austria", "autria": "Austria", "belgica": "Bélgica", "bolivia": "Bolivia",
    "brasil": "Brasil", "canada": "Canadá", "chile": "Chile", "china": "China", "colombia": "Colombia",
    "corea": "República de Corea", "ecuador": "Ecuador", "egipto": "Egipto", "espana": "España",
    "estados_unidos": "Estados Unidos", "francia": "Francia", "india": "India", "indonesia": "Indonesia",
    "israel": "Israel", "italia": "Italia", "japon": "Japón", "marruecos": "Marruecos", "mexico": "México",
    "nueva_zelanda": "Nueva Zelanda", "paises_bajos": "Países Bajos", "paraguay": "Paraguay",
    "peru": "Perú", "polonia": "Polonia", "portugal": "Portugal", "reino_unido": "Reino Unido",
    "rusia": "Rusia", "sudafrica": "Sudáfrica", "suiza": "Suiza", "tailandia": "Tailandia",
    "taiwan": "Taiwán", "uruguay": "Uruguay", "venezuela": "Venezuela", "vietnam": "Vietnam",
}
_BLOQUES = {
    "aladi": "ALADI", "aladi_total": "ALADI", "asean": "ASEAN", "can": "CAN", "can_total": "CAN",
    "mercado_comun_centr": "Mercado Común Centroamericano",
    "mercado_comun_centroamericano": "Mercado Común Centroamericano",
    "medio_oriente": "Medio Oriente", "total_mercosur": "MERCOSUR", "total_nafta": "NAFTA",
    "alianza_pacifico": "Alianza del Pacífico", "total_alianza_pacifico": "Alianza del Pacífico",
    "total_ue_28": "Unión Europea (UE-28)", "total_ue": "Unión Europea (UE-28)",
    "resto_ue_28": "Resto de la Unión Europea (UE-28)", "resto_ue": "Resto de la Unión Europea (UE-28)",
    "total_america_latina": "América Latina", "total_resto_al": "América Latina",
    "resto_america_latina": "Resto de América Latina", "resto_al": "Resto de América Latina",
    "total_africa": "África", "resto_africa": "Resto de África",
    "asia_pacifico": "Asia Pacífico", "total_asia_pacifico": "Asia Pacífico", "total_pacifico": "Asia Pacífico",
    "resto_asia_pacifico": "Resto de Asia Pacífico", "resto_pacifico": "Resto de Asia Pacífico",
    "resto_mundo": "Resto del mundo",
}

# Hoja -> (flujo, prefijos del slug en «Variable»; el más largo primero)
_HOJAS_PAIS = {
    "7. X Paises": (EXPORTACIONES, ("ica_exportaciones_",)),
    "8. M Paises": (IMPORTACIONES, ("ica_participacion_importaciones_", "ica_participacion_import_",
                                    "ica_importaciones_")),
    "9. Saldo Paises": (SALDO, ("ica_saldo_comercial_",)),
}
_HOJAS_NCM = {
    "4. X NCM": (EXPORTACIONES, "x_"),
    "5. M NCM": (IMPORTACIONES, "m_"),
    "6. Saldo NCM": (SALDO, "sc_"),
}

# --- Provincias, rubros y destinos -----------------------------------------------------------------

_PROVINCIAS = {
    "buenos_aires": "Buenos Aires", "pba": "Buenos Aires", "catamarca": "Catamarca", "chaco": "Chaco",
    "chubut": "Chubut", "ciudad_de_buenos_aires": "Ciudad Autónoma de Buenos Aires",
    "caba": "Ciudad Autónoma de Buenos Aires", "cordoba": "Córdoba", "corrientes": "Corrientes",
    "entre_rios": "Entre Ríos", "formosa": "Formosa", "jujuy": "Jujuy", "la_pampa": "La Pampa",
    "la_rioja": "La Rioja", "mendoza": "Mendoza", "misiones": "Misiones", "neuquen": "Neuquén",
    "rio_negro": "Río Negro", "salta": "Salta", "san_juan": "San Juan", "san_luis": "San Luis",
    "santa_cruz": "Santa Cruz", "santa_fe": "Santa Fe", "santiago_del_estero": "Santiago del Estero",
    "tierra_del_fuego": "Tierra del Fuego", "tucuman": "Tucumán",
    # Orígenes que no son una provincia, tal como los publica el INDEC.
    "extranjero": "Extranjero", "indeterminado": "Indeterminado",
    "plataforma_continental": "Plataforma continental",
}
# Total del país: no lleva cobertura en el nombre (es la cobertura natural).
_TOTALES_PAIS = ("total_exportaciones", "exportaciones_totales")
_SLUGS_ORDENADOS = tuple(sorted((*_PROVINCIAS, *_TOTALES_PAIS), key=len, reverse=True))

_RUBROS = {
    "pp": "Productos primarios",
    "moa": "Manufacturas de origen agropecuario (MOA)",
    "moi": "Manufacturas de origen industrial (MOI)",
    "cye": "Combustibles y energía",
}
_DESTINOS = {
    "alemania": "Alemania", "arabia_saudita": "Arabia Saudita", "argelia": "Argelia", "belgica": "Bélgica",
    "bolivia": "Bolivia", "brasil": "Brasil", "bulgaria": "Bulgaria", "canada": "Canadá", "chile": "Chile",
    "china": "China", "colombia": "Colombia", "costa_rica": "Costa Rica", "ecuador": "Ecuador",
    "egipto": "Egipto", "espania": "España", "estados_unidos": "Estados Unidos", "filipinas": "Filipinas",
    "finlandia": "Finlandia", "francia": "Francia", "hong_kong": "Hong Kong",
    "indet_continente": "Destino indeterminado", "india": "India", "indonesia": "Indonesia", "iraq": "Irak",
    "italia": "Italia", "japon": "Japón", "malasia": "Malasia", "mexico": "México",
    "paises_bajos": "Países Bajos", "paraguay": "Paraguay", "peru": "Perú", "reino_unido": "Reino Unido",
    "republica_corea": "República de Corea", "rusia": "Rusia", "siria": "Siria", "sudafrica": "Sudáfrica",
    "suiza": "Suiza", "uruguay": "Uruguay", "venezuela": "Venezuela", "vietnam": "Vietnam",
    "zf_zonamerica_ex_montevideo_uruguay": "Zona Franca Zonamérica (Uruguay)",
    "resto": "Resto de destinos",
}

_HOJA_RUBRO = "36. Expo. provincia y rubro"
_HOJA_DESTINO = "37. Expo. provincia y destino"


def _provincia(slug: str) -> tuple[str, str] | None:
    """(clave de la jurisdicción, resto del slug) o None."""
    for clave in _SLUGS_ORDENADOS:
        if slug.startswith(clave + "_"):
            return clave, slug[len(clave) + 1:]
    return None


def _sin_prefijo(valor: str, prefijos: tuple[str, ...]) -> str | None:
    for prefijo in prefijos:
        if valor.startswith(prefijo):
            return valor[len(prefijo):]
    return None


def _resultado(flujo: str, apertura: str, grupo: str, nombre: str) -> Clasificacion:
    return Clasificacion(tema=TEMA, indicador=INDICADOR, cobertura=flujo, apertura=apertura, medida=grupo,
                         nombre=nombre, unidad=UNIDAD)


# --- Reglas por familia ----------------------------------------------------------------------------

def _ncm(serie: Serie) -> Clasificacion | None:
    flujo, prefijo = _HOJAS_NCM[serie.hoja]
    slug = _sin_prefijo(serie.variable, (prefijo,))
    if slug is None:
        return None
    if slug == "total":
        return _resultado(flujo, POR_NCM, TOTAL_Y_RESTO, flujo)
    if slug == "resto":
        return _resultado(flujo, POR_NCM, TOTAL_Y_RESTO, unir(flujo, "Resto"))
    capitulo = _capitulo(slug)
    if capitulo is None:
        return None
    return _resultado(flujo, POR_NCM, seccion_de(capitulo), unir(flujo, CAPITULOS[capitulo]))


def _pais(serie: Serie) -> Clasificacion | None:
    flujo, prefijos = _HOJAS_PAIS[serie.hoja]
    slug = _sin_prefijo(serie.variable, prefijos)
    if slug is None:
        return None
    if slug in ("totales", "total"):
        return _resultado(flujo, POR_PAIS, TOTAL, flujo)
    if slug in _PAISES:
        return _resultado(flujo, POR_PAIS, PAISES, unir(flujo, _PAISES[slug]))
    if slug in _BLOQUES:
        return _resultado(flujo, POR_PAIS, BLOQUES, unir(flujo, _BLOQUES[slug]))
    return None


def _por_provincia(serie: Serie) -> Clasificacion | None:
    encontrado = _provincia(serie.variable)
    if encontrado is None:
        return None
    clave, resto = encontrado
    nacional = clave in _TOTALES_PAIS
    grupo = TOTAL_PAIS if nacional else _PROVINCIAS[clave]
    cobertura = "" if nacional else grupo
    if serie.hoja == _HOJA_RUBRO:
        apertura = POR_PROVINCIA_RUBRO
        if resto.startswith("total_"):
            # El total nacional es simplemente «Exportaciones»; el de una jurisdicción lleva «Total».
            dimension = "" if nacional else TOTAL
        elif resto in _RUBROS:
            dimension = _RUBROS[resto]
        else:
            return None
    else:
        apertura = POR_PROVINCIA_DESTINO
        if resto.startswith("total_"):
            dimension = "" if nacional else TOTAL
        elif resto in _DESTINOS:
            dimension = _DESTINOS[resto]
        else:
            return None
    return _resultado(EXPORTACIONES, apertura, grupo, unir(EXPORTACIONES, dimension, cobertura))


def clasificar(serie: Serie) -> Clasificacion | None:
    if serie.fuente != FUENTE or serie.archivo != ARCHIVO:
        return None
    if serie.hoja in _HOJAS_NCM:
        return _ncm(serie)
    if serie.hoja in _HOJAS_PAIS:
        return _pais(serie)
    if serie.hoja in (_HOJA_RUBRO, _HOJA_DESTINO):
        return _por_provincia(serie)
    return None
