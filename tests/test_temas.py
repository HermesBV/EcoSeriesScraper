import unittest

import pandas as pd

from tools.generar_codificacion import _normalize_inventory
from tools.temas import TEMAS, normalizar_tema, unificar_tema_reservas


class TemasTests(unittest.TestCase):
    def test_ocho_temas_y_variantes_conocidas(self):
        self.assertEqual(len(TEMAS), 8)
        self.assertEqual(normalizar_tema("Trabajo"), "Trabajo e ingresos")
        self.assertEqual(normalizar_tema("Salarios"), "Trabajo e ingresos")
        self.assertEqual(normalizar_tema("Riesgo país"), "Mercados financieros")
        self.assertEqual(normalizar_tema("Tipo de cambio"), "Sector externo")
        self.assertEqual(normalizar_tema(""), "Sin clasificar")

    def test_tema_nuevo_exige_revision(self):
        with self.assertRaisesRegex(ValueError, "Revisar si corresponde"):
            normalizar_tema("Tema nuevo")

    def test_generador_aplica_el_control_al_inventario(self):
        inventory = pd.DataFrame([
            {"Código fuente": "fuente", "ID": "serie-1", "Nombre serie": "Serie", "Tema": "Trabajo"},
        ])
        result = _normalize_inventory(inventory)
        self.assertEqual(result.iloc[0]["Tema"], "Trabajo e ingresos")
        inventory.loc[0, "Tema"] = "Tema nuevo"
        with self.assertRaisesRegex(ValueError, "Tema no reconocido"):
            _normalize_inventory(inventory)

    def test_reservas_del_bcra_van_a_sector_externo_y_sus_pasivos_no(self):
        monetario = "Moneda y sistema financiero"
        nombres = {
            "Reservas internacionales del BCRA, en millones de dólares | Reservas internacionales y pasivos": True,
            "Saldo de oro, divisas, colocaciones a plazo y otros activos de reserva (en millones de USD)": True,
            "Variación diaria de reservas internacionales por compra de divisas (en millones de USD)": True,
            "Apertura activo. Apertura de reservas internacionales. Oro neto de previsiones | Estado": True,
            "BALANCE CAMBIARIO - Variación contable de Reservas Internacionales del BCRA - Egresos": True,
            "Pasivos monetarios, en millones de dólares (letras) | Reservas internacionales y pasivos": False,
            "Apertura pasivo. Apertura de obligaciones con organismos internacionales. Contrapartida del uso del tramo de reservas": False,
            "Saldo de Asignaciones de DEGs del año 2009 (en millones de USD)": False,
        }
        inventario = pd.DataFrame(
            [{"Institución": "BCRA", "Nombre serie": nombre, "Tema": monetario} for nombre in nombres]
            + [{"Institución": "INDEC", "Nombre serie": "Reservas internacionales del BCRA", "Tema": "Precios"}]
        )
        resultado = unificar_tema_reservas(inventario)
        for fila, esperado in zip(resultado.itertuples(), nombres.values()):
            self.assertEqual(fila.Tema == "Sector externo", esperado, fila._2)
        self.assertEqual(resultado.iloc[-1]["Tema"], "Precios")


if __name__ == "__main__":
    unittest.main()
