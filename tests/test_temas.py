import unittest

import pandas as pd

from tools.generar_codificacion import _normalize_inventory
from tools.temas import TEMAS, normalizar_tema


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


if __name__ == "__main__":
    unittest.main()
