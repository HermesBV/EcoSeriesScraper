import unittest

import pandas as pd

from tools.agrupaciones import completar_agrupaciones


class AgrupacionesTests(unittest.TestCase):
    def test_ipc_conserva_libro_hojas_y_region(self):
        rows = pd.DataFrame([
            {"Código fuente": "indec-ipc", "Archivo origen": "latest.xls",
             "Hoja origen": sheet, "Nombre serie": title,
             "Variable": title.split("|")[-2].strip()}
            for sheet, title in [
                ("Índices IPC Cobertura Nacional",
                 "IPC nacional | Región Cuyo - Alimentos | region-cuyo-alimentos | mensual"),
                ("Índices IPC Cobertura Nacional",
                 "IPC nacional | Región Cuyo - Vivienda | region-cuyo-vivienda | mensual"),
                ("Variación mensual IPC Nacional",
                 "IPC nacional | Región Cuyo - Alimentos | region-cuyo-alimentos | mensual"),
                ("Variación mensual IPC Nacional",
                 "IPC nacional | Región Cuyo - Vivienda | region-cuyo-vivienda | mensual"),
            ]
        ])
        result = completar_agrupaciones(rows)
        self.assertEqual(set(result["Grupo de hojas"]), {"IPC cobertura nacional"})
        self.assertEqual(set(result["Grupo de series 1"]), {"Región Cuyo"})
        self.assertTrue(result["Grupo de series 2"].isna().all())

    def test_no_crea_grupos_solitarios(self):
        rows = pd.DataFrame([{"Código fuente": "indec-ipc", "Archivo origen": "latest.xls",
                             "Hoja origen": "IPC GBA Base dic 2016",
                             "Nombre serie": "IPC | Grupo aislado | Alimentos"}])
        result = completar_agrupaciones(rows)
        self.assertTrue(result["Grupo de hojas"].isna().all())
        self.assertTrue(result["Grupo de series 1"].isna().all())


if __name__ == "__main__":
    unittest.main()
