# Tarea 3 – Procesamiento de datos con Apache Spark

Curso: Big Data (código 202016911) – UNAD

## Problema

Analizar qué variables ambientales influyen más en la energía eléctrica que produce una planta de ciclo combinado, para entender cómo afectan las condiciones del entorno a su rendimiento.

## Dataset

**Combined Cycle Power Plant (`Folds5x2_pp`)**: 9.568 registros con medidas horarias de una planta eléctrica.

| Columna | Descripción |
|---|---|
| AT | Temperatura ambiente (°C) |
| V | Vacío de escape (cm Hg) |
| AP | Presión ambiente (mbar) |
| RH | Humedad relativa (%) |
| PE | Energía eléctrica producida (MW) |

El archivo original (`datos/Folds5x2_pp.xlsx`) tiene 5 hojas con los mismos datos en distinto orden; se usa solo `Sheet1`.

## Qué hace el procesamiento batch (`batch_spark.py`)

1. **Carga** del CSV con Spark y revisión del esquema.
2. **Limpieza**: conteo de nulos, eliminación de duplicados y corrección de humedades mayores al 100 %.
3. **Transformación**: nueva columna `rango_temp` (<10, 10-20, 20-30, >=30 °C).
4. **Operaciones con RDD**: `map`, `filter`, `reduceByKey`, `count`, `take`, `reduce`.
5. **EDA con DataFrames**: estadísticas descriptivas, correlación con `PE` y promedio de `PE` por rango de temperatura.
6. **Visualización**: tres gráficas guardadas en `resultados/`.

## Requisitos

- Python 3.12
- Java 17 (JDK)
- Librerías: ver `requirements.txt`

## Cómo ejecutarlo

```bash
python -m venv venv
# Windows (PowerShell)
.\venv\Scripts\Activate.ps1
# Linux / Mac
source venv/bin/activate

pip install -r requirements.txt

python convertir_a_csv.py     # genera datos/dataset.csv (una sola vez)
python batch_spark.py
```

Los resultados quedan en la carpeta `resultados/`:
`correlaciones.png`, `pe_por_rango.png`, `dispersion_at_pe.png` y `pe_por_rango.csv`.

## Notas

- En Windows, Spark muestra avisos sobre `winutils.exe` y `HADOOP_HOME`: no afectan la ejecución.
- Se usa `pandas<3` porque PySpark aún no soporta del todo pandas 3.0.