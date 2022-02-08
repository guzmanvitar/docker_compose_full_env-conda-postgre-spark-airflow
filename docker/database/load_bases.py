# Imports
import pathlib
from bases import BaseProveedores
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
import logging

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

DIR_P = pathlib.Path(__file__).resolve().parents[0]


# Creacion de schemas para la database
progre_con = create_engine("postgresql://admin:admin@database:5432/postgresdb")

database = BaseProveedores('br', 'admin', 'local')

for schema in ["dw", "dwcla"]:
    try:
        progre_con.execute(f"CREATE SCHEMA {schema}")
    except ProgrammingError:
        logger.info(f"El schema {schema} ya existe en la database")


# DPRODUDCTO
if database.db_table_exists("d_producto") & (not DIR_P.joinpath("recovery_d_producto.pickle").exists()):
    logger.info("*** La tabla d_producto ya fue cargada con éxito")

else:
    logger.info("*** Cargando tabla d_producto")

    # Correccion de tipos
    dtype_dprod = {f"EST_MER_{i}_CODIGO": str for i in range(1, 11)}

    # Carga a la base
    database.crear_tabla_desde_pandas(
        DIR_P.joinpath("data/dprod_filtrada.csv"), "d_producto", db_schema="dw", pd_dtype=dtype_dprod,
    )


# DEMPRESA
if database.db_table_exists("d_producto_empresa") & (not DIR_P.joinpath("recovery_d_producto_empresa.pickle").exists()):
    logger.info("*** La tabla d_producto_empresa ya fue cargada con éxito")

else:
    logger.info("*** Cargando tabla d_producto_empresa")

    # Correccion de tipos
    dtype_dempresa = {"ART_CODIGO": str}

    # Carga a la base
    database.crear_tabla_desde_pandas(
        DIR_P.joinpath("data/dempresa_filtrada.csv"), "d_producto_empresa", db_schema="dw", pd_dtype=dtype_dempresa,
    )


# DW BASES LIVIANAS
tablas = [
    path.stem
    for path in DIR_P.glob("data/*.csv")
    if path.stem not in ["dempresa_filtrada", "dprod_filtrada", "d_producto_modelo", "d_producto_14"]
]

for nombre in tablas:
    if database.db_table_exists(f"{nombre}") & (not DIR_P.joinpath(f"recovery_{nombre}.pickle").exists()):
        logger.info(f"*** La tabla {nombre} ya fue cargada con éxito")

    else:
        logger.info(f"*** Cargando tabla {nombre}")

        # Carga a la base
        database.crear_tabla_desde_pandas(
            DIR_P.joinpath(f"data/{nombre}.csv"), f"{nombre}", db_schema="dw",
        )

# DWCLA
tablas = ["d_producto_modelo", "d_producto_14"]

for nombre in tablas:
    if database.db_table_exists(f"{nombre}") & (not DIR_P.joinpath(f"recovery_{nombre}.pickle").exists()):
        logger.info(f"*** La tabla {nombre} ya fue cargada con éxito")

    else:
        logger.info(f"*** Cargando tabla {nombre}")

        # Carga a la base
        database.crear_tabla_desde_pandas(
            DIR_P.joinpath(f"data/{nombre}.csv"), f"{nombre}", db_schema="dwcla",
        )
