import logging
import pathlib
from datetime import datetime
import os
from typing import List

import pandas as pd
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.conf import SparkConf

from bases import BaseProveedores


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#########################
### CONSTANTES UTILES ###
#########################
CONVERSION_TIPO_PERIODO = {"mes": "mensual", "año": "anual"}

LOCAL_HADOOP_PATHS = {
    'ventas_detalladas': '/home/arquimedes/conda_jupyter/system/hadoop_data/ventas_detalladas_muestreada.parquet',
    'ventas_diarias': '/home/arquimedes/conda_jupyter/system/hadoop_data/ventas_diarias_muestreada.parquet',
    'ventas_mensuales': '/home/arquimedes/conda_jupyter/system/hadoop_data/ventas_mensuales_muestreada.parquet',
    'precios_moda': '/home/arquimedes/conda_jupyter/system/hadoop_data/precios_moda_muestreada.parquet',
    }


###########################
### CONFIGURACION SPARK ###
###########################
def crear_sesion_spark(app_name:str, entorno:str = 'produccion'):
    logger.info("*** Iniciando Sesión Spark")

    if entorno == 'produccion':
        spark_conf = SparkConf().setAll([
            ('spark.master', 'yarn'),
            ('spark.app.name', app_name),
            ('spark.ui.showConsoleProgress', 'false'),
            ('spark.driver.memory', "1G"),
            ('spark.driver.cores', "2"),
            ('spark.sql.execution.arrow.pyspark.enabled', 'true'),
        ])       
        
    elif entorno == 'local':
        spark_conf = SparkConf().setAll([
            ('spark.master', 'spark://spark:7077'),
            ('spark.app.name', app_name),
            ('spark.submit.deployMode', 'client'),
            ('spark.ui.showConsoleProgress', 'true'),
            ('spark.eventLog.enabled', 'true'),
            ('spark.logConf', 'true'),
            ('spark.driver.bindAddress', '0.0.0.0'),
            ('spark.dynamicAllocation.executorIdleTimeout', '600'),
            ('spark.driver.host', os.environ.get('LOCAL_IP')),
        ])        
    else:
        raise ValueError("El entorno debe ser 'local' o 'produccion'")

    spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark_ctxt = spark_session.sparkContext
    spark_reader = spark_session.read
    spark_streamReader = spark_session.readStream
    spark_ctxt.setLogLevel("WARN")   

    logger.info("  ---> Sesión Spark iniciada")
    
    return spark_session


########################
### UTILIDADES SPARK ###
########################
def registrar_parquet(spark_session, tabla:str, cols:List[str], pais:str = 'br', periodo:str = None, nombre_vista:str = None, entorno:str = 'produccion'):
    """
    Crea una vista temporal en spark con los dataframes en parquet del hdfs. 
    
    Parámetros:
        spark_session: sesion de spark para el procesamiento
        tabla ({'ventas_detalladas', 'ventas_diarias', 'ventas_mensuales', 'precios_moda'}): la tabla que se quiere acceder. 
        periodo (str): período a considerar para la consulta. 
        nombre_vista (str): nombre de la vista a generar.
        entorno ({'local', 'produccion'}): entorno de trabajo
    """   
    if nombre_vista is None:
        nombre_vista = tabla

    if entorno == 'local':
        path = LOCAL_HADOOP_PATHS[tabla]
        df = pd.read_parquet(path).filter(cols)
        spark_session.createDataFrame(df).createOrReplaceTempView(nombre_vista)

        
    elif entorno == 'produccion':
        if tabla == 'ventas_detalladas':
            path = f"/u01/dw/prod/stage/{pais}/ventas/{periodo}/*"
            
        elif tabla == 'ventas_diarias':
            path = f"/u01/dw/prod/stage/{pais}/snapshots/historico/pfmdiario_{periodo}*.parquet"
            
        elif tabla == 'ventas_mensuales':
            path = f"/u01/dw/prod/stage/{pais}/snapshots/historico/pfmmensual_{periodo}.parquet"

        elif tabla == 'precios_moda':
            path = f"/precios_moda/{pais}/precios_moda.parquet"
        
        else:
            raise ValueError("La tabla debe der ventas_detalladas, ventas_diarias, ventas_mensuales o precios_moda")
        
        spark_session.read.parquet(path).select(cols).createOrReplaceTempView(nombre_vista)
         
        

def crear_vista_oracle(spark_session, query: str, nombre_vista: str, pais: str = "br", entorno: str = "produccion") -> DataFrame:
    """
    Crea una vista temporal con el nombre dado en la base proveedores, a partir de una consulta SQL.

    Devuelve el spark DataFrame.
    """
    if entorno == 'produccion':
        conn_base_proveedores = f"jdbc:oracle:thin:iposs_sel/iposs_sel1023@192.168.1.167:1521/prvbi{pais}1"

        df_spark = (
            (
                spark_session.read.option("encoding", "WE8ISO8859P1")
                .option("characterEncoding", "WE8ISO8859P1")
                .format("jdbc")
                .option("url", conn_base_proveedores)
            )
            .option("dbtable", f"({query})")
            .load()
        )
        # en versiones más nuevas de spark en vez de option dbtable
        # se puede hacer option query (no precisa parentesis)
        # esto de dbtable es medio hacky, crea tabla con la relación pasada y le tira select *

        df_spark.createOrReplaceTempView(nombre_vista)
        
    elif entorno == 'local':
        pandas_df = BaseProveedores('br', 'admin', 'local').ejecutar_consulta(query)
        
        df_spark = spark_session.createDataFrame(pandas_df)
        
        df_spark.createOrReplaceTempView(nombre_vista)
        
    else:
        raise ValueError("El entorno debe ser 'local' o 'produccion'")

    return df_spark


################################
### FUNCIONES PARA PANDAS DF ###
################################
def agregar_precios_impventa_cantventa_dproducto(
    pandas_df: pd.DataFrame,
    pais: str,
    periodo: str = "mes",
    precios: bool = True,
    ventas: bool = True,
    unidades_vendidas: bool = True,
    pdv_con_ventas: bool = True,
    precio_min: bool = True,
    precio_medio: bool = True,
    precio_max: bool = True,
    *,
    entorno: str = 'produccion',
) -> pd.DataFrame:
    """
    Dado un dataframe que contenga codigo (en d_producto) devuelve el mismo dataframe con el agregado de
    las columnas de precio unitario, importe ventas y unidades vendidas. La funcion no se vale de moda
    ni historicos, esto la hace mas lenta, pero sirve para todos los paises.

    Parámetros:
        pandas_df (pandas.DataFrame): csv de entrada.
        pais ({'br', 'ar', 'uy', 'py', 'pe'}): país al cual pedimos los datos.
        periodo ({'mes', 'año'}): período desde el cual queremos traer los datos.
        ventas, precios, unidades_vendidas,pdv_con_ventas ({True,False}): indican si se desea agregar esas columnas
        True por defecto.

    Return:
        df_formateado (pandas.DataFrame): pdf con las columnas agregadas.
    """

    logger.info("*** Agregando info de ventas a nivel de d_producto")

    # por vago quedó bien feo
    logger.debug(
        "Con parámetros"
        " pais, periodo, precios, ventas, unidades_vendidas, pdv_con_ventas, precio_min, precio_medio, precio_max: %s",
        ", ".join(
            map(
                str,
                [
                    pais,
                    periodo,
                    precios,
                    ventas,
                    unidades_vendidas,
                    pdv_con_ventas,
                    precio_min,
                    precio_medio,
                    precio_max,
                ],
            )
        ),
    )
    
    spark_session = crear_sesion_spark('agregar_precios_impventa_cantventa_dproducto', entorno)
    
    # Aplico lstrip en df por si las dudas y ordeno por prod codigo
    pandas_df = pandas_df.assign(PROD_CODIGO=pandas_df["PROD_CODIGO"].astype(str).str.lstrip()).sort_values(
        "PROD_CODIGO"
    )

    spark_session.createDataFrame(pandas_df[["PROD_CODIGO"]]).registerTempTable("prod")

    # Registro la tabla de ventas
    logger.info("*** Registrando informacion de ventas")

    if periodo == "mes":
        periodo_str = (datetime.now() + relativedelta(months=-1)).strftime("%Y%m")
    elif periodo == "año":
        periodo_str = datetime.now().strftime("%Y")
    else:
        raise ValueError(f"Período '{periodo}' no válido.")

    cols = ["prod_codigo", "sum_cant_vta", "sum_imp_vta", "cant_registros", "pdv_codigo"]
    
    registrar_parquet(spark_session, 'ventas_diarias', cols, pais, periodo_str, "ventas", entorno)
    
    logger.info("  ---> Informacion de ventas registrada")

    # True == 1
    # False == 0
    # True * str == str
    # False * str == ''
    query_precios = f"""
        {', percentile(ventas.sum_imp_vta / ventas.sum_cant_vta, 0.1) as PRECIO_MIN' * precio_min}
        {', percentile(ventas.sum_imp_vta / ventas.sum_cant_vta, 0.5) as PRECIO_MEDIO' * precio_medio}
        {', percentile(ventas.sum_imp_vta / ventas.sum_cant_vta, 0.9) as PRECIO_MAX' * precio_max}
    """.strip()  # el .strip es solo para poder tenerla formateada más legible acá y que no aparezcan los \n's después

    query_ventas = ", SUM(ventas.sum_imp_vta) AS IMP_VTA"
    query_tickets = ", SUM(cant_registros)  AS UNIDADES_VENDIDAS"
    query_pdv_ventas = ", COUNT (DISTINCT pdv_codigo)  AS PDV_CON_VENTAS"

    consulta = f""" 
        SELECT prod.PROD_CODIGO 
        {query_precios * precios}
        {query_ventas * ventas}
        {query_tickets * unidades_vendidas}
        {query_pdv_ventas * pdv_con_ventas}
        from prod
          LEFT JOIN ventas
            ON prod.PROD_CODIGO = ventas.prod_codigo
        group by prod.PROD_CODIGO
        """

    logger.info("*** Procesando dataframe final")
    precios_ventas_tickets_join = spark_session.sql(consulta).toPandas()

    salida = pandas_df.merge(precios_ventas_tickets_join, on="PROD_CODIGO")

    logger.info("### Finalizó el agregado de info de ventas a nivel de d_producto")
    spark_session.stop()

    return salida


def agregar_precios_impventa_moda(
    pandas_df: pd.DataFrame,
    year: str = "2020",
    corte_ventas: str = "0",
    ventas: bool = True,
    precios: bool = True,
    *,
    entorno: str = 'produccion',
) -> pd.DataFrame:
    """
    Dado un dataframe que contenga las columnas código de barra y prod_codigo (d_producto) devuelve el mismo dataframe
    con el agregado de las columnas de precio min, precio medio, precio max e imp_vta (importe de ventas en lo que va
    del año). Se utilizan las tablas de precios y ventas moda para acelerar el procesamiento, como contrapartida la
    funcion sirve solo para brasil.

    Parámetros:
        pandas_df (pandas.DataFrame): csv de entrada.
        year (int): año desde el cual queremos las ventas.
        corte_ventas: limita el mínimo de ventas.
        ventas, precios ({True,False}): indican si se desea agregar cada una de estas columnas, True por defecto.

    Return:
        df_formateado (pandas.DataFrame): pdf con las columnas agregadas.
    """

    logger.info("*** Agregando info de precios a nivel de prod_codigo y codigo_barras")
    logger.debug(
        "Con parámetros year, corte_ventas, ventas, precios: %s",
        ", ".join(map(str, [year, corte_ventas, ventas, precios])),
    )

    spark_session = crear_sesion_spark('agregar_precios_impventa_moda', entorno)

    # Aplico lstrip en df por si las dudas
    pandas_df = pandas_df.assign(CODIGO_BARRAS=pandas_df["CODIGO_BARRAS"].astype(str).str.lstrip()).assign(
        PROD_CODIGO=pandas_df["PROD_CODIGO"].astype(str).str.lstrip()
    )

    if precios:
        logger.info("*** Registrando informacion de precios")
        cols = ["codigo_unico", "codigo_barras", "precio"]
    
        registrar_parquet(spark_session, 'precios_moda', cols, 'br', '', "precios", entorno)
        logger.info("  ---> Informacion de precios registrada")

        logger.info("*** Procesando dataframe con precios")
        precios_quantiles = spark_session.sql(
            """
            select
              ltrim(CODIGO_BARRAS)           AS CODIGO_BARRAS,
              percentile_approx(precio, 0.1) as PRECIO_MIN,
              percentile_approx(precio, 0.5) as PRECIO_MEDIO,
              percentile_approx(precio, 0.9) as PRECIO_MAX
            from precios pr
            group by ltrim(CODIGO_BARRAS)
            """
        )

        # Hacemos el join con los codigos del df en spark antes de traer nada para reducir tiempos de procesamiento
        df_cod_spark = spark_session.createDataFrame(pandas_df[["CODIGO_BARRAS"]])
        precios_join_cod = precios_quantiles.join(df_cod_spark, "CODIGO_BARRAS")

        precios_join_pd = (
            precios_join_cod.toPandas()
            .rename(columns={"codigo_barras": "CODIGO_BARRAS"})
            .assign(CODIGO_BARRAS=lambda d: d["CODIGO_BARRAS"].astype(str))
        )

        pandas_df = pandas_df.merge(precios_join_pd, on="CODIGO_BARRAS", how="left")
        logger.info("  ---> Procesamiento de precios listo")

    if ventas:
        logger.info("*** Registrando informacion de ventas")
        cols = ["prod_codigo", "sum_imp_vta"]
    
        registrar_parquet(spark_session, 'ventas_mensuales', cols, 'br', year, "v", entorno)
        logger.info("  ---> Informacion de ventas registrada")

        logger.info("*** Procesando dataframe con ventas")
        spark_session.createDataFrame(pandas_df[["CODIGO_BARRAS", "PROD_CODIGO"]]).createOrReplaceTempView("prod")

        cod_ventas = spark_session.sql(
            f"""
            select
              prod.CODIGO_BARRAS,
              sum(v.sum_imp_vta) imp_vta
            from
              v
                join prod
                     on prod.PROD_CODIGO = v.prod_codigo
            group by prod.CODIGO_BARRAS
            having sum(v.sum_imp_vta) >= {corte_ventas}
            """
        )

        df_ventas = (
            cod_ventas.toPandas()
            .rename(columns={"codigo_barras": "CODIGO_BARRAS"})
            .assign(CODIGO_BARRAS=lambda d: d["CODIGO_BARRAS"].astype(str))
        )

        pandas_df = (
            pandas_df.merge(df_ventas, on="CODIGO_BARRAS", how="left")
            .rename(columns={"imp_vta": "IMP_VTA"})
            .sort_values("IMP_VTA", ascending=False)
        )

        logger.info("  ---> Procesamiento de ventas listp")

    logger.info("Finalizó el agregado de info de precios a nivel de prod_codigo y codigo_barras.")
    spark_session.stop()

    return pandas_df


# def agregar_precio_ventas_cantidad_dempresa(
#     pandas_df: pd.DataFrame,
#     pais: str,
#     periodo: str = "mes",
#     precios: bool = True,
#     ventas: bool = True,
#     unidades_vendidas: bool = True,
#     precio_min: bool = True,
#     precio_medio: bool = True,
#     precio_max: bool = True,
#     con_cache: bool = False,
#     *,
#     spark_master_manager: str = SPARK_MASTER_MANAGER,
#     spark_driver_cores: str = SPARK_DRIVER_CORES,
#     spark_driver_memory: str = SPARK_DRIVER_MEMORY,
# ) -> pd.DataFrame:
#     """
#     Dado un dataframe que contenga prod_emp_codigo (codigo en d_producto_empresa) devuelve
#     el mismo dataframe con el agregado de las columnas de precio unitario, importe ventas y cantidad
#     de tickets.

#     La funcion sirve solo para productos definidos en d_producto_empresa, en particular
#     sirve para los pesables.

#     Los parámetros `precio_{min,medio,max}` agregan granularidad para no computar cosas de más en spark.
#     Por compatibilidad solo afectan si precios == True y alguno de ellos es False.

#     Parámetros:
#         pandas_df (pandas.DataFrame): dataframe conteniendo columna 'CODIGO' con prod_emp_codigo's.
#         pais ({'br', 'ar', 'uy', 'py', 'pe'}): país al cual pedimos los datos.
#         periodo ({'mes', 'año'): mes anterior o año actual.
#         ventas, precios, unidades_vendidas ({True,False}): si se desean agregar estas columnas, True por defecto.
#         precio_{min,medio,max} bool: subselección de columnas de precios.
#         con_cache bool: usar cache o no, por defecto no (por compatibilidad).

#     Return:
#         df_formateado (pandas.DataFrame): df con las columnas agregadas.
#     """

#     logger.info("*** Agregando info de ventas a nivel de prod_emp_codigo")

#     if periodo == "mes":
#         fecha_periodo = (datetime.now() + relativedelta(months=-1)).strftime("%Y%m")
#     elif periodo == "año":
#         fecha_periodo = datetime.now().strftime("%Y")
#     else:
#         raise ValueError(f"Período '{periodo}' no válido.")

#     # me compliqué solo con las subflags de precios, y bue
#     filtro_columnas = [
#         True,
#         precios and precio_min,
#         precios and precio_medio,
#         precios and precio_max,
#         ventas,
#         unidades_vendidas,
#     ]

#     # fue para hacerlo rapido sin numpy o pandas, no me juzguen
#     columnas_info_precios = [
#         col
#         for col, filtro_col in zip(
#             ["CODIGO", "PRECIO_MIN", "PRECIO_MEDIO", "PRECIO_MAX", "IMP_VTA", "UNIDADES_VENDIDAS"], filtro_columnas
#         )
#         if filtro_col
#     ]

#     glob_ventas_spark = f"/u01/dw/prod/stage/{pais}/ventas/{fecha_periodo}*/*"

#     if not con_cache:

#         precios_ventas_tickets = info_precios_prod_empresa(
#             codigos_empresa=pandas_df["CODIGO"],
#             glob_ventas_spark=glob_ventas_spark,
#             spark_master_manager=spark_master_manager,
#             spark_driver_cores=spark_driver_cores,
#             spark_driver_memory=spark_driver_memory,
#         )

#         precios_ventas_tickets = precios_ventas_tickets.filter(columnas_info_precios)

#     else:
#         tipo_periodo = CONVERSION_TIPO_PERIODO[periodo]

#         ruta_cache_precios = pathlib.Path(
#             "/u01/home/ds/deploy/develop/calidad/clasificaciones/tablas"
#             f"/{pais}/precios/d_empresa/{tipo_periodo}/{fecha_periodo}.csv"
#         )

#         if ruta_cache_precios.exists():
#             logger.info("Cargando cache de datos %s", str(ruta_cache_precios))
#             codigos_cacheados = pd.read_csv(ruta_cache_precios, usecols=["CODIGO"], squeeze=False).dropna()

#         else:
#             logger.info("No se encontró cache de datos.")
#             codigos_cacheados = pd.DataFrame(columns=["CODIGO"])

#         # atento al pirulo
#         codigos_faltantes = pandas_df.loc[~pandas_df["CODIGO"].isin(codigos_cacheados["CODIGO"]), ["CODIGO"]]

#         codigos_faltantes = codigos_faltantes.dropna().sort_values("CODIGO").drop_duplicates()

#         cant_codigos_faltantes = codigos_faltantes.shape[0]
#         logger.info("Hay %i prod_emp_codigos sin información de precios.", cant_codigos_faltantes)

#         if not codigos_faltantes.empty:

#             actualizar_cache_precios_prod_emp(
#                 codigos_faltantes,
#                 ruta_cache=ruta_cache_precios,
#                 glob_ventas_spark=glob_ventas_spark,
#                 spark_master_manager=spark_master_manager,
#                 spark_driver_cores=spark_driver_cores,
#                 spark_driver_memory=spark_driver_memory,
#             )

#         logger.info("Leyendo cache de datos %s", str(ruta_cache_precios))

#         precios_ventas_tickets = (
#             pd.read_csv(ruta_cache_precios, usecols=columnas_info_precios)
#             .dropna(subset=["CODIGO"])
#             .drop_duplicates(subset=["CODIGO"])
#         )

#         # para que el merge no tenga problemas de (d)tipo, lo casteamos al mismo tipo que pandas_df
#         dtype_entrada = pandas_df.filter(["CODIGO"]).dtypes.to_dict()
#         precios_ventas_tickets = precios_ventas_tickets.astype(dtype_entrada)

#     # el suffixes es por si hay alguna columna repetida, para que no le cambie el nombre a la original
#     salida = pandas_df.merge(
#         precios_ventas_tickets, on=["CODIGO"], how="left", suffixes=(None, "_info_precios"), validate="1:1"
#     )

#     logger.info("Finalizó agregado de info de ventas a nivel de prod_emp_codigo.")

#     return salida


# def info_precios_prod_empresa(
#     codigos_empresa: pd.Series,
#     glob_ventas_spark: str,
#     *,
#     spark_master_manager: str = SPARK_MASTER_MANAGER,
#     spark_driver_cores: str = SPARK_DRIVER_CORES,
#     spark_driver_memory: str = SPARK_DRIVER_MEMORY,
# ):
#     """
#     Saca sumarizacion de ventas de prod_emp_codigo para los parquets de venta pasados (en forma de glob).
#     """
#     consulta_info_precios = """
#             select
#               pec.codigo,
#               percentile_approx(v.imp_vta / v.cant_vta, 0.1) as precio_min,
#               percentile_approx(v.imp_vta / v.cant_vta, 0.5) as precio_medio,
#               percentile_approx(v.imp_vta / v.cant_vta, 0.9) as precio_max,
#               sum(v.imp_vta)                                 as imp_vta,
#               count(*)                                       as unidades_vendidas
#             from prod_emp_codigos pec
#               inner join ventas v
#                 on pec.codigo = v.prod_emp_codigo
#             where v.imp_vta > 0
#               and v.cant_vta > 0
#               and v.prod_emp_codigo is not null
#             group by pec.codigo
#             """

#     spark_session = (
#         SparkSession.builder.master(spark_master_manager)
#         .appName("precios_prod_emp")
#         .config("spark.driver.memory", spark_driver_memory)
#         .config("spark.driver.cores", spark_driver_cores)
#         .getOrCreate()
#     )
#     spark_session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

#     print("\nSesión Spark iniciada\n")

#     spark_session.createDataFrame(
#         codigos_empresa.dropna().drop_duplicates().sort_values().to_frame()
#     ).registerTempTable("prod_emp_codigos")

#     ventas_spark = spark_session.read.parquet(glob_ventas_spark).select("prod_emp_codigo", "cant_vta", "imp_vta")
#     ventas_spark.registerTempTable("ventas")

#     info_codigos_empresa = (
#         spark_session.sql(consulta_info_precios).toPandas().rename(columns=str.upper).drop_duplicates(subset=["CODIGO"])
#     )

#     logger.info("Cerrando sesión de spark.")
#     spark_session.stop()

#     return info_codigos_empresa


# def actualizar_cache_precios_prod_emp(
#     codigos_faltantes: pd.DataFrame,
#     ruta_cache: pathlib.Path,
#     glob_ventas_spark: str,
#     *,
#     spark_master_manager: str = SPARK_MASTER_MANAGER,
#     spark_driver_cores: str = SPARK_DRIVER_CORES,
#     spark_driver_memory: str = SPARK_DRIVER_MEMORY,
# ):
#     """
#     Hace la consulta para los códigos que faltan y los agrega al cache.

#     Si el cache no existe lo crea.
#     """
#     info_codigos_faltantes = info_precios_prod_empresa(
#         codigos_empresa=codigos_faltantes.squeeze(),
#         glob_ventas_spark=glob_ventas_spark,
#         spark_master_manager=spark_master_manager,
#         spark_driver_cores=spark_driver_cores,
#         spark_driver_memory=spark_driver_memory,
#     )

#     logger.info("Se encontró info de precios de %i prod_emp_codigos.", info_codigos_faltantes["CODIGO"].nunique())

#     info_codigos_faltantes = info_codigos_faltantes.merge(
#         codigos_faltantes, on=["CODIGO"], how="outer", validate="1:1"
#     ).dropna(subset=["CODIGO"])

#     logger.info(
#         "Se agregan prod_emp_codigos sin ventas en el período, totalizando %i codigos.", info_codigos_faltantes.shape[0]
#     )

#     if ruta_cache.exists():

#         logger.info("Agregando info al cache de datos %s", str(ruta_cache))
#         # TIL que .to_csv tiene un modo 'append'
#         info_codigos_faltantes.to_csv(ruta_cache, mode="a", header=False, index=False)

#     else:
#         logger.info("Creando cache de datos %s", str(ruta_cache))

#         ruta_cache.parent.mkdir(exist_ok=True, parents=True)
#         info_codigos_faltantes.to_csv(ruta_cache, index=False)
