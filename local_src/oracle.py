import pandas as pd
from sqlalchemy.dialects.oracle import VARCHAR2
from masa_madre.consultas.bases import BaseProveedores


def consulta_d_producto(year: int, pais: str, class_state: str = None, est_mer2: str = None):
    """
    Realiza una consulta y devuelve un pdv con elementos de D_PRODUCTO

    Parámetros:
        year (int o 'todo') : año desde que queremos los datos. Si ponemos todo es desde los inicios.
        pais ({'br', 'ar', 'uy', 'py', 'pe'}): país al cual pedimos los datos.
        class_state ({None,'clasificados', 'no_clasificados'}): indica si queremos los códigos clasificados,
        no clasificados o todos.
        est_mer2 ({None, 'Food', 'Non_Food'}): indica si queremos o no diferenciar en el est_mer_2.

    Return:
        data_producto (pandas.DataFrame): pdf con la tabla de D_PRODUCTO filtrada según las opciones descritas.
    """

    # Creamos las fracciones de consulta para filtrar la tabla y las ponemos en una lista.
    query_str_list = []

    if year != "todo":
        query_str_list.append(f"FECHA_ULT_MOV > DATE '{year}-01-01'")

    if class_state == "clasificados":
        query_str_list.append("EST_MER_CODIGO <> '0'")
    elif class_state == "no_clasificados":
        query_str_list.append("EST_MER_CODIGO = '0'")

    if est_mer2 == "Food":
        query_str_list.append("EST_MER_2_DESCRIPCION = 'Food'")
    elif est_mer2 == "Non_Food":
        query_str_list.append("EST_MER_2_DESCRIPCION = 'Non Food'")

    # Juntamos los strings de las fracciones de consulta agregando las expresiones where y and donde corresponde.

    if query_str_list != []:
        query_str = "AND " + " AND ".join(query_str_list)
    else:
        query_str = ""

    consulta = f"""
     SELECT *
     FROM DW.D_PRODUCTO
     WHERE CODIGO_BARRAS_SIN_CEROS IS NOT NULL
     {query_str}
     """
    data_producto = (
        BaseProveedores(pais)
        .ejecutar_consulta(consulta)
        .drop("CODIGO_BARRAS", axis=1)
        .rename(columns={"CODIGO_BARRAS_SIN_CEROS": "CODIGO_BARRAS"})
        .assign(CODIGO_BARRAS=lambda d: d["CODIGO_BARRAS"].astype(str), CODIGO=lambda d: d["CODIGO"].astype(str))
    )

    return data_producto


def consulta_d_empresa_full(
    year: str, pais: str, class_state: str = None, est_mer2: str = None, filtrar_por_defecto: bool = False
):
    """
    Realiza una consulta y devuelve un pdv con elementos de D_PRODUCTO_EMPRESA filtrada con columnas de D_PRODUCTO

    Parámetros:
        year (str o 'todo') : año desde que queremos los datos. Si ponemos todo es desde los inicios.
        pais ({'br', 'ar', 'uy', 'py', 'pe'}): país al cual pedimos los datos.
        class_state ({None,'clasificados', 'no_clasificados'}): indica si queremos los códigos clasificados,
        no clasificados o todos.
        est_mer2 ({None, 'Food', 'Non_Food'}): indica si queremos o no diferenciar en el est_mer_2.
        filtrar_por_defecto ({'True', 'False'}): Indica si se quiere eliminar las descripciones por defecto en la
            consulta, False por defecto.

    Return:
        data_empresa (pandas.DataFrame): pdv con la tabla de D_PRODUCTO_EMPRESA
    """

    # Creamos las fracciones de consulta para filtrar la tabla y las ponemos en una lista.
    query_str_list = []

    if year != "todo":
        query_str_list.append(f"prodemp.FECHA_ULT_MOV > DATE '{year}-01-01'")

    if class_state == "clasificados":
        query_str_list.append("prod.EST_MER_CODIGO <> '0'")
    elif class_state == "no_clasificados":
        query_str_list.append("prod.EST_MER_CODIGO = '0'")

    if est_mer2 == "Food":
        query_str_list.append("prod.EST_MER_2_DESCRIPCION = 'Food'")
    elif est_mer2 == "Non_Food":
        query_str_list.append("prod.EST_MER_2_DESCRIPCION = 'Non Food'")

    if filtrar_por_defecto:
        query_str_list.append("prodemp.DESCRIPCION <> 'Por Defecto'")

    # Juntamos los strings de las fracciones de consulta agregando las expresiones where y and donde corresponde.

    if query_str_list != []:
        query_str = "AND " + " AND ".join(query_str_list)
    else:
        query_str = ""

    consulta = f"""
    SELECT ltrim(prodemp.CODIGO_BARRAS, '0') AS CODIGO_BARRAS, prodemp.CODIGO, prodemp.EMP_CODIGO,
            prodemp.ART_CODIGO, prodemp.FECHA_ULT_MOV, prodemp.RUBRO, prodemp.DESCRIPCION,
            COUNT(*) OVER (PARTITION BY ltrim(prodemp.CODIGO_BARRAS, '0'), prodemp.DESCRIPCION) AS CANTIDAD
    FROM DW.D_PRODUCTO_EMPRESA prodemp
    JOIN DW.D_PRODUCTO prod
    ON LTRIM(prod.CODIGO_BARRAS, '0') = LTRIM(prodemp.CODIGO_BARRAS, '0')
    WHERE prodemp.CODIGO_BARRAS IS NOT NULL
    {query_str}
    """

    data_empresa = (
        BaseProveedores(pais)
        .ejecutar_consulta(consulta)
        .assign(
            CODIGO_BARRAS=lambda d: d["CODIGO_BARRAS"].astype(str),
            DESCRIPCION=lambda d: d["DESCRIPCION"].astype(str),
            CANTIDAD=lambda d: d["CANTIDAD"].astype(int),
        )
    )

    return data_empresa


def consulta_d_empresa(
    year: str, pais: str, class_state: str = None, est_mer2: str = None, filtrar_por_defecto: bool = False
):
    """
    Realiza una consulta y devuelve un pdv con elementos de D_PRODUCTO_EMPRESA filtrada con columnas de D_PRODUCTO.
    Es una version mas liviana de la funcion consulta_d_empresa que trae solo las columnas que mas usamos.

    Parámetros:
        year (str o 'todo') : año desde que queremos los datos. Si ponemos todo es desde los inicios.
        pais ({'br', 'ar', 'uy', 'py', 'pe'}): país al cual pedimos los datos.
        class_state ({None,'clasificados', 'no_clasificados'}): indica si queremos los códigos clasificados,
        no clasificados o todos.
        est_mer2 ({None, 'Food', 'Non_Food'}): indica si queremos o no diferenciar en el est_mer_2.
        filtrar_por_defecto ({'True', 'False'}): Indica si se quiere eliminar las descripciones por defecto en la
            consulta, False por defecto.

    Return:
        data_empresa (pandas.DataFrame): pdv con la tabla de D_PRODUCTO_EMPRESA
    """

    # Creamos las fracciones de consulta para filtrar la tabla y las ponemos en una lista.
    query_str_list = []

    if year != "todo":
        query_str_list.append(f"prodemp.FECHA_ULT_MOV > DATE '{year}-01-01'")

    if class_state == "clasificados":
        query_str_list.append("prod.EST_MER_CODIGO <> '0'")
    elif class_state == "no_clasificados":
        query_str_list.append("prod.EST_MER_CODIGO = '0'")

    if est_mer2 == "Food":
        query_str_list.append("prod.EST_MER_2_DESCRIPCION = 'Food'")
    elif est_mer2 == "Non_Food":
        query_str_list.append("prod.EST_MER_2_DESCRIPCION = 'Non Food'")

    if filtrar_por_defecto:
        query_str_list.append("prodemp.DESCRIPCION <> 'Por Defecto'")

    # Juntamos los strings de las fracciones de consulta agregando las expresiones where y and donde corresponde.

    if query_str_list != []:
        query_str = "AND " + " AND ".join(query_str_list)
    else:
        query_str = ""

    consulta = f"""
        SELECT ltrim(prodemp.CODIGO_BARRAS, '0') AS CODIGO_BARRAS, prodemp.DESCRIPCION, COUNT(*) AS CANTIDAD
        FROM DW.D_PRODUCTO_EMPRESA prodemp
        JOIN DW.D_PRODUCTO prod
        ON LTRIM(prod.CODIGO_BARRAS, '0') = LTRIM(prodemp.CODIGO_BARRAS, '0')
        WHERE prodemp.CODIGO_BARRAS IS NOT NULL
        {query_str}
        GROUP BY ltrim(prodemp.CODIGO_BARRAS, '0'), prodemp.DESCRIPCION
        """

    data_empresa = (
        BaseProveedores(pais)
        .ejecutar_consulta(consulta)
        .assign(
            CODIGO_BARRAS=lambda d: d["CODIGO_BARRAS"].astype(str),
            DESCRIPCION=lambda d: d["DESCRIPCION"].astype(str),
            CANTIDAD=lambda d: d["CANTIDAD"].astype(int),
        )
    )

    return data_empresa


def productos_proveedor(proveedor: str):
    """
    Dada un proovedor como nombre en string, devuelve un dataframe con los codigos de barra que ve ese proveedor
    """
    consulta_barras = f"""
        SELECT CODIGO
        FROM DWCLA.P_EMPRESAS
        WHERE DESCRIPCION = '{proveedor}'
        """
    cod_empresa_df = BaseProveedores("br").ejecutar_consulta(consulta_barras)

    if len(cod_empresa_df) == 0:
        return print("No existe un proveedor con ese nombre")
    else:
        cod_empresa = cod_empresa_df.at[0, "CODIGO"]
        consulta_barras = f"""
        SELECT prod.CODIGO_BARRAS, prod.DESCRIPCION AS DESCRIPCION_ACTUAL
        FROM DW.D_PRODUCTO prod
        JOIN DWCLA.D_PRODUCTO_{cod_empresa} prode
        ON prod.CODIGO = prode.CODIGO_SE
        """
        productos_empresa = BaseProveedores("br").ejecutar_consulta(consulta_barras)

        return productos_empresa


def agregar_categorias_barras(pandas_df: pd.DataFrame, pais: str):
    """
    Dado un dataframe que contenga códigos de barra devuelve el mismo dataframe con el agregado
    de la columna de cateogrias definidas a las que pertenece cada codigo de barras.

    Parámetros:
        pandas_df (pandas.DataFrame): csv de entrada.
        pais ({'br', 'ar', 'uy', 'py', 'pe'}): país al cual pedimos los datos.

    Return:
        df_formateado (pandas.DataFrame): pdv con las columnas agregadas.
    """

    # Aplico lstrip en df por si las dudas
    pandas_df = pandas_df.assign(CODIGO_BARRAS=pandas_df["CODIGO_BARRAS"].astype(str).str.lstrip())

    # Datos categorias
    consulta = """
        select codigo_barras, listagg(categoria, ',') within group (order by categoria) categorias
        from (
        select distinct ltrim(p.codigo_barras, '0') codigo_barras,
            c.descripcion || ' (' || em.descripcion || ')' categoria
        from dw.d_producto p
        join iposp.p_estruct_mercadologicas_plana e on e.est_mer_codigo = p.est_mer_codigo
        join iposp.p_categorias c on c.codigo = e.cat_codigo
        join iposp.p_empresas em on em.codigo = c.pemp_codigo
        where em.codigo != 1 and em.descripcion != 'CPG BRASIL' and em.descripcion != 'Prueba')
        group by codigo_barras
        """
    categorias = BaseProveedores(pais).ejecutar_consulta(consulta)
    print("data categorias lista")

    # Formateo
    df_formateado = pandas_df.merge(categorias, on="CODIGO_BARRAS", how="left")

    return df_formateado


def agregar_categorias_barras_prediccion(pandas_df: pd.DataFrame, pais: str = "br"):
    """
    Dado un dataframe que contenga códigos de barra y predicciones de estmer devuelve
    el mismo dataframe con el agregado de la columna de cateogrias a las que perteneceria
    cada codigo si su estmer prediccion es correcto.

    Parámetros:
        pandas_df (pandas.DataFrame): csv de entrada.
        pais ({'br', 'ar', 'uy', 'py', 'pe'}): país al cual pedimos los datos.

    Return:
        df_formateado (pandas.DataFrame): pdv con las columnas agregadas.
    """

    # Preproceso el dataframe
    pandas_df = pandas_df.assign(
        CODIGO_BARRAS=pandas_df["CODIGO_BARRAS"].astype(str).str.lstrip("0"),
        ESTMER_PREDICCION=pandas_df["ESTMER_PREDICCION"].astype(str),
    )
    barras_prediccion = pandas_df.filter(["CODIGO_BARRAS", "ESTMER_PREDICCION"])

    # Creo una tabla temporal en oracle con las barras y su estmer predicho
    base_br = BaseProveedores()
    dataset_schema = {"CODIGO_BARRAS": VARCHAR2(20), "ESTMER_PREDICCION": VARCHAR2(100)}
    base_br.crear_tabla_desde_pandas(barras_prediccion, "temp_estmer_prediccion", schema=dataset_schema)

    # Datos categorias
    consulta = """
        select codigo_barras, listagg(categoria, ',') within group (order by categoria) categorias_prediccion
        from (
        select distinct ltrim(tep.codigo_barras, '0') codigo_barras,
            c.descripcion || ' (' || em.descripcion || ')' categoria
        from dwcla.temp_estmer_prediccion tep
        join iposp.p_estruct_mercadologicas_plana e on e.est_mer_codigo = tep.ESTMER_PREDICCION
        join iposp.p_categorias c on c.codigo = e.cat_codigo
        join iposp.p_empresas em on em.codigo = c.pemp_codigo
        where em.codigo != 1 and em.descripcion != 'CPG BRASIL' and em.descripcion != 'Prueba')
        group by codigo_barras
        """
    categorias = BaseProveedores(pais).ejecutar_consulta(consulta)
    print("data categorias lista")

    # Formateo
    df_formateado = pandas_df.merge(categorias, on="CODIGO_BARRAS", how="left")

    return df_formateado


def agregar_proveedorprobable_barras(pandas_df: pd.DataFrame, pais: str):
    """
    Dado un dataframe que contenga códigos de barra devuelve el mismo dataframe con el agregado
    de la columna de proveedor mas probable en base a la raiz de cada codigo de barras.

    Parámetros:
        pandas_df (pandas.DataFrame): csv de entrada.
        pais ({'br', 'ar', 'uy', 'py', 'pe'}): país al cual pedimos los datos.

    Return:
        df_formateado (pandas.DataFrame): pdv con las columnas agregadas.
    """

    # Definimos una pequeña funcion para obtener los codigos de proveedores del df
    def codigo_proveedores(row):
        if len(row) == 13:
            return row[:-3]
        elif len(row) == 14:
            return row[1:-3]
        else:
            return row

    # Procesamos df, l stripeando por las dudas y agregarndo el codigo proveedores
    pandas_df = pandas_df.assign(CODIGO_BARRAS=pandas_df["CODIGO_BARRAS"].astype(str).str.lstrip()).assign(
        CODIGO_BARRAS_PROVEEDOR=pandas_df["CODIGO_BARRAS"].astype(str).apply(codigo_proveedores)
    )

    # Proveedores posibles
    consulta = """
    SELECT DISTINCT CODIGO_BARRAS_PROVEEDOR,
        FIRST_VALUE("Proveedor") OVER(PARTITION BY CODIGO_BARRAS_PROVEEDOR
                                      ORDER BY CANTIDAD DESC) PROVEEDOR_MAS_PROBABLE
    FROM(
        WITH TABLA AS (
        SELECT ltrim("Codigo Barras SKU", '0') AS CODIGO_BARRAS,
            SUBSTR(ltrim("Codigo Barras SKU", '0'),0,LENGTH(ltrim("Codigo Barras SKU", '0'))-3)
                AS CODIGO_BARRAS_PROVEEDOR, "Proveedor"
        FROM DWCLA.D_PRODUCTO_MODELO
        WHERE LENGTH(ltrim("Codigo Barras SKU", '0')) = 13 AND "Proveedor" != 'OUTRO FABRICANTE'
            AND "Proveedor" != 'SIN PROVEEDOR ASOCIADO'
        UNION
        SELECT ltrim("Codigo Barras SKU", '0') AS CODIGO_BARRAS,
            SUBSTR(ltrim("Codigo Barras SKU", '0'),2,LENGTH(ltrim("Codigo Barras SKU", '0'))-4)
                AS CODIGO_BARRAS_PROVEEDOR, "Proveedor"
        FROM DWCLA.D_PRODUCTO_MODELO
        WHERE LENGTH(ltrim("Codigo Barras SKU", '0')) = 14 AND "Proveedor" != 'OUTRO FABRICANTE'
            AND "Proveedor" != 'SIN PROVEEDOR ASOCIADO')
        SELECT CODIGO_BARRAS_PROVEEDOR, "Proveedor", COUNT(*) AS CANTIDAD
        FROM TABLA
        GROUP BY CODIGO_BARRAS_PROVEEDOR, "Proveedor"
        ORDER BY CODIGO_BARRAS_PROVEEDOR, CANTIDAD DESC)
        """

    proveedores = (
        BaseProveedores(pais)
        .ejecutar_consulta(consulta)
        .assign(CODIGO_BARRAS_PROVEEDOR=lambda d: d["CODIGO_BARRAS_PROVEEDOR"].astype(str))
    )

    # Formateo
    df_formateado = pandas_df.merge(proveedores, on="CODIGO_BARRAS_PROVEEDOR", how="left")

    return df_formateado
