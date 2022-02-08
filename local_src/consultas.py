import logging
import warnings

import cx_Oracle as cx
import pandas as pd

logger = logging.getLogger(__name__)


def oracle_query_executor(
    consulta: str,
    pais: str = "br",
    *,  # Esto significa que los argumentos posteriores son keyword only.
    ip: str = "192.168.1.167",
    nombre: str = "prvbi",
    usuario: str = "iposs_sel",
    password: str = "iposs_sel1023",
) -> pd.DataFrame:
    """
    Ejecuta consulta en la base oracle del país dado con el usuario dado.
    """
    warnings.warn(
        "oracle_query_executor esta deprecada; usar bases.BaseProveedores(args).ejecutar_consulta", DeprecationWarning
    )

    con_string = f"{usuario}/{password}@{ip}:1521/{nombre}{pais}1"

    with cx.connect(con_string, encoding="UTF-8", nencoding="UTF-8") as oracle_con:
        df_resultado = pd.read_sql(consulta, con=oracle_con)

    return df_resultado


def filtro_columna_in_valores(columna: str, valores: pd.Series, formato: str = "{}") -> str:
    """
    Arma string de filtro sql "columna in (valores) [or columna in (valores[1000:]) ...]"

    Acepta format string y parte la lista de valores en 'or's de a 1000 valores.
    """

    if valores.isna().sum() > 0:
        logger.warning("Hay %i valores nulos, se ignoran.", valores.isna().sum())
        valores = valores[valores.notnull()]

    # si al groupby le pasás una función en el 'by'
    # la aplica al indice
    # y agrupa en los valores resultantes
    # (qué jugador el groupby la verdá)
    valores_in = (
        valores.apply(formato.format)
        .reset_index(drop=True)
        .groupby(by=lambda i: i // 1000)
        .agg(lambda s: s.str.cat(sep=", "))
        .apply(lambda x: f"({x})")
        .str.cat(sep=f"\n  OR {columna} IN ")
    )

    filtro_valores_in = f"{columna} IN " + valores_in

    return filtro_valores_in


def filtro_columna_like_valores(columna: str, valores: pd.Series, condicion: str = "'%{}%'") -> str:
    """
    Arma string de filtro sql "columna like '%valor1%' [or columna like '%valor2%' ...]"

    Acepta format string para cambiar la condición, por defecto es `columna like '%valor%'`.
    """

    if valores.isna().sum() > 0:
        logger.warning("Hay %i valores nulos. Se ignoran en el filtro LIKE.", valores.isna().sum())

    valores_like = valores.dropna().apply(condicion.format).str.cat(sep=f"\n OR {columna} LIKE ")

    filtro_valores_like = f"{columna} LIKE " + valores_like

    return filtro_valores_like
