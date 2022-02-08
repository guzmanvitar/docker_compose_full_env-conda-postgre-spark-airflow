import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.types import CHAR, FLOAT, NUMERIC
from typing import Dict, Optional, Union
import pathlib
import logging
import os
import subprocess
import pickle

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


DIR_P = pathlib.Path(__file__).resolve().parents[0]


class BaseProveedores:
    """
    Conexión al mock de las bases de proveedores de cada pais en progresql con métodos básicos para sacar datos.

    Atributos
    ---------
    pais : str
        pais al que corresponde la base
    entorno: str
        entorno de desarrollo, local o produccion
    usuario:
        usuario para la conexión a la database, por defecto el usuario utilizado al crear el docjer compose
    """

    ### CONSTANTES ###
    PAISES = {"uy", "ar", "br", "pe", "py"}

    ENTORNOS_IPS = {
        "local": "database:5432",
        "produccion": "192.168.1.167:1521",
    }

    USUARIOS_PASSWORDS = {
        "admin": "admin",
        "iposs_sel": "iposs_sel1023",
        "dwcla": "DWCLA1122",
    }

    # Diccionario de tipos pandas-sql
    PD_SQL_DICT = {np.dtype("int64"): NUMERIC(), np.dtype("O"): CHAR(120), np.dtype("float64"): FLOAT()}

    ### DEFINICION DE LA CLASE ###
    def __init__(self, pais: str = "br", usuario: str = "iposs_sel", entorno: str = "produccion"):
        """
        Usa credenciales sin permisos de modificación.
        """
        pais_saneado = pais.strip().lower()

        if pais_saneado not in BaseProveedores.PAISES:
            raise ValueError(f"Pais no válido, las opciones son: {BaseProveedores.PAISES}")
        self._pais = pais_saneado

        if entorno not in self.ENTORNOS_IPS:
            raise ValueError("Entorno inválido, debe ser local o producción.")
        self._entorno = entorno

        if (entorno == "produccion") & (usuario not in ["iposs_sel", "dwcla"]):
            raise ValueError(f"Usuario {usuario} inválido para el entorno {entorno}.")
        elif (entorno == "local") & (usuario != "admin"):
            raise ValueError(f"Usuario {usuario} inválido para el entorno {entorno}.")
        self._usuario = usuario

    @property
    def pais(self):
        return self._pais

    @property
    def entorno(self):
        return self._entorno

    @property
    def usuario(self):
        return self._usuario

    ### PROPIEDADES ADICIONALES - CONEXION Y CREDENCIALES ###
    @property
    def conn_string(self):
        if self.entorno != "produccion":
            raise ValueError("El entorno local no utiliza conn_string.")
        else:
            # formato para cx_oracle pero no para sqlalchemy (hojaldre!)
            return f"{self.usuario}/{self.USUARIOS_PASSWORDS[self.usuario]}\
                @{self.ENTORNOS_IPS[self.entorno]}/prvbi{self.pais}1"

    @property
    def credenciales(self):
        if self.entorno == "local":
            return f"postgresql://{self.usuario}:{self.USUARIOS_PASSWORDS[self.usuario]}"\
                f"@{self.ENTORNOS_IPS[self.entorno]}/postgresdb"
        elif self.entorno == "produccion":
            return f"oracle://{self.usuario}:{self.USUARIOS_PASSWORDS[self.usuario]}"\
                f"@{self.ENTORNOS_IPS[self.entorno]}/prvbi{self.pais}1"

    ### DEFINICIONES ADICIONALES ###
    def __eq__(self, other) -> bool:
        return (self.pais == other.pais) & (self.usuario == other.usuario)

    def __repr__(self):
        return f"BaseProveedoresMock({self.pais},{self.usuario})"

    ### METODOS ###
    def ejecutar_consulta(self, consulta: str, params: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """
        Ejecuta consulta en la base y devuelve el resultado en un dataframe.

        También acepta un diccionario de bind parameters {param: valor} donde en la query van de la forma ':param'.
        """
        conexion = create_engine(self.credenciales)

        df_resultado = pd.read_sql(consulta, params=params, con=conexion)

        df_resultado.columns = [col.upper() for col in df_resultado.columns]

        return df_resultado

    def crear_tabla_desde_pandas(
        self,
        path: Union[str, pathlib.Path],
        nombre: str,
        db_schema: str = "dw",
        pd_dtype: Optional[str] = None,
        chunksize: int = 100000,
        *args,
        **kwargs,
    ) -> None:
        """
        Crea vista a partir del dataframe dado. Requiere definir un df_schema con los tipos de las columnas en la base
        y los dtypes del dataframe para que coincidan, ej:
        df_schema = {
            "CODIGO_BARRAS": CHAR(60),
            "CANT_CONTENIDO": FLOAT(),
            "UNIDADES_CONTENIDO": NUMERIC(),
            "FECHA_PROCESO": DATE(),
        }
        """
        # Controles previos
        if (self.entorno == 'produccion'):
            # Chequeo de nombre para la base en produccion
            if self.usuario != 'dwcla':
                raise ValueError(f"El user {self.usuario} no tiene permisos para subir info a la base en produccion.")
            # Chequeo de permisos
            if not nombre.lower().startswith("temp_") or nombre.lower().startswith("tmp_"):
                raise ValueError("En producción el nombre de la vista debe empezar con 'temp_' o 'tmp_'.")
            # Para subir texto unicode (con acentos y pirulos) en el caso oracle
            os.environ["NLS_LANG"] = ".AL32UTF8"

        # Conexion a la base
        conexion = create_engine(self.credenciales)      
        
        # Leemos un primer chunk del dataframe para determinar tipos de datos en las columnas
        base_chunk = pd.read_csv(path, dtype=pd_dtype, nrows=chunksize)
        df_schema = {k.lower(): BaseProveedores.PD_SQL_DICT[base_chunk.dtypes[k]] for k in base_chunk.dtypes.keys()}
        
        cols = [col.lower() for col in base_chunk.columns]

        # Determinamos la cantidad de particiones que vamos a usar
        lineas_df = int(subprocess.check_output(f"wc -l {str(path)}", shell=True).split()[0])
        particiones = int(np.ceil(lineas_df / chunksize))

        # Chequeamos si hay archivos de checkpoint de corridas anteriores
        checkpoint_path = DIR_P.joinpath(f"recovery_{nombre}.pickle")

        if checkpoint_path.exists():
            with open(checkpoint_path, "rb") as f:
                checkpoint = pickle.load(f)
        else:
            checkpoint = 0

        particion = checkpoint

        # Cargamos el dataframe en particiones, si se desconecta la base dumpea un pickle con el numero de la ultima
        # particion cargada con éxito
        for df_particion in pd.read_csv(
            path, dtype=pd_dtype, chunksize=chunksize, skiprows=range(1, chunksize * checkpoint), *args, **kwargs
        ):
            # Registramos la particion que estamos procesando
            particion += 1
            logger.info(f"-- Cargando a la base la particion {particion}/{particiones}")
            
            # Ajustamos columnas
            df_particion.columns = cols
            
            # Cargamos
            df_particion.to_sql(
                name=nombre, con=conexion, schema=db_schema, if_exists="append", index=False, dtype=df_schema,
            )
            # guardamos el numero de la última partición procesada con éxito
            with open(checkpoint_path, "wb") as f:
                pickle.dump(particion, f)

        # Si llegamos al final eliminamos los checkpoints
        if pathlib.Path(checkpoint_path).exists():
            checkpoint_path.unlink()

    def db_table_exists(self, tablename):
        conexion = create_engine(self.credenciales)
        query = f"select * from information_schema.tables where table_name='{tablename}'"

        # return results of sql query from conn as a pandas dataframe
        results_df = pd.read_sql_query(query, conexion)

        # True if we got any results back, False if we didn't
        return bool(len(results_df))
