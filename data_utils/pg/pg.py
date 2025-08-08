import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import asyncpg
from asyncpg import Connection
import pandas as pd
from typing import List, Optional
import logging
import time
from itertools import islice

class SyncPostgresConnector:
    def __init__(self, 
                 dbname: str, 
                 user: str, 
                 password: str, 
                 host: str = 'localhost', 
                 port: int = 5432, 
                 debug: bool = False):
        self.config = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        self.conn = None
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            self.logger.setLevel(logging.DEBUG if debug else logging.INFO)
    
            formatter = logging.Formatter(
                '[%(name)s] [%(asctime)s] [%(levelname)s] => %(message)s',
                '%Y-%m-%d %H:%M:%S'
            )
            
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            
            self.logger.addHandler(handler)

        self.logger.propagate = False

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        """Установить соединение с БД"""
        try:
            self.conn = psycopg2.connect(**self.config)
            self.logger.info("Connected to PostgreSQL successfully")
        except Exception as e:
            self.logger.error(f"Connection error: {e}")
            raise

    def close(self):
        """Закрыть соединение"""
        if self.conn and not self.conn.closed:
            self.conn.close()
            self.logger.info("Connection closed")

    def execute_query(
        self,
        query: str,
        params: Optional[tuple] = None,
        fetch: bool = False,
        with_columns: bool = False
    ) -> Optional[List[tuple]]:
        """
        Выполнить SQL запрос.

        :param query: SQL-запрос
        :param params: Параметры для запроса
        :param fetch: Если True — вернуть результат
        :param with_columns: Если True — вернуть (data, column_names), иначе только data
        :return: В зависимости от параметров — список кортежей, либо tuple(data, column_names)
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params or ())

                if fetch:
                    data = cursor.fetchall()
                    if with_columns:
                        column_names = [desc[0] for desc in cursor.description] if cursor.description else []
                        return data, column_names
                    else:
                        return data
                self.conn.commit()
                return None
        except psycopg2.errors.UndefinedColumn as e:
            self.logger.error(f"{e.diag.message_primary[:250]}...")
            raise
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"Query execution error: {e}\nQuery: {query[:250]}...")
            raise 

    def insert_dataframe(self, df: pd.DataFrame, table: str, page_size: int = 100) -> int:
        """Вставить DataFrame в таблицу"""
        if df.empty:
            return 0

        columns = df.columns.tolist()
        self.logger.debug(f"Column headers of the DataFrame to insert:\n{columns}")
        self.logger.debug(f"Header of the DataFrame to be inserted:\n{df.head()}")
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )

        try:
            with self.conn.cursor() as cursor:  
                start_time = time.time() 

                execute_batch(
                    cursor,
                    query,
                    df.replace({pd.NA: None}).itertuples(index=False, name=None),
                    page_size=page_size
                )
                execution_time = time.time() - start_time
                self.conn.commit()
                self.logger.debug(f"Execution time () - {execution_time:.4f}")
                return len(df)
        except Exception as e:
            self.conn.rollback()
            self.logger.error(f"DataFrame insert error: {e}\nTable: {table}")
            raise

class AsyncPostgresConnector:
    def __init__(self, dbname: str, user: str, password: str, host: str = 'localhost', port: int = 5432, debug = False):
        self.config = {
            'database': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }
        self.conn: Optional[Connection] = None
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            self.logger.setLevel(logging.DEBUG if debug else logging.INFO)
    
            formatter = logging.Formatter(
                '[%(name)s] [%(asctime)s] [%(levelname)s] => %(message)s',
                '%Y-%m-%d %H:%M:%S'
            )
            
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            
            self.logger.addHandler(handler)

        self.logger.propagate = False

    def _setup_logger(self, debug: bool):
        """Настройка логгера"""
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)
        
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '[%(name)s] [%(asctime)s] [%(levelname)s] => %(message)s',
                '%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        self.logger.propagate = False

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def connect(self):
        """Установить соединение с БД"""
        try:
            self.conn = await asyncpg.connect(**self.config)
            self.logger.info("Connected to PostgreSQL successfully")
        except Exception as e:
            self.logger.error(f"Connection error: {e}")
            raise

    async def close(self):
        """Закрыть соединение"""
        if self.conn and not self.conn.is_closed():
            await self.conn.close()
            self.logger.info("Connection closed")

    async def execute_query(self, query: str, params: Optional[list] = None, fetch: bool = False) -> Optional[List[asyncpg.Record]]:
        """Выполнить SQL запрос"""
        try:
            if fetch:
                return await self.conn.fetch(query, *(params or []))
            await self.conn.execute(query, *(params or []))
        except Exception as e:
            self.logger.error(f"Query execution error: {e}\nQuery: {query}")
            raise

    async def insert_dataframe(self, df: pd.DataFrame, table: str, schema: str, batch_size: int = 100) -> int:
        if df.empty:
            return 0

        try:
            from io import BytesIO
            buffer = BytesIO()
            
            df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N', encoding='utf-8')
            buffer.seek(0)
            
            columns = df.columns.tolist()
            start_time = time.time()
            await self.conn.copy_to_table(
                table_name=table,
                schema_name=schema,
                source=buffer,
                columns=columns,
                format='csv',
                delimiter='\t',
                null='\\N'
            )
            execution_time = time.time() - start_time
            self.logger.debug(f"Execution time (COPY) - {execution_time:.4f} sec")
            return len(df)
        except Exception as e:
            self.logger.error(f"DataFrame insert error: {e}\nTable: {table}")
            raise

    async def insert_dataframe_executemany(self, df: pd.DataFrame, table: str, batch_size: int = 100) -> int:

        if df.empty:
            return 0

        columns = df.columns.tolist()
        cols_str = ', '.join(f'"{col}"' for col in columns)
        placeholders = ', '.join(f'${i+1}' for i in range(len(columns)))
        query = f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders})"

        try:
            records = df.replace({pd.NA: None}).itertuples(index=False, name=None)
            
            total = 0
            start_time = time.time()
            async with self.conn.transaction():
                for i in range(0, len(df), batch_size):
                    batch = list(islice(records, batch_size))
                    await self.conn.executemany(query, batch)
                    total += len(batch)
            execution_time = time.time() - start_time
            self.logger.debug(f"Execution time (INSERT) - {execution_time:.4f}")
            
            return total
        except Exception as e:
            self.logger.error(f"DataFrame insert error: {e}\nTable: {table}")
            raise
    
    async def upsert_dataframe(self, df: pd.DataFrame, table: str, conflict_columns: list, update_columns: list = None, batch_size: int = 100) -> int:
        if df.empty:
            return 0

        columns = df.columns.tolist()
        cols_str = ', '.join(f'"{col}"' for col in columns)
        placeholders = ', '.join(f'${i+1}' for i in range(len(columns)))
        

        conflict_cols_str = ', '.join(f'"{col}"' for col in conflict_columns)
        
        if update_columns is None:
            update_columns = [col for col in columns if col not in conflict_columns]
        
        update_set = ', '.join(f'"{col}" = EXCLUDED."{col}"' for col in update_columns)
        
        query = f"""
        INSERT INTO {table} ({cols_str})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_cols_str}) DO UPDATE
        SET {update_set}
        """

        try:
            records = df.replace({pd.NA: None}).itertuples(index=False, name=None)
            
            total = 0
            start_time = time.time()
            async with self.conn.transaction():
                for i in range(0, len(df), batch_size):
                    batch = list(islice(records, batch_size))
                    await self.conn.executemany(query, batch)
                    total += len(batch)
            
            execution_time = time.time() - start_time
            self.logger.info(f"Execution time (UPSERT) - {execution_time:.4f} sec, rows affected: {total}")
            
            return total
        except Exception as e:
            self.logger.error(f"UPSERT error: {e}\nTable: {table}\nQuery: {query}")
            raise

    async def upsert_dataframe_with_ids(self, df: pd.DataFrame, table: str, id_column: str, batch_size: int = 100) -> int:

        if df.empty:
            return 0

        # Проверка индекса
        if df.index.name != id_column:
            raise ValueError(f"Индекс DataFrame должен называться '{id_column}'")

        # Все колонки кроме ID
        data_columns = [col for col in df.columns if col != id_column]
        cols_str = ', '.join(f'"{col}"' for col in [id_column] + data_columns)
        placeholders = ', '.join(f'${i+1}' for i in range(len([id_column] + data_columns)))
        
        query = f"""
        INSERT INTO {table} ({cols_str})
        VALUES ({placeholders})
        ON CONFLICT ({id_column}) DO UPDATE SET
        {', '.join(f'"{col}" = EXCLUDED."{col}"' for col in data_columns)}
        """

        try:
            # Подготовка данных: преобразуем индекс в колонку
            temp_df = df.reset_index()
            records = temp_df[[id_column] + data_columns].to_records(index=False).tolist()
            
            total = 0
            start_time = time.time()
            async with self.conn.transaction():
                # Пакетная вставка через executemany
                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]
                    await self.conn.executemany(query, batch)
                    total += len(batch)
            
            execution_time = time.time() - start_time
            self.logger.debug(f"UPSERT completed in {execution_time:.4f} sec | Rows affected: {total}")
            
            return total
        except Exception as e:
            self.logger.error(f"UPSERT failed: {e}\nTable: {table}")
            raise