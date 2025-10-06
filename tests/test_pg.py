import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, AsyncMock, call
from data_utils.pg.pg import SyncPostgresConnector, AsyncPostgresConnector
import psycopg2
import asyncpg


class TestSyncPostgresConnector:
    """Тесты для класса SyncPostgresConnector."""

    @pytest.fixture
    def mock_connection(self):
        """Фикстура, создающая мок для psycopg2 connection."""
        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = None
        return mock_conn, mock_cursor

    @pytest.fixture
    def sync_connector(self):
        """Фикстура, создающая экземпляр SyncPostgresConnector."""
        return SyncPostgresConnector(
            dbname='test_db',
            user='test_user',
            password='test_password',
            host='localhost',
            port=5432,
            debug=False
        )

    @pytest.fixture
    def sample_dataframe(self):
        """Фикстура с тестовым DataFrame."""
        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })

    # Тесты для __init__

    def test_init_basic(self):
        """Тест базовой инициализации SyncPostgresConnector."""
        connector = SyncPostgresConnector(
            dbname='my_db',
            user='my_user',
            password='my_pass'
        )
        assert connector.config['dbname'] == 'my_db'
        assert connector.config['user'] == 'my_user'
        assert connector.config['password'] == 'my_pass'
        assert connector.config['host'] == 'localhost'
        assert connector.config['port'] == 5432
        assert connector.conn is None

    def test_init_with_custom_host_port(self):
        """Тест инициализации с кастомными хостом и портом."""
        connector = SyncPostgresConnector(
            dbname='my_db',
            user='my_user',
            password='my_pass',
            host='remote.host.com',
            port=5433
        )
        assert connector.config['host'] == 'remote.host.com'
        assert connector.config['port'] == 5433

    # Тесты для context manager

    def test_context_manager(self, sync_connector, mock_connection):
        """Тест использования как контекстного менеджера."""
        mock_conn, _ = mock_connection

        with patch('psycopg2.connect', return_value=mock_conn):
            with sync_connector as conn:
                assert conn is sync_connector
                assert sync_connector.conn is not None

            # Проверяем, что соединение закрыто
            mock_conn.close.assert_called_once()

    # Тесты для connect

    def test_connect_success(self, sync_connector, mock_connection):
        """Тест успешного подключения."""
        mock_conn, _ = mock_connection

        with patch('psycopg2.connect', return_value=mock_conn) as mock_connect:
            sync_connector.connect()

            mock_connect.assert_called_once_with(
                dbname='test_db',
                user='test_user',
                password='test_password',
                host='localhost',
                port=5432
            )
            assert sync_connector.conn == mock_conn

    def test_connect_failure(self, sync_connector):
        """Тест ошибки подключения."""
        with patch('psycopg2.connect', side_effect=psycopg2.Error("Connection failed")):
            with pytest.raises(psycopg2.Error):
                sync_connector.connect()

    # Тесты для close

    def test_close_connection(self, sync_connector, mock_connection):
        """Тест закрытия соединения."""
        mock_conn, _ = mock_connection
        sync_connector.conn = mock_conn

        sync_connector.close()

        mock_conn.close.assert_called_once()

    def test_close_already_closed(self, sync_connector, mock_connection):
        """Тест закрытия уже закрытого соединения."""
        mock_conn, _ = mock_connection
        mock_conn.closed = True
        sync_connector.conn = mock_conn

        sync_connector.close()

        # Не должно вызывать close, если уже закрыто
        mock_conn.close.assert_not_called()

    # Тесты для execute_query

    def test_execute_query_simple(self, sync_connector, mock_connection):
        """Тест выполнения простого запроса."""
        mock_conn, mock_cursor = mock_connection
        sync_connector.conn = mock_conn

        query = "INSERT INTO test_table (name) VALUES (%s)"
        params = ('test_value',)

        sync_connector.execute_query(query, params)

        mock_cursor.execute.assert_called_once_with(query, params)
        mock_conn.commit.assert_called_once()

    def test_execute_query_with_fetch(self, sync_connector, mock_connection):
        """Тест выполнения запроса с получением результата."""
        mock_conn, mock_cursor = mock_connection
        sync_connector.conn = mock_conn

        mock_cursor.fetchall.return_value = [(1, 'Alice'), (2, 'Bob')]

        query = "SELECT * FROM users"
        result = sync_connector.execute_query(query, fetch=True)

        assert result == [(1, 'Alice'), (2, 'Bob')]
        mock_cursor.execute.assert_called_once_with(query, ())
        mock_cursor.fetchall.assert_called_once()

    def test_execute_query_with_columns(self, sync_connector, mock_connection):
        """Тест выполнения запроса с получением имен колонок."""
        mock_conn, mock_cursor = mock_connection
        sync_connector.conn = mock_conn

        mock_cursor.fetchall.return_value = [(1, 'Alice'), (2, 'Bob')]
        mock_cursor.description = [('id',), ('name',)]

        query = "SELECT id, name FROM users"
        result = sync_connector.execute_query(query, fetch=True, with_columns=True)

        data, columns = result
        assert data == [(1, 'Alice'), (2, 'Bob')]
        assert columns == ['id', 'name']

    def test_execute_query_executemany(self, sync_connector, mock_connection):
        """Тест множественной вставки."""
        mock_conn, mock_cursor = mock_connection
        sync_connector.conn = mock_conn

        query = "INSERT INTO test_table (id, name) VALUES (%s, %s)"
        params_list = [(1, 'Alice'), (2, 'Bob'), (3, 'Charlie')]

        sync_connector.execute_query(query, params_list=params_list)

        mock_cursor.executemany.assert_called_once_with(query, params_list)
        mock_conn.commit.assert_called_once()

    def test_execute_query_no_connection(self, sync_connector):
        """Тест выполнения запроса без установленного соединения."""
        with pytest.raises(RuntimeError, match="Database connection is not established"):
            sync_connector.execute_query("SELECT 1")

    def test_execute_query_exception(self, sync_connector, mock_connection):
        """Тест обработки исключения при выполнении запроса."""
        mock_conn, mock_cursor = mock_connection
        sync_connector.conn = mock_conn

        mock_cursor.execute.side_effect = Exception("Query error")

        with pytest.raises(Exception, match="Query error"):
            sync_connector.execute_query("SELECT * FROM nonexistent")

        mock_conn.rollback.assert_called_once()

    # Тесты для insert_dataframe

    def test_insert_dataframe_success(self, sync_connector, mock_connection, sample_dataframe):
        """Тест успешной вставки DataFrame."""
        mock_conn, mock_cursor = mock_connection
        sync_connector.conn = mock_conn

        with patch('data_utils.pg.pg.execute_batch') as mock_execute_batch:
            result = sync_connector.insert_dataframe(sample_dataframe, 'test_table', page_size=100)

            assert result == 3
            mock_execute_batch.assert_called_once()
            mock_conn.commit.assert_called_once()

    def test_insert_dataframe_empty(self, sync_connector):
        """Тест вставки пустого DataFrame."""
        empty_df = pd.DataFrame()
        result = sync_connector.insert_dataframe(empty_df, 'test_table')

        assert result == 0

    def test_insert_dataframe_exception(self, sync_connector, mock_connection, sample_dataframe):
        """Тест обработки исключения при вставке DataFrame."""
        mock_conn, mock_cursor = mock_connection
        sync_connector.conn = mock_conn

        with patch('data_utils.pg.pg.execute_batch', side_effect=Exception("Insert error")):
            with pytest.raises(Exception, match="Insert error"):
                sync_connector.insert_dataframe(sample_dataframe, 'test_table')

            mock_conn.rollback.assert_called_once()


class TestAsyncPostgresConnector:
    """Тесты для класса AsyncPostgresConnector."""

    @pytest.fixture
    def mock_async_connection(self):
        """Фикстура, создающая мок для asyncpg connection."""
        mock_conn = AsyncMock()
        mock_conn.is_closed.return_value = False
        return mock_conn

    @pytest.fixture
    def async_connector(self):
        """Фикстура, создающая экземпляр AsyncPostgresConnector."""
        return AsyncPostgresConnector(
            dbname='test_db',
            user='test_user',
            password='test_password',
            host='localhost',
            port=5432,
            debug=False
        )

    @pytest.fixture
    def sample_dataframe(self):
        """Фикстура с тестовым DataFrame."""
        return pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })

    # Тесты для __init__

    def test_init_basic(self):
        """Тест базовой инициализации AsyncPostgresConnector."""
        connector = AsyncPostgresConnector(
            dbname='my_db',
            user='my_user',
            password='my_pass'
        )
        assert connector.config['database'] == 'my_db'
        assert connector.config['user'] == 'my_user'
        assert connector.config['password'] == 'my_pass'
        assert connector.config['host'] == 'localhost'
        assert connector.config['port'] == 5432
        assert connector.conn is None

    def test_init_with_custom_host_port(self):
        """Тест инициализации с кастомными хостом и портом."""
        connector = AsyncPostgresConnector(
            dbname='my_db',
            user='my_user',
            password='my_pass',
            host='remote.host.com',
            port=5433
        )
        assert connector.config['host'] == 'remote.host.com'
        assert connector.config['port'] == 5433

    # Тесты для async context manager

    @pytest.mark.asyncio
    async def test_async_context_manager(self, async_connector, mock_async_connection):
        """Тест использования как асинхронного контекстного менеджера."""
        with patch('asyncpg.connect', return_value=mock_async_connection):
            async with async_connector as conn:
                assert conn is async_connector
                assert async_connector.conn is not None

            # Проверяем, что соединение закрыто
            mock_async_connection.close.assert_called_once()

    # Тесты для connect

    @pytest.mark.asyncio
    async def test_connect_success(self, async_connector, mock_async_connection):
        """Тест успешного асинхронного подключения."""
        with patch('asyncpg.connect', return_value=mock_async_connection) as mock_connect:
            await async_connector.connect()

            mock_connect.assert_called_once_with(
                database='test_db',
                user='test_user',
                password='test_password',
                host='localhost',
                port=5432
            )
            assert async_connector.conn == mock_async_connection

    @pytest.mark.asyncio
    async def test_connect_failure(self, async_connector):
        """Тест ошибки асинхронного подключения."""
        with patch('asyncpg.connect', side_effect=Exception("Connection failed")):
            with pytest.raises(Exception, match="Connection failed"):
                await async_connector.connect()

    # Тесты для close

    @pytest.mark.asyncio
    async def test_close_connection(self, async_connector, mock_async_connection):
        """Тест закрытия асинхронного соединения."""
        async_connector.conn = mock_async_connection

        await async_connector.close()

        mock_async_connection.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_already_closed(self, async_connector, mock_async_connection):
        """Тест закрытия уже закрытого асинхронного соединения."""
        mock_async_connection.is_closed.return_value = True
        async_connector.conn = mock_async_connection

        await async_connector.close()

        # Не должно вызывать close, если уже закрыто
        mock_async_connection.close.assert_not_called()

    # Тесты для execute_query

    @pytest.mark.asyncio
    async def test_execute_query_simple(self, async_connector, mock_async_connection):
        """Тест выполнения простого асинхронного запроса."""
        async_connector.conn = mock_async_connection

        query = "INSERT INTO test_table (name) VALUES ($1)"
        params = ['test_value']

        await async_connector.execute_query(query, params)

        mock_async_connection.execute.assert_called_once_with(query, 'test_value')

    @pytest.mark.asyncio
    async def test_execute_query_with_fetch(self, async_connector, mock_async_connection):
        """Тест выполнения асинхронного запроса с получением результата."""
        async_connector.conn = mock_async_connection

        mock_result = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
        mock_async_connection.fetch.return_value = mock_result

        query = "SELECT * FROM users"
        result = await async_connector.execute_query(query, fetch=True)

        assert result == mock_result
        mock_async_connection.fetch.assert_called_once_with(query)

    @pytest.mark.asyncio
    async def test_execute_query_exception(self, async_connector, mock_async_connection):
        """Тест обработки исключения при выполнении асинхронного запроса."""
        async_connector.conn = mock_async_connection

        mock_async_connection.execute.side_effect = Exception("Query error")

        with pytest.raises(Exception, match="Query error"):
            await async_connector.execute_query("SELECT * FROM nonexistent")

    # Тесты для insert_dataframe

    @pytest.mark.asyncio
    async def test_insert_dataframe_success(self, async_connector, mock_async_connection, sample_dataframe):
        """Тест успешной асинхронной вставки DataFrame с COPY."""
        async_connector.conn = mock_async_connection

        result = await async_connector.insert_dataframe(sample_dataframe, 'test_table', 'public', batch_size=100)

        assert result == 3
        mock_async_connection.copy_to_table.assert_called_once()

    @pytest.mark.asyncio
    async def test_insert_dataframe_empty(self, async_connector):
        """Тест асинхронной вставки пустого DataFrame."""
        empty_df = pd.DataFrame()
        result = await async_connector.insert_dataframe(empty_df, 'test_table', 'public')

        assert result == 0

    @pytest.mark.asyncio
    async def test_insert_dataframe_exception(self, async_connector, mock_async_connection, sample_dataframe):
        """Тест обработки исключения при асинхронной вставке DataFrame."""
        async_connector.conn = mock_async_connection
        mock_async_connection.copy_to_table.side_effect = Exception("Insert error")

        with pytest.raises(Exception, match="Insert error"):
            await async_connector.insert_dataframe(sample_dataframe, 'test_table', 'public')

    # Тесты для insert_dataframe_executemany

    @pytest.mark.asyncio
    async def test_insert_dataframe_executemany_success(self, async_connector, mock_async_connection, sample_dataframe):
        """Тест успешной асинхронной вставки DataFrame через executemany."""
        async_connector.conn = mock_async_connection

        # Мокаем transaction
        mock_transaction = AsyncMock()
        mock_async_connection.transaction.return_value.__aenter__.return_value = mock_transaction
        mock_async_connection.transaction.return_value.__aexit__.return_value = None

        result = await async_connector.insert_dataframe_executemany(sample_dataframe, 'test_table', batch_size=2)

        assert result == 3
        # Проверяем, что executemany был вызван
        assert mock_async_connection.executemany.call_count >= 1

    @pytest.mark.asyncio
    async def test_insert_dataframe_executemany_empty(self, async_connector):
        """Тест асинхронной вставки пустого DataFrame через executemany."""
        empty_df = pd.DataFrame()
        result = await async_connector.insert_dataframe_executemany(empty_df, 'test_table')

        assert result == 0

    # Тесты для upsert_dataframe

    @pytest.mark.asyncio
    async def test_upsert_dataframe_success(self, async_connector, mock_async_connection, sample_dataframe):
        """Тест успешного upsert DataFrame."""
        async_connector.conn = mock_async_connection

        # Мокаем transaction
        mock_transaction = AsyncMock()
        mock_async_connection.transaction.return_value.__aenter__.return_value = mock_transaction
        mock_async_connection.transaction.return_value.__aexit__.return_value = None

        conflict_columns = ['id']
        update_columns = ['name', 'age']

        result = await async_connector.upsert_dataframe(
            sample_dataframe,
            'test_table',
            conflict_columns,
            update_columns,
            batch_size=2
        )

        assert result == 3
        assert mock_async_connection.executemany.call_count >= 1

    @pytest.mark.asyncio
    async def test_upsert_dataframe_auto_update_columns(self, async_connector, mock_async_connection, sample_dataframe):
        """Тест upsert DataFrame с автоматическим определением update_columns."""
        async_connector.conn = mock_async_connection

        # Мокаем transaction
        mock_transaction = AsyncMock()
        mock_async_connection.transaction.return_value.__aenter__.return_value = mock_transaction
        mock_async_connection.transaction.return_value.__aexit__.return_value = None

        conflict_columns = ['id']

        result = await async_connector.upsert_dataframe(
            sample_dataframe,
            'test_table',
            conflict_columns,
            batch_size=2
        )

        assert result == 3

    @pytest.mark.asyncio
    async def test_upsert_dataframe_empty(self, async_connector):
        """Тест upsert пустого DataFrame."""
        empty_df = pd.DataFrame()
        result = await async_connector.upsert_dataframe(empty_df, 'test_table', ['id'])

        assert result == 0

    # Тесты для upsert_dataframe_with_ids

    @pytest.mark.asyncio
    async def test_upsert_dataframe_with_ids_success(self, async_connector, mock_async_connection):
        """Тест успешного upsert DataFrame с использованием индекса как ID."""
        async_connector.conn = mock_async_connection

        # Создаем DataFrame с именованным индексом
        df = pd.DataFrame({
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
        df.index = pd.Index([1, 2, 3], name='user_id')

        # Мокаем transaction
        mock_transaction = AsyncMock()
        mock_async_connection.transaction.return_value.__aenter__.return_value = mock_transaction
        mock_async_connection.transaction.return_value.__aexit__.return_value = None

        result = await async_connector.upsert_dataframe_with_ids(df, 'test_table', 'user_id', batch_size=2)

        assert result == 3
        assert mock_async_connection.executemany.call_count >= 1

    @pytest.mark.asyncio
    async def test_upsert_dataframe_with_ids_wrong_index(self, async_connector):
        """Тест upsert DataFrame с неправильным именем индекса."""
        df = pd.DataFrame({
            'name': ['Alice', 'Bob'],
            'age': [25, 30]
        })
        df.index.name = 'wrong_id'

        with pytest.raises(ValueError, match="Индекс DataFrame должен называться 'user_id'"):
            await async_connector.upsert_dataframe_with_ids(df, 'test_table', 'user_id')

    @pytest.mark.asyncio
    async def test_upsert_dataframe_with_ids_empty(self, async_connector):
        """Тест upsert пустого DataFrame с ID."""
        empty_df = pd.DataFrame()
        empty_df.index.name = 'user_id'
        result = await async_connector.upsert_dataframe_with_ids(empty_df, 'test_table', 'user_id')

        assert result == 0
