import pytest
import pandas as pd
import io
from unittest.mock import MagicMock, patch, ANY
from data_utils.parquet_loader.parquet_loader import ParquetUploader


class TestParquetUploader:
    """Тесты для класса ParquetUploader."""

    @pytest.fixture
    def mock_s3_uploader(self):
        """Фикстура, создающая мок для S3Uploader."""
        mock_uploader = MagicMock()
        # Мокаем метод _resolve_bucket, чтобы он просто возвращал переданный bucket_name
        # или default_bucket
        mock_uploader._resolve_bucket.side_effect = lambda name=None: name or mock_uploader.default_bucket
        mock_uploader.default_bucket = 'test-default-bucket'
        # Мокаем _get_s3_url
        mock_uploader._get_s3_url.return_value = 'https://test-s3.example.com/test-bucket/test-key'
        return mock_uploader

    @pytest.fixture
    def parquet_uploader(self, mock_s3_uploader):
        """Фикстура, создающая экземпляр ParquetUploader с замоканным S3Uploader."""
        return ParquetUploader(mock_s3_uploader)

    @pytest.fixture
    def sample_dataframe(self):
        """Фикстура с тестовым DataFrame."""
        return pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c'],
            'col3': [10.1, 20.2, 30.3]
        })

    @pytest.fixture
    def sample_tuples(self):
        """Фикстура с тестовыми данными в виде списка кортежей."""
        return [(1, 'a', 10.1), (2, 'b', 20.2), (3, 'c', 30.3)]

    # Тесты для upload_dataframe

    def test_upload_dataframe_success(self, parquet_uploader, mock_s3_uploader, sample_dataframe):
        """Тест успешной загрузки DataFrame."""
        s3_key = 'data/test.parquet'
        bucket_name = 'my-bucket'

        with patch('data_utils.parquet_loader.parquet_loader.tempfile.NamedTemporaryFile') as mock_tempfile:
            mock_temp_file_instance = MagicMock()
            mock_temp_file_instance.name = '/tmp/mock_file.parquet'
            mock_tempfile.return_value.__enter__.return_value = mock_temp_file_instance
            mock_tempfile.return_value.__exit__.return_value = None

            with patch('data_utils.parquet_loader.parquet_loader.os.unlink') as mock_unlink:
                parquet_uploader.upload_dataframe(sample_dataframe, s3_key, bucket_name)

                mock_tempfile.assert_called_once_with(suffix='.parquet', delete=False)

                mock_temp_file_instance.write.assert_called()

                mock_s3_uploader.upload_file.assert_called_once_with(
                    local_file_path='/tmp/mock_file.parquet',
                    s3_key=s3_key,
                    bucket_name=bucket_name,
                    skip_if_exists=False
                )

                mock_unlink.assert_called_once_with('/tmp/mock_file.parquet')

    def test_upload_dataframe_with_kwargs(self, parquet_uploader, mock_s3_uploader, sample_dataframe):
        """Тест загрузки DataFrame с дополнительными параметрами."""
        s3_key = 'data/test.parquet'
        parquet_kwargs = {'compression': 'gzip'}

        with patch('data_utils.parquet_loader.parquet_loader.tempfile.NamedTemporaryFile'):
             with patch('data_utils.parquet_loader.parquet_loader.os.unlink'):

                with patch.object(sample_dataframe, 'to_parquet') as mock_to_parquet:
                    parquet_uploader.upload_dataframe(sample_dataframe, s3_key, parquet_kwargs=parquet_kwargs)

                    args, kwargs = mock_to_parquet.call_args
                    assert kwargs.get('compression') == 'gzip'
                    assert kwargs.get('index') is False

    def test_upload_dataframe_exception(self, parquet_uploader, sample_dataframe, capsys):
        """Тест обработки исключения при загрузке DataFrame."""
        s3_key = 'data/test.parquet'
        # Мокаем to_parquet, чтобы он выбрасывал исключение
        with patch.object(sample_dataframe, 'to_parquet', side_effect=Exception("Test error")):
            parquet_uploader.upload_dataframe(sample_dataframe, s3_key)

        captured = capsys.readouterr()
        assert "Ошибка загрузки DataFrame: Test error" in captured.out

    # Тесты для upload_tuples

    def test_upload_tuples_success(self, parquet_uploader, mock_s3_uploader):
        """Тест успешной загрузки списка кортежей."""
        data = [(1, 'a'), (2, 'b')]
        columns = ['id', 'name']
        s3_key = 'data/tuples.parquet'
        bucket_name = 'tuple-bucket'

        with patch.object(parquet_uploader, 'upload_dataframe') as mock_upload_df:
            parquet_uploader.upload_tuples(data, s3_key, columns, bucket_name)

            # Проверяем, что upload_dataframe был вызван с правильным DataFrame
            mock_upload_df.assert_called_once()
            args, kwargs = mock_upload_df.call_args
            df_arg = args[0]
            assert isinstance(df_arg, pd.DataFrame)
            pd.testing.assert_frame_equal(df_arg, pd.DataFrame(data, columns=columns))
            assert args[1] == s3_key
            assert args[2] == bucket_name

    def test_upload_tuples_no_columns(self, parquet_uploader, mock_s3_uploader):
        """Тест загрузки кортежей без указания колонок."""
        data = [(1, 'a'), (2, 'b')]
        s3_key = 'data/tuples_no_cols.parquet'

        with patch.object(parquet_uploader, 'upload_dataframe') as mock_upload_df:
            parquet_uploader.upload_tuples(data, s3_key)

            args, kwargs = mock_upload_df.call_args
            df_arg = args[0]
            expected_df = pd.DataFrame(data)
            pd.testing.assert_frame_equal(df_arg, expected_df)

    def test_upload_tuples_exception(self, parquet_uploader, capsys):
        """Тест обработки исключения при загрузке кортежей."""
        data = [(1, 'a')]
        s3_key = 'data/error.parquet'
        with patch('data_utils.parquet_loader.parquet_loader.pd.DataFrame', side_effect=Exception("DataFrame error")):
            parquet_uploader.upload_tuples(data, s3_key)

        captured = capsys.readouterr()
        assert "Ошибка загрузки данных кортежей: DataFrame error" in captured.out

    # Тесты для download_dataframe

    def test_download_dataframe_success(self, parquet_uploader, mock_s3_uploader):
        """Тест успешного скачивания DataFrame."""
        s3_key = 'data/download.parquet'
        bucket_name = 'download-bucket'
        test_df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
        buffer = io.BytesIO()
        test_df.to_parquet(buffer, index=False)
        buffer.seek(0)
        parquet_bytes = buffer.getvalue()

        def mock_download_fileobj(bucket, key, target_buffer):
            target_buffer.write(parquet_bytes)
            target_buffer.seek(0)

        mock_s3_uploader.s3_client.download_fileobj.side_effect = mock_download_fileobj
        mock_s3_uploader._resolve_bucket.return_value = bucket_name

        result_df = parquet_uploader.download_dataframe(s3_key, bucket_name)

        assert result_df is not None
        pd.testing.assert_frame_equal(result_df, test_df)
        mock_s3_uploader.s3_client.download_fileobj.assert_called_once_with(bucket_name, s3_key, ANY)

    def test_download_dataframe_exception(self, parquet_uploader, mock_s3_uploader, capsys):
        """Тест обработки исключения при скачивании DataFrame."""
        s3_key = 'data/error_download.parquet'
        mock_s3_uploader.s3_client.download_fileobj.side_effect = Exception("Download error")
        mock_s3_uploader._resolve_bucket.return_value = 'error-bucket'

        result_df = parquet_uploader.download_dataframe(s3_key, 'error-bucket')

        assert result_df is None
        captured = capsys.readouterr()
        assert "Ошибка скачивания DataFrame: Download error" in captured.out

    # Тесты для dataframe_to_s3_url

    def test_dataframe_to_s3_url_success(self, parquet_uploader, mock_s3_uploader):
        """Тест успешного сохранения DataFrame и получения URL."""
        test_df = pd.DataFrame({'X': [10, 20]})
        s3_key = 'data/url_test.parquet'
        bucket_name = 'url-bucket'
        expected_url = 'https://test-s3.example.com/url-bucket/data/url_test.parquet'

        with patch.object(parquet_uploader, 'upload_dataframe') as mock_upload_df:

            mock_s3_uploader._resolve_bucket.return_value = bucket_name
            mock_s3_uploader._get_s3_url.return_value = expected_url

            url = parquet_uploader.dataframe_to_s3_url(test_df, s3_key, bucket_name)

            mock_upload_df.assert_called_once_with(test_df, s3_key, bucket_name, False)
            assert url == expected_url

    # Тесты для append_dataframe

    def test_append_dataframe_new_file(self, parquet_uploader, mock_s3_uploader):
        """Тест добавления DataFrame в несуществующий файл (создание нового)."""
        new_df = pd.DataFrame({'A': [1, 2]})
        s3_key = 'data/append_new.parquet'
        bucket_name = 'append-bucket'

        with patch.object(parquet_uploader, 'download_dataframe', side_effect=Exception("File not found")):
            with patch.object(parquet_uploader, 'upload_dataframe') as mock_upload_df:
                result_df = parquet_uploader.append_dataframe(new_df, s3_key, bucket_name)

                mock_upload_df.assert_called_once_with(new_df, s3_key, bucket_name, parquet_kwargs=None)
                pd.testing.assert_frame_equal(result_df, new_df)

    def test_append_dataframe_existing_file(self, parquet_uploader, mock_s3_uploader):
        """Тест добавления DataFrame к существующему файлу."""
        existing_df = pd.DataFrame({'A': [1, 2], 'B': [5, 6]})
        new_df = pd.DataFrame({'A': [3, 4], 'B': [7, 8]})
        expected_combined_df = pd.DataFrame({'A': [1, 2, 3, 4], 'B': [5, 6, 7, 8]})

        s3_key = 'data/append_existing.parquet'

        with patch.object(parquet_uploader, 'download_dataframe', return_value=existing_df):
            with patch.object(parquet_uploader, 'upload_dataframe') as mock_upload_df:
                result_df = parquet_uploader.append_dataframe(new_df, s3_key)

                pd.testing.assert_frame_equal(result_df, expected_combined_df)

                mock_upload_df.assert_called_once()
                args, kwargs = mock_upload_df.call_args
                uploaded_df = args[0]
                pd.testing.assert_frame_equal(uploaded_df, expected_combined_df)

    def test_append_dataframe_exception(self, parquet_uploader, mock_s3_uploader, capsys):
        """Тест обработки исключения при добавлении DataFrame."""
        new_df = pd.DataFrame({'C': [99]})
        s3_key = 'data/append_error.parquet'

        with patch.object(parquet_uploader, 'download_dataframe', side_effect=Exception("Read error")):
            with patch.object(parquet_uploader, 'upload_dataframe') as mock_upload_df:
                result_df = parquet_uploader.append_dataframe(new_df, s3_key)

                mock_upload_df.assert_called_once_with(new_df, s3_key, None, parquet_kwargs=None)
                pd.testing.assert_frame_equal(result_df, new_df)

                captured = capsys.readouterr()
                assert "Файл data/append_error.parquet не существует, создаем новый" in captured.out

    # Тесты для upsert_dataframe и _perform_upsert

    def test_upsert_dataframe_new_file(self, parquet_uploader, mock_s3_uploader):
        """Тест upsert в несуществующий файл (создание нового)."""
        new_df = pd.DataFrame({'id': [1, 2], 'val': ['A', 'B']})
        s3_key = 'data/upsert_new.parquet'
        key_columns = ['id']

        with patch.object(parquet_uploader, 'download_dataframe', return_value=None):
             with patch.object(parquet_uploader, 'upload_dataframe') as mock_upload_df:
                result_df = parquet_uploader.upsert_dataframe(new_df, s3_key, key_columns)

                mock_upload_df.assert_called_once_with(new_df, s3_key, None, parquet_kwargs=None)
                pd.testing.assert_frame_equal(result_df, new_df)

    def test_upsert_dataframe_success(self, parquet_uploader, mock_s3_uploader):
        """Тест успешного upsert."""
        existing_df = pd.DataFrame({'id': [1, 2], 'val': ['A_old', 'B']})
        new_df = pd.DataFrame({'id': [1, 3], 'val': ['A_new', 'C']}) # id=1 обновляется, id=3 добавляется
        # Ожидаемый результат: id=1 с новым значением, id=2 сохраняется, id=3 добавляется
        expected_df = pd.DataFrame({'id': [2, 1, 3], 'val': ['B', 'A_new', 'C']})

        s3_key = 'data/upsert.parquet'
        key_columns = ['id']

        with patch.object(parquet_uploader, 'download_dataframe', return_value=existing_df):
            with patch.object(parquet_uploader, 'upload_dataframe') as mock_upload_df:
                result_df = parquet_uploader.upsert_dataframe(new_df, s3_key, key_columns)

                assert result_df is not None
                # Проверяем, что upload был вызван
                mock_upload_df.assert_called_once()

    def test_perform_upsert_success(self, parquet_uploader):
        """Тест внутреннего метода _perform_upsert."""
        existing_df = pd.DataFrame({'id': [1, 2, 3], 'val': ['A', 'B', 'C']})
        new_df = pd.DataFrame({'id': [1, 4], 'val': ['A_updated', 'D']})
        key_columns = ['id']

        result_df = parquet_uploader._perform_upsert(existing_df, new_df, key_columns)

        # Проверяем результат: id=1 обновлен, id=2 и 3 остались, id=4 добавлен
        # Используем sort_values для стабильного сравнения
        result_sorted = result_df.sort_values('id').reset_index(drop=True)
        expected_sorted = pd.DataFrame({'id': [1, 2, 3, 4], 'val': ['A_updated', 'B', 'C', 'D']}).sort_values('id').reset_index(drop=True)
        pd.testing.assert_frame_equal(result_sorted, expected_sorted)
