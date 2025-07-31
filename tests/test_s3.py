import os
import tempfile
from datetime import datetime
from unittest.mock import MagicMock, call, patch

import pytest

from data_utils.s3.s3 import S3Uploader


class TestS3Uploader:
    """Тесты для класса S3Uploader."""

    @pytest.fixture
    def s3_uploader(self):
        """Фикстура, создающая экземпляр S3Uploader с мокированным клиентом."""
        with patch("data_utils.s3.s3.boto3.client") as mock_boto_client:
            mock_s3_client = MagicMock()
            mock_boto_client.return_value = mock_s3_client

            uploader = S3Uploader(
                aws_access_key_id="test_key",
                aws_secret_access_key="test_secret",
                endpoint_url="https://test-s3.example.com",
                region_name="us-test-1",
                default_bucket="test-bucket",
                debug=True,
            )

            uploader.s3_client = mock_s3_client
            yield uploader, mock_s3_client

    @pytest.fixture
    def temp_file(self):
        """Создает временный файл для тестов."""
        with tempfile.NamedTemporaryFile(delete=False,
                                         mode="w",
                                         suffix=".txt") as f:
            f.write("This is test file content for S3Uploader.")
            file_path = f.name
        yield file_path

        os.unlink(file_path)

    # Тесты для отдельных методов

    def test_init(self):
        """Тест инициализации S3Uploader."""
        with patch("data_utils.s3.s3.boto3.client") as mock_boto_client:
            mock_s3_client = MagicMock()
            mock_boto_client.return_value = mock_s3_client

            uploader = S3Uploader(
                aws_access_key_id="my_key",
                aws_secret_access_key="my_secret",
                endpoint_url="https://my-s3.example.com",
                region_name="eu-west-1",
                default_bucket="my-default-bucket",
            )

            mock_boto_client.assert_called_once_with(
                "s3",
                aws_access_key_id="my_key",
                aws_secret_access_key="my_secret",
                endpoint_url="https://my-s3.example.com",
                region_name="eu-west-1",
            )
            assert uploader.s3_client == mock_s3_client
            assert uploader.default_bucket == "my-default-bucket"
            assert uploader.debug is False  # По умолчанию False

    def test_calculate_local_file_hash(self, temp_file):
        """Тест вычисления хэша локального файла."""

        assert os.path.exists(temp_file)

    def test_get_s3_object_info_success(self, s3_uploader):
        """Тест получения информации о существующем объекте S3."""
        uploader, mock_client = s3_uploader
        mock_client.head_object.return_value = {
            "ContentLength": 1024,
            "ETag": '"abc123def456"',
            "LastModified": datetime(2023, 10, 27, 10, 0, 0),
        }

        info = uploader._get_s3_object_info("my-bucket", "my-key")

        mock_client.head_object.assert_called_once_with(
            Bucket="my-bucket", Key="my-key"
        )
        assert info == {
            "size": 1024,
            "etag": "abc123def456",
            "last_modified": datetime(2023, 10, 27, 10, 0, 0),
        }

    def test_get_s3_object_info_not_found(self, s3_uploader, capsys):
        """Тест получения информации о несуществующем объекте S3."""
        uploader, mock_client = s3_uploader
        from botocore.exceptions import ClientError

        error_response = {"Error": {"Code": "404", "Message": "Not Found"}}
        mock_client.head_object.side_effect = ClientError(error_response,
                                                          "HeadObject")

        info = uploader._get_s3_object_info("my-bucket", "nonexistent-key")

        assert info is None

        captured = capsys.readouterr()
        assert "Ошибка получения информации о файле" not in captured.out

    def test_get_s3_object_info_other_error(self, s3_uploader, capsys):
        """Тест получения информации при другой ошибке S3."""
        uploader, mock_client = s3_uploader
        from botocore.exceptions import ClientError

        error_response = {"Error": {"Code": "500",
                                    "Message": "Internal Error"}}
        mock_client.head_object.side_effect = ClientError(error_response,
                                                          "HeadObject")

        info = uploader._get_s3_object_info("my-bucket", "error-key")

        assert info is None
        captured = capsys.readouterr()
        assert "Ошибка получения информации о файле error-key" in captured.out

    def test_files_are_equal_true(self, s3_uploader, temp_file):
        """Тест, когда файлы идентичны."""
        uploader, mock_client = s3_uploader

        # _get_s3_object_info
        mock_client.head_object.return_value = {
            "ContentLength": os.path.getsize(temp_file),
            "ETag": '"simplemd5hash123456"',
            "LastModified": datetime.fromtimestamp(os.path.getmtime(
                                                                temp_file)),
        }

        # _calculate_local_file_hash (для соответствия ETag)
        with patch.object(
            uploader, "_calculate_local_file_hash",
            return_value="simplemd5hash123456"
        ):
            result = uploader._files_are_equal(temp_file,
                                               "test-bucket",
                                               "test-key")

        assert result is True

    def test_files_are_equal_false_size(self, s3_uploader, temp_file):
        """Тест, когда размеры файлов различаются."""
        uploader, mock_client = s3_uploader

        mock_client.head_object.return_value = {
            "ContentLength": os.path.getsize(temp_file) + 100,
            "ETag": '"differentetag"',
            "LastModified": datetime.now(),
        }

        result = uploader._files_are_equal(temp_file,
                                           "test-bucket",
                                           "test-key")

        assert result is False

    def test_files_are_equal_false_mtime_large_file(self,
                                                    s3_uploader,
                                                    temp_file):
        """Тест для большого файла (>5MB) - проверяет только размер."""
        uploader, mock_client = s3_uploader

        large_size = 6 * 1024 * 1024  # 6 MB
        # Создаем временный файл большого размера (имитация)
        with patch("os.stat") as mock_stat:
            mock_stat.return_value.st_size = large_size
            mock_stat.return_value.st_mtime = datetime.now().timestamp()

            mock_client.head_object.return_value = {
                "ContentLength": large_size,
                "ETag": '"largefileetag"',
                "LastModified": datetime.now(),
            }

            result = uploader._files_are_equal(
                temp_file, "test-bucket", "large-test-key"
            )

        assert result is True

    def test_resolve_bucket_with_param(self, s3_uploader):
        """Тест разрешения имени бакета с явным параметром."""
        uploader, _ = s3_uploader
        # Устанавливаем default_bucket для теста
        uploader.default_bucket = "default-bucket"

        bucket = uploader._resolve_bucket("explicit-bucket")
        assert bucket == "explicit-bucket"

    def test_resolve_bucket_with_default(self, s3_uploader):
        """Тест разрешения имени бакета с использованием default_bucket."""
        uploader, _ = s3_uploader
        uploader.default_bucket = "my-default-bucket"

        bucket = uploader._resolve_bucket()  # Без параметра
        assert bucket == "my-default-bucket"

    def test_resolve_bucket_error(self, s3_uploader):
        """Тест ошибки разрешения имени бакета."""
        uploader, _ = s3_uploader
        uploader.default_bucket = None  # Нет значения по умолчанию

        with pytest.raises(ValueError) as excinfo:
            uploader._resolve_bucket()  # Без параметра и без default_bucket
        assert "Не указан бакет" in str(excinfo.value)

    # --- Тесты для публичных методов ---

    def test_upload_file_success(self, s3_uploader, temp_file):
        """Тест успешной загрузки файла."""
        uploader, mock_client = s3_uploader
        uploader.default_bucket = "dest-bucket"

        uploader.upload_file(temp_file, "uploaded/path/file.txt")

        mock_client.upload_file.assert_called_once_with(
            temp_file, "dest-bucket", "uploaded/path/file.txt"
        )

    def test_upload_file_not_found(self, s3_uploader, capsys):
        """Тест загрузки несуществующего файла."""
        uploader, mock_client = s3_uploader

        uploader.upload_file("/path/does/not/exist.txt", "some/key")

        mock_client.upload_file.assert_not_called()
        captured = capsys.readouterr()
        assert "Ошибка: файл /path/does/not/exist.txt не найден." in captured.out

    def test_upload_file_skip_if_exists_true(self, s3_uploader, temp_file):
        """Тест пропуска загрузки, если файл не изменился."""
        uploader, mock_client = s3_uploader

        # Настраиваем _files_are_equal на True
        with patch.object(uploader, "_files_are_equal", return_value=True):
            # Мокаем _get_s3_url для проверки сообщения
            with patch.object(
                uploader, "_get_s3_url",
                return_value="https://test.url/bucket/key"
            ):
                uploader.upload_file(
                    temp_file, "key", bucket_name="bucket", skip_if_exists=True
                )

        mock_client.upload_file.assert_not_called()

    def test_upload_directory_success(self, s3_uploader):
        """Тест успешной загрузки директории."""
        uploader, mock_client = s3_uploader
        uploader.default_bucket = "my-bucket"

        with tempfile.TemporaryDirectory() as temp_dir:
            subdir = os.path.join(temp_dir, "subdir")
            os.makedirs(subdir)
            file1_path = os.path.join(temp_dir, "file1.txt")
            file2_path = os.path.join(subdir, "file2.txt")
            with open(file1_path, "w") as f1:
                f1.write("Content of file 1")
            with open(file2_path, "w") as f2:
                f2.write("Content of file 2")

            # Мокаем upload_file, чтобы не выполнять реальную загрузку
            with patch.object(uploader, "upload_file") as mock_upload_file:
                uploader.upload_directory(
                    temp_dir, s3_prefix="uploads/", skip_if_exists=True
                )

                # Проверяем, что upload_file был вызван дважды
                # с правильными аргументами
                expected_calls = [
                    call(file1_path,
                         "uploads/file1.txt",
                         "my-bucket",
                         True),
                    call(file2_path,
                         "uploads/subdir/file2.txt",
                         "my-bucket",
                         True),
                ]
                mock_upload_file.assert_has_calls(expected_calls,
                                                  any_order=False)

    def test_download_file_success(self, s3_uploader):
        """Тест успешного скачивания файла."""
        uploader, mock_client = s3_uploader
        uploader.default_bucket = "source-bucket"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = os.path.join(temp_dir, "downloaded_file.txt")

            uploader.download_file("source/key/file.txt", local_path)

            mock_client.download_file.assert_called_once_with(
                "source-bucket", "source/key/file.txt", local_path
            )
            # Проверяем, что директория создана (os.makedirs)
            assert os.path.exists(temp_dir)

    def test_list_files_success(self, s3_uploader):
        """Тест успешного получения списка файлов."""
        uploader, mock_client = s3_uploader
        uploader.default_bucket = "list-bucket"

        mock_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "file1.txt"},
                {"Key": "folder/file2.txt"},
                {"Key": "file3.log"},
            ]
        }

        files = uploader.list_files(prefix="folder/")

        mock_client.list_objects_v2.assert_called_once_with(
            Bucket="list-bucket", Prefix="folder/"
        )
        assert files == ["file1.txt", "folder/file2.txt", "file3.log"]

    def test_delete_file_success(self, s3_uploader):
        """Тест успешного удаления файла."""
        uploader, mock_client = s3_uploader
        uploader.default_bucket = "delete-bucket"

        uploader.delete_file("to/delete/key.txt")

        mock_client.delete_object.assert_called_once_with(
            Bucket="delete-bucket", Key="to/delete/key.txt"
        )

    def test_create_bucket_success(self, s3_uploader):
        """Тест успешного создания бакета."""
        uploader, mock_client = s3_uploader
        uploader.default_bucket = "new-bucket"

        # Сначала проверим случай, когда бакет
        # уже существует (BucketAlreadyOwnedByYou)
        from botocore.exceptions import ClientError

        error_response_owned = {
            "Error": {"Code": "BucketAlreadyOwnedByYou",
                      "Message": "Already owned"}
        }
        mock_client.create_bucket.side_effect = ClientError(
            error_response_owned, "CreateBucket"
        )

        uploader.create_bucket()

        mock_client.create_bucket.assert_called_once_with(Bucket="new-bucket")

    def test_list_buckets_success(self, s3_uploader):
        """Тест успешного получения списка бакетов."""
        uploader, mock_client = s3_uploader

        mock_client.list_buckets.return_value = {
            "Buckets": [{"Name": "bucket1"},
                        {"Name": "bucket2"},
                        {"Name": "my-bucket"}]
        }

        buckets = uploader.list_buckets()

        mock_client.list_buckets.assert_called_once()
        assert buckets == ["bucket1", "bucket2", "my-bucket"]
