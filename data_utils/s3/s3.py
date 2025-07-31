import os
import boto3
import hashlib
from botocore.exceptions import ClientError
from datetime import datetime

class S3Uploader:
    def __init__(self, aws_access_key_id, aws_secret_access_key, endpoint_url=None, 
                 region_name='us-east-1', default_bucket=None, debug=False):
        """
        Инициализация клиента S3.

        :param aws_access_key_id: AWS Access Key ID
        :param aws_secret_access_key: AWS Secret Access Key
        :param endpoint_url: URL эндпоинта S3
        :param region_name: Регион S3
        :param default_bucket: Бакет по умолчанию (опционально)
        :param debug: Включить режим отладки
        """
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url,
            region_name=region_name
        )
        self.default_bucket = default_bucket
        self.debug = debug

    def _calculate_local_file_hash(self, file_path):
        """
        Вычисление MD5 хэша локального файла.

        :param file_path: Путь к локальному файлу
        :return: MD5 хэш в виде hex строки
        """
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            print(f"Ошибка вычисления хэша для файла {file_path}: {e}")
            return None

    def _get_s3_object_info(self, bucket_name, s3_key):
        """
        Получение информации о файле в S3 (размер и ETag/хэш).

        :param bucket_name: Название бакета
        :param s3_key: Ключ объекта в S3
        :return: Словарь с информацией о файле или None, если файл не найден
        """
        try:
            response = self.s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            return {
                'size': response['ContentLength'],
                'etag': response['ETag'].strip('"'),
                'last_modified': response['LastModified']
            }
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Файл не найден
                return None
            else:
                print(f"Ошибка получения информации о файле {s3_key}: {e}")
                return None

    def _files_are_equal(self, local_file_path, bucket_name, s3_key):
        """
        Сравнение локального файла с файлом в S3 по размеру, времени модификации и хэшу.

        :param local_file_path: Путь к локальному файлу
        :param bucket_name: Название бакета
        :param s3_key: Ключ объекта в S3
        :return: True, если файлы идентичны, False в противном случае
        """
        try:
            # Получение информации о локальном файле
            local_stat = os.stat(local_file_path)
            local_size = local_stat.st_size
            local_mtime = local_stat.st_mtime
            local_mtime_dt = datetime.fromtimestamp(local_mtime)
        except OSError as e:
            print(f"Ошибка получения информации о локальном файле {local_file_path}: {e}")
            return False

        # Получение информации о файле в S3
        s3_info = self._get_s3_object_info(bucket_name, s3_key)
        
        if s3_info is None:
            # Файл не существует в S3
            if self.debug:
                print(f"Файл {s3_key} не существует в S3")
            return False

        # Сравнение размеров
        if local_size != s3_info['size']:
            if self.debug:
                print(f"Размеры не совпадают: локальный={local_size}, S3={s3_info['size']}")
            return False

        # Сравнение времени модификации
        s3_mtime = s3_info['last_modified']
        s3_mtime_ts = s3_mtime.timestamp()
        
        if self.debug:
            print(f"Сравнение времени модификации:")
            print(f"  Локальный файл: {local_mtime_dt} (timestamp: {local_mtime})")
            print(f"  S3 файл:        {s3_mtime} (timestamp: {s3_mtime_ts})")
            print(f"  Разница:        {abs(local_mtime - s3_mtime_ts)} секунд")

        # Для файлов размером больше 5 МБ полагаемся только на размер
        # Для меньших файлов проверяем время модификации
        if local_size > 5 * 1024 * 1024:  # больше 5 МБ
            # Для больших файлов проверяем только размер
            if self.debug:
                print("Файл большой (>5MB), проверяем только размер")
            return True
        else:
            time_diff = abs(local_mtime - s3_mtime_ts)
            if time_diff <= 2:
                if self.debug:
                    print(f"Время модификации совпадает (разница {time_diff} сек)")
                return True
            else:
                if self.debug:
                    print(f"Время модификации НЕ совпадает (разница {time_diff} сек)")

        # Если размеры совпадают, но время нет - делаем дополнительную проверку хэша
        # Но только для небольших файлов
        if local_size < 50 * 1024 * 1024:  # меньше 50 МБ
            local_hash = self._calculate_local_file_hash(local_file_path)
            if local_hash is None:
                return False
            
            # Проверяем, является ли ETag простым MD5 (без дефиса)
            if '-' not in s3_info['etag']:
                # ETag - это простой MD5
                if local_hash == s3_info['etag']:
                    if self.debug:
                        print("Хэши совпадают")
                    return True
                else:
                    if self.debug:
                        print(f"Хэши НЕ совпадают: локальный={local_hash}, S3={s3_info['etag']}")

        return False

    def _resolve_bucket(self, bucket_name=None):
        """
        Разрешение имени бакета: используется переданный параметр или бакет по умолчанию.

        :param bucket_name: Название бакета (опционально)
        :return: Название бакета
        :raises ValueError: Если бакет не указан и не установлен по умолчанию
        """
        if bucket_name is not None:
            return bucket_name
        elif self.default_bucket is not None:
            return self.default_bucket
        else:
            raise ValueError("Не указан бакет. Укажите bucket_name или установите default_bucket при инициализации.")

    def upload_file(self, local_file_path, s3_key, bucket_name=None, skip_if_exists=False):
        """
        Загрузка одного файла в S3 с возможностью пропуска неизмененных файлов.

        :param local_file_path: Путь к локальному файлу
        :param s3_key: Ключ объекта в S3 (путь внутри бакета)
        :param bucket_name: Название S3 бакета (опционально, если установлен default_bucket)
        :param skip_if_exists: Если True, не загружать файл, если он уже существует и не изменился
        """
        bucket = self._resolve_bucket(bucket_name)
        
        if not os.path.exists(local_file_path):
            print(f"Ошибка: файл {local_file_path} не найден.")
            return

        if skip_if_exists:
            if self._files_are_equal(local_file_path, bucket, s3_key):
                print(f"Файл {local_file_path} не изменился, пропускаем загрузку в {self._get_s3_url(bucket, s3_key)}")
                return

        try:
            self.s3_client.upload_file(local_file_path, bucket, s3_key)
            print(f"Файл {local_file_path} успешно загружен в {self._get_s3_url(bucket, s3_key)}")
        except ClientError as e:
            print(f"Ошибка загрузки файла: {e}")

    def upload_directory(self, local_directory, s3_prefix='', bucket_name=None, skip_if_exists=False):
        """
        Загрузка всей директории в S3 с сохранением структуры.

        :param local_directory: Локальная директория для загрузки
        :param s3_prefix: Префикс пути в S3 (например, 'uploads/')
        :param bucket_name: Название S3 бакета (опционально, если установлен default_bucket)
        :param skip_if_exists: Если True, не загружать файлы, которые уже существуют и не изменились
        """
        bucket = self._resolve_bucket(bucket_name)
        
        if not os.path.isdir(local_directory):
            print(f"Ошибка: {local_directory} не является директорией.")
            return

        for root, dirs, files in os.walk(local_directory):
            for file in files:
                local_file_path = os.path.join(root, file)
                # Относительный путь от корня директории
                relative_path = os.path.relpath(local_file_path, local_directory)
                s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")  # Для Windows-энджоеров
                self.upload_file(local_file_path, s3_key, bucket, skip_if_exists)

    def download_file(self, s3_key, local_file_path, bucket_name=None):
        """
        Скачивание файла из S3.

        :param s3_key: Ключ объекта в S3
        :param local_file_path: Локальный путь для сохранения файла
        :param bucket_name: Название S3 бакета (опционально, если установлен default_bucket)
        """
        bucket = self._resolve_bucket(bucket_name)
        
        try:
            # Создание директории, если она не существует
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            self.s3_client.download_file(bucket, s3_key, local_file_path)
            print(f"Файл успешно скачан из {self._get_s3_url(bucket, s3_key)} в {local_file_path}")
        except ClientError as e:
            print(f"Ошибка скачивания файла: {e}")

    def list_files(self, prefix='', bucket_name=None):
        """
        Получение списка файлов в бакете с опциональным префиксом.

        :param prefix: Префикс для фильтрации файлов
        :param bucket_name: Название S3 бакета (опционально, если установлен default_bucket)
        :return: Список ключей файлов
        """
        bucket = self._resolve_bucket(bucket_name)
        
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            else:
                return []
        except ClientError as e:
            print(f"Ошибка получения списка файлов: {e}")
            return []

    def delete_file(self, s3_key, bucket_name=None):
        """
        Удаление файла из S3.

        :param s3_key: Ключ объекта в S3
        :param bucket_name: Название S3 бакета (опционально, если установлен default_bucket)
        """
        bucket = self._resolve_bucket(bucket_name)
        
        try:
            self.s3_client.delete_object(Bucket=bucket, Key=s3_key)
            print(f"Файл {s3_key} успешно удален из бакета {bucket}")
        except ClientError as e:
            print(f"Ошибка удаления файла: {e}")

    def _get_s3_url(self, bucket_name, s3_key):
        """
        Вспомогательный метод для формирования URL объекта в S3.

        :param bucket_name: Название бакета
        :param s3_key: Ключ объекта
        :return: Строка с URL объекта
        """
        endpoint = self.s3_client.meta.endpoint_url
        if endpoint:
            return f"{endpoint}/{bucket_name}/{s3_key}"
        else:
            return f"s3://{bucket_name}/{s3_key}"

    def create_bucket(self, bucket_name=None):
        """
        Создание бакета (если он не существует).

        :param bucket_name: Название бакета (опционально, если установлен default_bucket)
        """
        bucket = self._resolve_bucket(bucket_name)
        
        try:
            self.s3_client.create_bucket(Bucket=bucket)
            print(f"Бакет {bucket} успешно создан")
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyExists':
                print(f"Бакет {bucket} уже существует")
            elif e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                print(f"Бакет {bucket} уже принадлежит вам")
            else:
                print(f"Ошибка создания бакета: {e}")

    def list_buckets(self):
        """
        Получение списка всех бакетов.

        :return: Список названий бакетов
        """
        try:
            response = self.s3_client.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]
            return buckets
        except ClientError as e:
            print(f"Ошибка получения списка бакетов: {e}")
            return []