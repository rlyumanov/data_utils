# data_utils

[![Tests](https://github.com/rlyumanov/data-utils/actions/workflows/test.yml/badge.svg)](https://github.com/rlyumanov/data-utils/actions/workflows/test.yml)
[![Python Version](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/)

Python-библиотека для работы с данными, предоставляющая утилиты для операций с S3 и Parquet файлами.

## Возможности

Пакет включает два модуля:

- **s3** - Управление файлами в S3 с загрузкой/скачиванием, умным сравнением файлов и пакетными операциями
- **parquet_loader** - Операции с Parquet файлами на S3 с возможностью upsert и append

## Установка

```bash
pip install git+https://github.com/rlyumanov/data-utils.git
```

### Требования

- Python >= 3.12
- boto3 >= 1.39.17
- pandas >= 2.0.0
- pyarrow >= 21.0.0

## Быстрый старт

### Работа с S3

```python
from data_utils.s3 import S3Uploader

# Инициализация с дефолтным bucket
s3 = S3Uploader(
    aws_access_key_id='your_key',
    aws_secret_access_key='your_secret',
    bucket_name='my-bucket',
    region_name='us-east-1'
)

# Загрузка файла
s3.upload_file('local_file.csv', 'path/in/s3/file.csv')

# Загрузка директории с умным пропуском неизмененных файлов
s3.upload_directory('local_dir/', 's3_dir/', skip_if_exists=True)

# Скачивание файла
s3.download_file('path/in/s3/file.csv', 'local_file.csv')

# Удаление всех файлов в директории (с подтверждением)
s3.delete_all_files_in_directory('s3_dir/', confirm=True)
```

**Умное сравнение файлов:**
- Файлы >5MB: только сравнение по размеру (оптимизация производительности)
- Файлы <5MB: сравнение времени модификации с допуском 2 секунды
- Файлы <50MB: полная MD5 верификация, если размер совпадает, но время отличается

### Работа с Parquet на S3

```python
from data_utils.s3 import S3Uploader
from data_utils.parquet_loader import ParquetUploader
import pandas as pd

# Создаем S3 uploader
s3 = S3Uploader(
    aws_access_key_id='your_key',
    aws_secret_access_key='your_secret',
    bucket_name='my-bucket'
)

# Создаем Parquet uploader (требует S3Uploader)
parquet = ParquetUploader(s3)

# Загрузка DataFrame как Parquet
df = pd.DataFrame({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})
parquet.upload_dataframe(df, 'path/to/data.parquet')

# Загрузка из кортежей
data = [(1, 'a'), (2, 'b'), (3, 'c')]
columns = ['id', 'value']
parquet.upload_tuples(data, columns, 'path/to/data.parquet')

# Скачивание Parquet в DataFrame
df = parquet.download_dataframe('path/to/data.parquet')

# Upsert данных (загружает существующий файл, объединяет, загружает обратно)
new_df = pd.DataFrame({'id': [2, 4], 'value': ['b_updated', 'd']})
parquet.upsert_dataframe(new_df, 'path/to/data.parquet', key_columns=['id'])

# Append данных (простая конкатенация без дедупликации)
parquet.append_dataframe(new_df, 'path/to/data.parquet')
```