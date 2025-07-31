import io
import pandas as pd
import tempfile
import os

class ParquetUploader:
    def __init__(self, s3_uploader):
        """
        Инициализация uploader'а для работы с Parquet файлами.
        
        :param s3_uploader: Экземпляр класса S3Uploader
        """
        self.s3_uploader = s3_uploader

    def upload_dataframe(self, df, s3_key, bucket_name=None, skip_if_exists=False, 
                        parquet_kwargs=None):
        """
        Загрузка pandas DataFrame в S3 в формате Parquet.

        :param df: pandas DataFrame для загрузки
        :param s3_key: Ключ объекта в S3 (путь внутри бакета)
        :param bucket_name: Название S3 бакета (опционально, если установлен default_bucket)
        :param skip_if_exists: Если True, не загружать файл, если он уже существует и не изменился
        :param parquet_kwargs: Дополнительные параметры для to_parquet (опционально)
        """
        try:
            buffer = io.BytesIO()
            parquet_kwargs = parquet_kwargs or {}
            df.to_parquet(buffer, index=False, **parquet_kwargs)
            buffer.seek(0)
            
            # Используем существующий метод загрузки файла из S3Uploader
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                tmp_file.write(buffer.getvalue())
                tmp_file_path = tmp_file.name
            
            self.s3_uploader.upload_file(
                local_file_path=tmp_file_path,
                s3_key=s3_key,
                bucket_name=bucket_name,
                skip_if_exists=skip_if_exists
            )
            
            os.unlink(tmp_file_path)
            
        except Exception as e:
            print(f"Ошибка загрузки DataFrame: {e}")

    def upload_tuples(self, data, s3_key, columns=None, bucket_name=None, 
                     skip_if_exists=False, parquet_kwargs=None):
        """
        Загрузка списка кортежей в S3 в формате Parquet.

        :param data: список кортежей для загрузки
        :param s3_key: Ключ объекта в S3 (путь внутри бакета)
        :param columns: названия колонок (опционально)
        :param bucket_name: Название S3 бакета (опционально, если установлен default_bucket)
        :param skip_if_exists: Если True, не загружать файл, если он уже существует и не изменился
        :param parquet_kwargs: Дополнительные параметры для to_parquet (опционально)
        """
        try:
            df = pd.DataFrame(data, columns=columns)
            self.upload_dataframe(df, s3_key, bucket_name, skip_if_exists, parquet_kwargs)
            
        except Exception as e:
            print(f"Ошибка загрузки данных кортежей: {e}")

    def download_dataframe(self, s3_key, bucket_name=None):
        """
        Скачивание Parquet файла из S3 в pandas DataFrame.

        :param s3_key: Ключ объекта в S3
        :param bucket_name: Название S3 бакета (опционально, если установлен default_bucket)
        :return: pandas DataFrame или None в случае ошибки
        """
        try:
            # Скачивание через существующий клиент в память
            buffer = io.BytesIO()
            bucket = self.s3_uploader._resolve_bucket(bucket_name)
            self.s3_uploader.s3_client.download_fileobj(bucket, s3_key, buffer)
            buffer.seek(0)
            
            # Чтение Parquet в DataFrame
            df = pd.read_parquet(buffer)
            return df
        except Exception as e:
            print(f"Ошибка скачивания DataFrame: {e}")
            return None

    def dataframe_to_s3_url(self, df, s3_key, bucket_name=None, skip_if_exists=False):
        """
        Сохранение DataFrame и возврат URL файла в S3.

        :param df: pandas DataFrame
        :param s3_key: Ключ объекта в S3
        :param bucket_name: Название S3 бакета (опционально)
        :param skip_if_exists: Пропустить, если файл не изменился
        :return: URL файла в S3 или None
        """
        self.upload_dataframe(df, s3_key, bucket_name, skip_if_exists)
        bucket = self.s3_uploader._resolve_bucket(bucket_name)
        return self.s3_uploader._get_s3_url(bucket, s3_key)

    def upsert_dataframe(self, new_df, s3_key, key_columns, bucket_name=None, 
                        parquet_kwargs=None):
        """
        Upsert (update + insert) DataFrame в существующую Parquet таблицу в S3.

        :param new_df: Новый DataFrame для upsert
        :param s3_key: Ключ объекта в S3
        :param key_columns: Список колонок, используемых как ключи для upsert
        :param bucket_name: Название S3 бакета (опционально)
        :param parquet_kwargs: Дополнительные параметры для to_parquet (опционально)
        :return: Объединенный DataFrame
        """
        try:
            # Проверяем, существует ли файл в S3
            bucket = self.s3_uploader._resolve_bucket(bucket_name)
            existing_df = None
            
            try:
                existing_df = self.download_dataframe(s3_key, bucket_name)
            except Exception:
                # Файл не существует, создаем новый
                print(f"Файл {s3_key} не существует, создаем новый")
                self.upload_dataframe(new_df, s3_key, bucket_name, parquet_kwargs=parquet_kwargs)
                return new_df
            
            if existing_df is not None:
                # Выполняем upsert: объединяем существующие и новые данные
                # Удаляем дубликаты по ключевым колонкам, оставляя новые записи
                combined_df = self._perform_upsert(existing_df, new_df, key_columns)
                
                self.upload_dataframe(combined_df, s3_key, bucket_name, parquet_kwargs=parquet_kwargs)
                return combined_df
            else:
                # Если существующий файл не удалось прочитать, загружаем новый
                self.upload_dataframe(new_df, s3_key, bucket_name, parquet_kwargs=parquet_kwargs)
                return new_df
                
        except Exception as e:
            print(f"Ошибка при upsert операции: {e}")
            return None

    def _perform_upsert(self, existing_df, new_df, key_columns):
        """
        Выполнение upsert операции на уровне DataFrame.

        :param existing_df: Существующий DataFrame
        :param new_df: Новый DataFrame
        :param key_columns: Ключевые колонки для upsert
        :return: Объединенный DataFrame
        """
        try:
            missing_keys = set(key_columns) - set(existing_df.columns) - set(new_df.columns)
            if missing_keys:
                raise ValueError(f"Отсутствуют ключевые колонки: {missing_keys}")
            
            # Удаляем дубликаты из существующего DataFrame по ключевым колонкам,
            # которые присутствуют в новом DataFrame
            if not new_df.empty:
                # Создаем индекс по ключевым колонкам для нового DataFrame
                new_index = new_df.set_index(key_columns)
                
                # Удаляем строки из существующего DataFrame, если они есть в новом
                existing_without_duplicates = existing_df.merge(
                    new_index.reset_index()[key_columns], 
                    on=key_columns, 
                    how='left', 
                    indicator=True
                )
                existing_without_duplicates = existing_without_duplicates[
                    existing_without_duplicates['_merge'] == 'left_only'
                ].drop('_merge', axis=1)
                
                # Объединяем оставшиеся существующие записи с новыми
                combined_df = pd.concat([existing_without_duplicates, new_df], ignore_index=True)
            else:
                combined_df = existing_df.copy()
            
            return combined_df
            
        except Exception as e:
            print(f"Ошибка при выполнении upsert: {e}")
            # В случае ошибки возвращаем объединение всех данных
            return pd.concat([existing_df, new_df], ignore_index=True)

    def append_dataframe(self, new_df, s3_key, bucket_name=None, parquet_kwargs=None):
        """
        Добавление DataFrame к существующей Parquet таблице в S3.

        :param new_df: Новый DataFrame для добавления
        :param s3_key: Ключ объекта в S3
        :param bucket_name: Название S3 бакета (опционально)
        :param parquet_kwargs: Дополнительные параметры для to_parquet (опционально)
        :return: Объединенный DataFrame
        """
        try:
            existing_df = None
            
            try:
                existing_df = self.download_dataframe(s3_key, bucket_name)
            except Exception:
                # Файл не существует, создаем новый
                print(f"Файл {s3_key} не существует, создаем новый")
                self.upload_dataframe(new_df, s3_key, bucket_name, parquet_kwargs=parquet_kwargs)
                return new_df
            
            if existing_df is not None:
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                
                self.upload_dataframe(combined_df, s3_key, bucket_name, parquet_kwargs=parquet_kwargs)
                return combined_df
            else:
                # Если существующий файл не удалось прочитать, загружаем новый
                self.upload_dataframe(new_df, s3_key, bucket_name, parquet_kwargs=parquet_kwargs)
                return new_df
                
        except Exception as e:
            print(f"Ошибка при добавлении данных: {e}")
            return None