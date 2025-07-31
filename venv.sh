#!/bin/bash

VENV_DIR=".venv"
if [ ! -d "$VENV_DIR" ]; then
    echo "Создание виртуального окружения: $VENV_DIR"
    python3 -m venv "$VENV_DIR"
else
    echo "Виртуальное окружение '$VENV_DIR' уже существует"
fi

source "$VENV_DIR/bin/activate"

if [ $? -ne 0 ]; then
    echo "Ошибка при активации виртуального окружения"
    exit 1
fi

echo "Виртуальное окружение успешно активировано"

echo "Обновление pip"
pip install --upgrade pip

REQ_FILE="requirements_dev.txt"
if [ -f "$REQ_FILE" ]; then
    echo "Установка библиотек из файла '$REQ_FILE'..."
    pip install -r "$REQ_FILE"
else
    echo "Файл '$REQ_FILE' не найден"
fi