from setuptools import setup, find_packages

# Читаем описание из README
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="data_utils",
    version="0.1.5",
    author="r.lyumanov",
    author_email="rlyumanov@gmail.com",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rlyumanov/data-utils",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    python_requires=">=3.12",
    install_requires=[                          
        "boto3>=1.39.17",
        "pandas>=2.0.0",
        "pyarrow>=21.0.0",
    ],

    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-mock>=3.6.0",
            "pytest-cov>=4.0.0",
            "moto>=4.0.0",
            "black",
            "flake8",
        ]
    },
)