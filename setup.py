"""
Setup script for Airflow Enterprise Toolkit Python package.
"""

from setuptools import setup, find_packages

setup(
    name="airflow-enterprise-toolkit",
    version="0.1.0",
    description="Enterprise toolkit for converting Control-M jobs to Airflow DAGs",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Airflow Enterprise Toolkit Team",
    author_email="team@example.com",
    url="https://github.com/preedep/airflow-enterprise-toolkit",
    packages=find_packages(where="python"),
    package_dir={"": "python"},
    python_requires=">=3.8",
    install_requires=[
        "apache-airflow>=3.0.0,<4.0.0",
        "python-dateutil>=2.8.0",
        "pytz>=2023.3",
    ],
    extras_require={
        "providers": [
            "apache-airflow-providers-postgres>=5.0.0",
            "apache-airflow-providers-mysql>=3.0.0",
            "apache-airflow-providers-sqlite>=3.0.0",
            "apache-airflow-providers-http>=4.0.0",
            "apache-airflow-providers-ssh>=3.0.0",
        ],
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    keywords="airflow control-m migration etl dag",
    project_urls={
        "Bug Reports": "https://github.com/preedep/airflow-enterprise-toolkit/issues",
        "Source": "https://github.com/preedep/airflow-enterprise-toolkit",
        "Documentation": "https://airflow-enterprise-toolkit.readthedocs.io/",
    },
)
