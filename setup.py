from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="mongo-oracle-etl",
    version="1.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="MongoDB to Oracle ETL Pipeline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/mongo-oracle-etl",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "pymongo>=4.5.0",
        "cx-Oracle>=8.3.0",
        "SQLAlchemy>=2.0.23",
        "python-dotenv>=1.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-mock>=3.12.0",
            "pytest-cov>=4.1.0",
            "black>=23.11.0",
            "flake8>=6.1.0",
            "mypy>=1.7.0",
        ],
        "airflow": [
            "apache-airflow>=2.7.3",
        ],
    },
    entry_points={
        "console_scripts": [
            "mongo-oracle-etl=scripts.run_etl:main",
        ],
    },
)