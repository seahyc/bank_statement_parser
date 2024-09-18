from setuptools import setup, find_packages
import os

def get_version():
    version = os.environ.get('GITHUB_REF_NAME')
    if version and version.startswith('v'):
        return version[1:]  # Remove 'v' prefix
    return '0.1.5'  # Default version if not a release

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="ocbc_dbs_statement_parser",
    version=get_version(),
    author="Seah Ying Cong",
    author_email="seahyingcong@gmail.com",
    description="A tool to parse OCBC and DBS bank and credit card statements from PDF files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/seahyc/bank_statement_parser",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    python_requires='>=3.7, <3.9',  # Specify Python version constraint
    install_requires=[
        "camelot-py==0.11.0",
        "pandas==2.0.3",
        "pycountry==24.6.1",
        "pypdf==4.3.1",
    ],
    entry_points={
        "console_scripts": [
            "ocbc_dbs_statement_parser=ocbc_dbs_statement_parser.cli:cli",
        ],
    },
)