from setuptools import setup, find_packages
import os

def get_version():
    version = os.environ.get('GITHUB_REF_NAME')
    if version and version.startswith('v'):
        return version[1:]  # Remove 'v' prefix
    return '0.1.6'  # Default version if not a release

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
        "cffi==1.17.1",
        "chardet==5.2.0",
        "charset-normalizer==3.3.2",
        "click==8.1.7",
        "cryptography==43.0.1",
        "et-xmlfile==1.1.0",
        "ghostscript==0.7",
        "importlib_resources==6.4.5",
        "numpy==1.24.4",
        "opencv-python==4.10.0.84",
        "openpyxl==3.1.5",
        "pandas==2.0.3",
        "pdfminer.six==20240706",
        "pdftopng==0.2.3",
        "pycountry==24.6.1",
        "pycparser==2.22",
        "pypdf==4.3.1",
        "python-dateutil==2.9.0.post0",
        "pytz==2024.1",
        "six==1.16.0",
        "tabulate==0.9.0",
        "typing_extensions==4.12.2",
        "tzdata==2024.1",
        "zipp==3.20.2",
    ],
    entry_points={
        "console_scripts": [
            "ocbc_dbs_statement_parser=ocbc_dbs_statement_parser.cli:cli",
        ],
    },
)