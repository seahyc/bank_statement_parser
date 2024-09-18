from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="bank_statement_parser",
    version="0.1.0",
    author="Seah Ying Cong",
    author_email="seahyingcong@gmail.com",
    description="A tool to parse bank statements from PDF files",
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
    python_requires=">=3.7",
    install_requires=[
        "camelot-py==0.11.0",
        "pandas==2.0.3",
        "pycountry==24.6.1",
        "pypdf==4.3.1",
    ],
    entry_points={
        "console_scripts": [
            "bank_statement_parser=bank_statement_parser.main:cli",
        ],
    },
)