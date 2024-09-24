# OCBC DBS Statement Parser

A Python tool to parse bank statements from PDF files, specifically designed for OCBC and DBS statements but potentially compatible with other banks.

## Requirements

- Python 3.7 or 3.8

# Installation

```
pip install ocbc-dbs-statement-parser
```

or if you're running it from source:

```
pip install -e .
```

# Usage

```
python -m ocbc-dbs-statement-parser <pdf_path> [--debug] [--verify] [--help]
```

Locally
```
python -m ocbc-dbs-statement-parser.cli <pdf_path> [--debug] [--verify][--help]
```

## Features

- Extracts transactions from bank account and credit card statements
- Supports various date formats
- Verifies transaction totals
- Debug mode for detailed output

## Development

### Setup

1. Clone the repository
2. Create a virtual environment: `python -m venv venv`
3. Activate the virtual environment:
   - Windows: `venv\Scripts\activate`
   - macOS/Linux: `source venv/bin/activate`
4. Install dependencies: `pip install -r requirements.txt`

### Testing

To run tests using pytest, execute the following command:

```
pytest tests/test_main.py
```

### Push releases

```
bumpversion patch|minor|major
git push && git push --tags
```

## Dependencies

The main dependencies for this project are:

- camelot-py==0.11.0
- pandas==2.0.3
- pycountry==24.6.1
- pypdf==4.3.1

For a complete list of dependencies, please refer to the `requirements.txt` file.

## License

This project is licensed under the MIT License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.