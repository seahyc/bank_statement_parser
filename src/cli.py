import argparse
import json
from decimal import Decimal
from .main import parse_bank_statement

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def cli():
    parser = argparse.ArgumentParser(description="Process bank statement PDF")
    parser.add_argument("pdf_path", help="Path to the PDF file")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument("--verify", action="store_true", help="Verify transaction totals")
    args = parser.parse_args()

    result = parse_bank_statement(args.pdf_path, args.debug, args.verify)
    print(json.dumps(result, indent=2, default=decimal_default))

if __name__ == "__main__":
    cli()