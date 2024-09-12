import camelot
import pandas as pd
from typing import List, Dict
import re

def extract_tables(file_path: str) -> List[pd.DataFrame]:
    tables = camelot.read_pdf(file_path, pages='all', flavor='stream')
    return [table.df for table in tables]

# Hoisting the regex patterns so they're shared across functions
DATE_PATTERN = re.compile(r'\d{1,2}[/-]\d{1,2}([/-]\d{2,4})?|\d{1,2} \w{3}')
DESCRIPTION_PATTERN = re.compile(r'^(?!\d{1,2}[/-]\d{1,2}|[A-Za-z]{3} \d{1,2})(?!\(?\d{1,3}(,\d{3})*(\.\d{2})?\)?\s*(CR|DR)?)[A-Za-z0-9* .#:()/-]+$')
CURRENCY_PATTERN = re.compile(r'\(?\d{1,3}(,\d{3})*(\.\d{2})?\)?\s*(CR|DR)?')


def detect_multiple_matches(col_str):
    """
    Detects if there are multiple parts (e.g., date, description, currency) in the column string.
    Splits by newlines and determines how many transaction parts (date, description, currency) exist.
    """
    parts = col_str.split("\n")

    # If there are two parts, check for date-description or description-currency
    if len(parts) == 2:
        date_and_description_match = DATE_PATTERN.search(parts[0]) and DESCRIPTION_PATTERN.search(parts[1])
        description_and_currency_match = DESCRIPTION_PATTERN.search(parts[0]) and CURRENCY_PATTERN.search(parts[1])
        # Returns true if either date-description or description-currency is valid
        return date_and_description_match or description_and_currency_match

    # If there are three parts, check for date-description-currency
    elif len(parts) == 3:
        date_and_description_match = DATE_PATTERN.search(parts[0])
        description_match = DESCRIPTION_PATTERN.search(parts[1])
        currency_match = CURRENCY_PATTERN.search(parts[2])
        # Return true if all parts match date-description-currency
        return bool(date_and_description_match and description_match and currency_match)

    # No multiple matches detected
    return False


def split_and_rebuild_row(row, col_str, split_col_idx):
    """
    Split the column string by the separator (newline \n) and redistribute the parts into the row.
    Ensure other columns remain untouched by shifting them right.
    """
    parts = col_str.split("\n")
    new_row = row.copy()

    # Shift columns after split_col_idx to the right by len(parts) - 1 to make space for the new parts
    num_parts = len(parts)
    for idx in reversed(range(split_col_idx + 1, len(row))):
        new_row[idx + num_parts - 1] = new_row[idx]

    # Insert the parts into consecutive columns starting from split_col_idx
    for idx, part in enumerate(parts):
        new_row[split_col_idx + idx] = part

    return new_row


def is_transaction_table(table: pd.DataFrame) -> bool:
    """
    Determines if a table is a transaction table by checking for patterns of date, description, and currency.
    Processes all rows that contain multiple transaction elements in one column.
    """
    # Process each row in the table
    for row_idx, row in table.iterrows():
        modified_row = process_row(row)
        if modified_row is not None:
            # Replace the original row in the dataframe with the modified one
            table.loc[row_idx] = modified_row
    
    # After processing all rows, check if any row is a transaction row
    for _, row in table.iterrows():
        if is_transaction_row(row):
            print(f"\033[92m{table}\033[0m")
            return True
    return False

def process_row(row):
    for col_idx, col_value in enumerate(row):
        col_str = str(col_value)
        if detect_multiple_matches(col_str):
            # Split the column into parts and rebuild the row
            return split_and_rebuild_row(row, col_str, col_idx)
    return None

def is_transaction_row(row):
    # Simple transaction detection: Date → Description → Currency
    found_date = found_description = found_currency = False
    for col_value in row:
        col_str = str(col_value)
        
        if not found_date and DATE_PATTERN.match(col_str):
            found_date = True
            continue

        if found_date and not found_description and DESCRIPTION_PATTERN.match(col_str):
            found_description = True
            continue

        if found_description and not found_currency and CURRENCY_PATTERN.match(col_str):
            found_currency = True
            break

    return found_date and found_description and found_currency


def extract_transactions(tables: List[pd.DataFrame]) -> List[Dict]:
    transactions = []
    for table in tables:
        current_transaction = {}
        for _, row in table.iterrows():
            if is_transaction_row(row):
                # New transaction starts
                if current_transaction:
                    transactions.append(current_transaction)
                current_transaction = {}
                
                for col, value in row.items():
                    if pd.notna(value):
                        current_transaction[col] = ' '.join(str(value).split())
            elif current_transaction and not any('SUB-TOTAL' in str(val).upper() for val in row):
                # Continue adding to the current transaction if it's not empty and not a subtotal row
                for col, value in row.items():
                    if pd.notna(value):
                        cleaned_value = ' '.join(str(value).split())
                        if col in current_transaction:
                            current_transaction[col] += f" {cleaned_value}"
                        else:
                            current_transaction[col] = cleaned_value
        
        if current_transaction:
            transactions.append(current_transaction)
    
    return transactions

def main():
    file_path = '/Users/yingcong/Documents/Bank Statements/dbs_cc_07_2024.pdf'
    tables = extract_tables(file_path)
    transaction_tables = [table for table in tables if is_transaction_table(table)]
    transactions = extract_transactions(transaction_tables)

    if not transactions:
        print("No transactions found")
        return
    for transaction in transactions:
        print(transaction)

if __name__ == "__main__":
    main()