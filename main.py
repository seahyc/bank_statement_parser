import camelot
import pandas as pd
from pandas import DataFrame, Series
from typing import List, Dict, Tuple, Optional
import re, os

def extract_tables(file_path: str) -> List[pd.DataFrame]:
    tables = camelot.read_pdf(file_path, pages='all', flavor='stream')
    return [table.df for table in tables]

# Hoisting the regex patterns so they're shared across functions
DATE_PATTERN = re.compile(r'\d{1,2}[/-]\d{1,2}([/-]\d{2,4})?|\d{1,2} \w{3}')
DESCRIPTION_PATTERN = re.compile(r'^(?!\d{1,2}[/-]\d{1,2}|[A-Za-z]{3} \d{1,2})(?!\(?\d{1,3}(,\d{3})*(\.\d{2})?\)?\s*(CR|DR)?)[A-Za-z0-9* .#:()/-]+$')
CURRENCY_PATTERN = re.compile(r'\(?\d{1,3}(,\d{3})*(\.\d{2})?\)?\s*(CR|DR)?')


def detect_merged_rows(col_str: str):
    """
    Detects if there are multiple parts (e.g., date, description, currency) in the column string
    or if there are merged rows that need to be split.
    """
    parts = col_str.split("\n")

    # Check for specific cases of merged rows
    if len(parts) == 2:
        # Strip whitespace from both parts
        part1, part2 = [p.strip() for p in parts]
        
        # Check for specific text cases
        if (part1 == "Transaction" and part2 == "Value") or \
           (part1 == "Deposit" and part2 == "Balance") or \
           (part1 == "Date" and part2 == "Date"):
            return True

        # Check for various patterns
        pattern_combos = [
            (DATE_PATTERN, DATE_PATTERN),
            (CURRENCY_PATTERN, CURRENCY_PATTERN),
            (DATE_PATTERN, DESCRIPTION_PATTERN),
            (DESCRIPTION_PATTERN, CURRENCY_PATTERN),
        ]
        for pattern1, pattern2 in pattern_combos:
            if pattern1.search(part1) and pattern2.search(part2):
                return True

    # If there are three parts, check for date-description-currency
    elif len(parts) == 3:
        pattern_combos = [DATE_PATTERN, DESCRIPTION_PATTERN, CURRENCY_PATTERN]
        return all(pattern.search(part) for pattern, part in zip(pattern_combos, parts))

    return False


def split_and_rebuild_row(row: Series, col_str: str, split_col_idx: int, prev_split_info: Optional[Dict[int, Tuple[int, int]]] = None) -> Tuple[Series, Dict[int, Tuple[int, int]]]:
    """
    Split the column string by the separator (newline \n) and redistribute the parts into the row.
    Ensure other columns remain untouched by shifting them right.
    Returns the new row and the updated split information.
    """
    parts = col_str.split("\n")
    new_row = row.copy()
    current_split_info = prev_split_info.copy() if prev_split_info else {}

    if prev_split_info:
        max_columns = max(end for _, (_, end) in prev_split_info.items()) + 1
        new_row = pd.Series([pd.NA] * max_columns, index=range(max_columns))

        for old_idx, value in enumerate(row):
            if old_idx in prev_split_info:
                new_start, new_end = prev_split_info[old_idx]
                if len(parts) == 1:
                    new_row[new_start] = value
                elif len(parts) == (new_end - new_start + 1):
                    for i, part in enumerate(parts):
                        new_row[new_start + i] = part
                elif str(value).endswith('\n'):
                    new_row[new_start] = value.rstrip('\n')
                elif str(value).startswith('\n'):
                    offset = str(value).count('\n', 0, str(value).index(str(value).strip()))
                    new_row[new_start + offset] = value.lstrip('\n')
                else:
                    new_row[new_end] = value
            else:
                new_idx = sum(1 for start, end in prev_split_info.values() if start <= old_idx) + old_idx
                new_row[new_idx] = value
    else:
        # Shift columns after split_col_idx to the right by len(parts) - 1 to make space for the new parts
        num_parts = len(parts)
        for idx in reversed(range(split_col_idx + 1, len(row))):
            new_row[idx + num_parts - 1] = new_row[idx]

        # Insert the parts into consecutive columns starting from split_col_idx
        for idx, part in enumerate(parts):
            new_row[split_col_idx + idx] = part

        current_split_info[split_col_idx] = (split_col_idx, split_col_idx + num_parts - 1)

    return new_row, current_split_info


def is_transaction_row(row: Series) -> bool:
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

def detect_and_process_transaction_table(table: DataFrame) -> Tuple[DataFrame, bool]:
    """
    Processes a table for transactions and returns a new DataFrame along with a boolean indicating if it's a transaction table.
    """
    modified_table: List[Series] = []
    current_split_info = None
    
    # Process each row in the table
    for idx, row in table.iterrows():
        new_row = row.copy()
        columns_to_split = []

        for col_idx, col_value in enumerate(row):
            col_str = str(col_value)
            if detect_merged_rows(col_str):
                columns_to_split.append(col_idx)
        
        # Process columns in reverse order to avoid shifting indices
        for col_idx in sorted(columns_to_split, reverse=True):
            new_row, current_split_info = split_and_rebuild_row(new_row, str(new_row[col_idx]), col_idx, current_split_info)
        
        modified_table.append(new_row)
    
    # Create a new DataFrame with the modified rows
    max_columns = max(len(row) for row in modified_table)
    processed_table = pd.DataFrame(modified_table, columns=range(max_columns))
    
    # Check if any row is a transaction row
    is_transaction = any(is_transaction_row(row) for _, row in processed_table.iterrows())
    
    if is_transaction:
        print(table)
        print(f"\033[92m{processed_table}\033[0m")
    
    return processed_table, is_transaction

def is_bank_account_table(table: pd.DataFrame) -> bool:
    # Check if the table contains headers typically found in bank account statements
    header_keywords = ['withdrawal', 'deposit', 'balance']
    header_row = table.iloc[:10].astype(str).apply(lambda x: x.str.lower())
    return all(any(keyword in cell for cell in header_row.values.flatten()) for keyword in header_keywords)

def extract_bank_account_transactions(transaction_tables: List[pd.DataFrame]) -> List[Dict]:
    all_transactions = []
    
    for table in transaction_tables:
        transactions = []

def extract_credit_card_transactions(tables: List[pd.DataFrame]) -> List[Dict]:
    transactions = []
    non_transaction_markers = ['SUB-TOTAL', 'TOTAL', 'NEW TRANSACTIONS']
    
    def clean_text(text):
        return ' '.join(str(text).strip().split())
    
    def combine_amount(row):
        amount_pattern = re.compile(r'\(\d{1,3}(,\d{3})*(\.\d{2})?\)?')
        for i in range(len(row) - 1):
            if isinstance(row[i], str):
                match = amount_pattern.match(row[i])
                if match and not row[i].endswith(')'):
                    if row[i + 1] == ')':
                        row[i] = f"{row[i]})"
                        row[i + 1] = ''
        return row
    
    non_empty_columns = set()
    for table in tables:
        current_transaction = {}
        non_empty_columns = set()
        
        for idx, row in table.iterrows():
            if is_transaction_row(row):
                # New transaction starts
                if current_transaction:
                    transactions.append(current_transaction)
                current_transaction = {}
                
                row = combine_amount(row)
                
                for col, value in enumerate(row):
                    if pd.notna(value) and value != '':
                        current_transaction[col] = clean_text(value)
                        non_empty_columns.add(col)
                
                # Check next row for additional description
                if idx != table.index[-1]:
                    next_row = table.iloc[table.index.get_loc(idx) + 1]
                    if not is_transaction_row(next_row) and not any(marker in str(val).upper() for marker in non_transaction_markers for val in next_row):
                        additional_text = ' '.join(clean_text(val) for val in next_row if pd.notna(val) and val != '')
                        if additional_text:
                            description_col = 1
                            if description_col in current_transaction:
                                current_transaction[description_col] = clean_text(f"{current_transaction[description_col]} {additional_text}")
                            else:
                                current_transaction[description_col] = additional_text
        
        if current_transaction:
            transactions.append(current_transaction)
    
    # Remove empty columns
    cleaned_transactions = []
    for transaction in transactions:
        cleaned_transaction = {col: val for col, val in transaction.items() if col in non_empty_columns}
        cleaned_transactions.append(cleaned_transaction)
    
    return cleaned_transactions


def main():
    file_paths = [
        '360 ACCOUNT-2001-08-24.pdf',
        'dbs_acct_06_2024',
        'dbs_cc_05_2024',
        'OCBC 90.N CARD-9905-08-24'
    ]
    
    for file_path in file_paths:
        print(f"Processing file: {file_path}")
        file_path = os.path.join('/Users/yingcong/Documents/Bank Statements', file_path)
        tables = extract_tables(file_path)
        
        transaction_tables: List[pd.DataFrame] = []
        for table in tables:
            processed_table, is_transaction = detect_and_process_transaction_table(table)
            if is_transaction:
                transaction_tables.append(processed_table)
        
        if any(is_bank_account_table(table) for table in transaction_tables):
            transactions = extract_bank_account_transactions(transaction_tables)
        else:
            transactions = extract_credit_card_transactions(transaction_tables)
        
        if not transactions:
            print("No transactions found")
            return
        for transaction in transactions:
            print(transaction)

if __name__ == "__main__":
    main()