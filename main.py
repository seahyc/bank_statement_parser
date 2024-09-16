import camelot
import pandas as pd
from pandas import DataFrame, Series
import pycountry
from typing import List, Dict, Tuple, Optional
import re, os, string

def extract_tables(file_path: str) -> List[pd.DataFrame]:
    tables = camelot.read_pdf(file_path, pages='all', flavor='stream')
    return [table.df for table in tables]

# Hoisting the regex patterns so they're shared across functions
DATE_PATTERN = re.compile(r'\d{1,2}[/-]\d{1,2}([/-]\d{2,4})?|\d{1,2} \w{3}')
DESCRIPTION_PATTERN = re.compile(r'^(?!\d{1,2}[/-]\d{1,2}|[A-Za-z]{3} \d{1,2})(?!\(?\d{1,3}(,\d{3})*(\.\d{2})?\)?\s*(CR|DR)?)[A-Za-z0-9* .#:()/-]+$')
CURRENCY_PATTERN = re.compile(r'\(?\$?\s*\d{1,}(,\d{2,3})*(\.\d{2})\)?\s*(CR|DR)?')

def clean_text_basic(text: str) -> str:
    """
    Cleans the input text by removing non-printable characters and trimming whitespace.
    """
    # Remove non-printable characters
    cleaned = ''.join(c for c in text if c in string.printable).strip()
    return cleaned

def detect_merged_rows(col_str: str) -> bool:
    """
    Detects if there are multiple parts (e.g., date, description, currency) in the column string
    or if there are merged rows that need to be split.
    """
    parts = col_str.split("\n")

    # Check for specific cases of merged rows
    if len(parts) == 2:
        # Strip whitespace from both parts
        part1, part2 = [clean_text_basic(p) for p in parts]
        
        # Check for specific text cases (make comparisons case-insensitive)
        if (part1.lower() == "transaction" and part2.lower() == "value") or \
           (part1.lower() == "deposit" and part2.lower() == "balance") or \
           (part1.lower() == "date" and part2.lower() == "date"):
            return True

        # Check for various patterns
        pattern_combos = [
            (DATE_PATTERN, DATE_PATTERN),
            (CURRENCY_PATTERN, CURRENCY_PATTERN),
            (DATE_PATTERN, DESCRIPTION_PATTERN),
            (DESCRIPTION_PATTERN, CURRENCY_PATTERN),
            (DESCRIPTION_PATTERN, DESCRIPTION_PATTERN),  # Ensure this line is present
        ]
        for pattern1, pattern2 in pattern_combos:
            if pattern1.search(part1) and pattern2.search(part2):
                return True

    # If there are three parts, check for date-description-currency
    elif len(parts) == 3:
        pattern_combos = [DATE_PATTERN, DESCRIPTION_PATTERN, CURRENCY_PATTERN]
        return all(pattern.search(part) for pattern, part in zip(pattern_combos, parts))

    return False

def split_and_rebuild_row(row: Series, col_str: str, split_col_idx: int, split_columns_info: Dict[int, List[str]]) -> Series:
    """
    Splits the column string by newline and redistributes the parts into the row.
    Assigns the first part to the left subcolumn and the second part to the right subcolumn.
    If there's only one part, assigns it to the right subcolumn and sets the left to NaN.
    """
    parts = col_str.split("\n")
    subcolumns = split_columns_info.get(split_col_idx, [])
    
    # Shift columns to the right to make space for new parts (if necessary)
    if len(subcolumns) > 1:
        # Start shifting from the end to avoid overwriting
        for idx in reversed(range(split_col_idx + 1, len(row))):
            row[idx + len(subcolumns) - 1] = row[idx]
            row[idx] = ''
    
    # Assign split parts based on the number of parts
    if len(parts) == 2:
        # Assign first part to the left subcolumn
        row[split_col_idx] = parts[0].strip()
        
        # Assign second part to the right subcolumn
        row[split_col_idx + 1] = parts[1].strip()
    elif len(parts) == 1:
        # Assign the single part to the right subcolumn
        row[split_col_idx] = ''
        row[split_col_idx + 1] = parts[0].strip()
    else:
        # Handle unexpected number of parts by assigning NaN
        row[split_col_idx] = ''
        row[split_col_idx + 1] = ''
    return row

def is_header_row(row: Series) -> bool:
    """
    Determines if a given row is the header row based on the presence of specific keywords.
    """
    # Define header keywords
    bank_account_header_keywords = ['date', 'description', 'withdrawal', 'deposit', 'balance']
    credit_card_header_keywords = ['date', 'description', 'amount']
    # Convert all cells in the row to lowercase strings for case-insensitive comparison
    row_lower = row.astype(str).str.lower()
    
    # Check if all header keywords are present in the row
    return (
        all(any(keyword in cell for cell in row_lower) for keyword in bank_account_header_keywords) or
        all(any(keyword in cell for cell in row_lower) for keyword in credit_card_header_keywords)
    )

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

def clean_and_detect_transaction_table(table: DataFrame) -> Tuple[DataFrame, bool]:
    modified_table: List[Series] = []
    split_columns_info: Dict[int, List[str]] = {}  # Maps original column index to subcolumn names
    split_counts: Dict[int, int] = {}  # Keeps track of the number of new columns added at each split

    # Helper function to get current column index based on splits
    def get_current_col_idx(col_idx):
        return col_idx + sum(split_counts.get(idx, 0) for idx in split_counts if idx < col_idx)
    # First, process the header row to determine splits and shifts
    header_processed = False
    for _, row in table.iterrows():
        new_row = row.copy()

        if not header_processed and is_header_row(row):
            header_processed = True
            # Detect and split merged columns in the header
            columns_to_split = [col_idx for col_idx, col_value in enumerate(row) if detect_merged_rows(str(col_value))]
            
            # Sort columns to split in descending order to handle shifts correctly
            for orig_col_idx in sorted(columns_to_split, reverse=True):
                current_col_idx = get_current_col_idx(orig_col_idx)
                col_str = str(new_row[current_col_idx])
                parts = col_str.split("\n")
                subcolumns = [part.strip() for part in parts]
                
                # Record the split columns and their subcolumns
                split_columns_info[orig_col_idx] = subcolumns
                
                # Perform the split
                new_row = split_and_rebuild_row(new_row, col_str, current_col_idx, split_columns_info)
                
                # Update split_counts
                split_counts[orig_col_idx] = len(parts) - 1

            modified_table.append(new_row)
        else:
            # Only split columns identified from the header
            for orig_col_idx in sorted(split_columns_info.keys(), reverse=True):
                col_str=str(row[orig_col_idx])
                new_row = split_and_rebuild_row(new_row, col_str, orig_col_idx, split_columns_info)
            modified_table.append(new_row)
    
    # Determine the maximum number of columns after splitting
    max_columns = max(len(row) for row in modified_table)
    processed_table = pd.DataFrame(modified_table, columns=range(max_columns))

    # Check if any row is a transaction row
    is_transaction = any(is_transaction_row(row) for row in processed_table.itertuples(index=False))
    
    if is_transaction:
        print("\nProcessed Table:")
        print(f"\033[92m{processed_table}\033[0m")  # Green text for visibility
    
    return processed_table, is_transaction

def is_bank_account_table(table: pd.DataFrame) -> bool:
    # Check if the table contains headers typically found in bank account statements
    header_keywords = ['withdrawal', 'deposit', 'balance']
    header_row = table.iloc[:10].astype(str).apply(lambda x: x.str.lower())
    return all(any(keyword in cell for cell in header_row.values.flatten()) for keyword in header_keywords)

def extract_bank_account_transactions(tables: List[pd.DataFrame]) -> List[Dict]:
    return []

def extract_credit_card_transactions(tables: List[pd.DataFrame]) -> List[Dict]:
    transactions = []
    non_transaction_markers = {'SUB-TOTAL', 'TOTAL', 'NEW TRANSACTIONS'}

    # Dynamic location detection using pycountry
    country_codes = {country.alpha_2 for country in pycountry.countries}
    country_codes.update({country.alpha_3 for country in pycountry.countries})
    country_names = {country.name.upper() for country in pycountry.countries}
    location_keywords = country_codes.union(country_names)

    def clean_text(text):
        return ' '.join(str(text).strip().split())

    def parse_amount(amount_str):
        amount_str = amount_str.replace(',', '')
        amount_str = amount_str.replace(' ', '')
        is_negative = False

        # Handle starting or ending parentheses
        if amount_str.startswith('('):
            is_negative = True
            amount_str = amount_str[1:]
        if amount_str.endswith(')'):
            is_negative = True
            amount_str = amount_str[:-1]

        # Handle 'CR' or 'DR' notation
        if amount_str.endswith('CR'):
            is_negative = True
            amount_str = amount_str[:-2]
        elif amount_str.endswith('DR'):
            amount_str = amount_str[:-2]

        try:
            amount = float(amount_str)
            if is_negative:
                amount = -amount
            return amount
        except ValueError:
            return None

    def is_location(value_str):
        return value_str.upper() in location_keywords

    for table in tables:
        current_transaction = {}
        for idx, row in table.iterrows():
            if is_transaction_row(row):
                if current_transaction:
                    transactions.append(current_transaction)
                current_transaction = {}

                description_parts = []
                date_found = amount_found = False

                for value in row:
                    value_str = clean_text(value)
                    if not date_found and DATE_PATTERN.match(value_str):
                        current_transaction['Date'] = value_str
                        date_found = True
                    elif not amount_found and CURRENCY_PATTERN.search(value_str):
                        current_transaction['Amount (SGD)'] = parse_amount(value_str)
                        amount_found = True
                    elif not is_location(value_str) and value_str != '':
                        description_parts.append(value_str)

                current_transaction['Description'] = ' '.join(description_parts)

                # Check next row for additional description
                if table.index.get_loc(idx) + 1 < len(table):
                    next_row = table.iloc[table.index.get_loc(idx) + 1]
                    if not is_transaction_row(next_row) and not any(
                        marker in clean_text(val).upper() for val in next_row for marker in non_transaction_markers
                    ):
                        additional_text = ' '.join(
                            clean_text(val) for val in next_row
                            if pd.notna(val) and val != '' and not is_location(clean_text(val))
                        )
                        if additional_text:
                            current_transaction['Description'] += ' ' + additional_text
            else:
                continue  # Skip non-transaction rows

        if current_transaction:
            transactions.append(current_transaction)

    return transactions


def main():
    file_paths = [
        '360 ACCOUNT-2001-07-24.pdf',
        'dbs_acct_07_2024.pdf',
        # 'dbs_cc_05_2024.pdf',
        # 'OCBC 90.N CARD-9905-08-24.pdf'
    ]
    
    for file_path in file_paths:
        print("\033[95m" + "=" * 80)  # Bright purple
        print(f"Processing file: {file_path}")
        print("=" * 80 + "\033[0m")  # Reset color
        file_path_full = os.path.join('/Users/yingcong/Documents/Bank Statements', file_path)
        tables = extract_tables(file_path_full)
        
        transaction_tables: List[pd.DataFrame] = []
        for table in tables:
            processed_table, is_transaction = clean_and_detect_transaction_table(table)
            if is_transaction:
                transaction_tables.append(processed_table)
        
        if any(is_bank_account_table(table) for table in transaction_tables):
            transactions = extract_bank_account_transactions(transaction_tables)
        else:
            transactions = extract_credit_card_transactions(transaction_tables)
        
        if not transactions:
            print("No transactions found")
            continue
        for transaction in transactions:
            print(transaction)

if __name__ == "__main__":
    main()