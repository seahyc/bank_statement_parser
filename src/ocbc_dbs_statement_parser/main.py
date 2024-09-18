import camelot
import pandas as pd
from pandas import DataFrame, Series
from pycountry import countries
from typing import List, Dict, Tuple, Set, Optional
import re, string
from datetime import datetime
from pypdf import PdfReader
import warnings
from decimal import Decimal

# Suppress specific warnings
warnings.filterwarnings("ignore", message="No tables found in table area", module="camelot.parsers.stream")

# Debug output control
global DEBUG_OUTPUT
DEBUG_OUTPUT = False

def extract_tables(file_path: str) -> List[pd.DataFrame]:
    tables = camelot.read_pdf(file_path, pages='all', flavor='stream')
    return [table.df for table in tables]

# Hoisting the regex patterns so they're shared across functions
DATE_PATTERN = re.compile(r'\d{1,2}[/-]\d{1,2}([/-]\d{2,4})?|\d{1,2} \w{3}')
DESCRIPTION_PATTERN = re.compile(r'^(?!\d{1,2}[/-]\d{1,2}|[A-Za-z]{3} \d{1,2})(?!\(?\d{1,3}(,\d{3})*(\.\d{2})?\)?\s*(CR|DR)?)[A-Za-z0-9* .#:()/-]+$')
CURRENCY_PATTERN = re.compile(r'\(?\$?\s*\d{1,}(,\d{2,3})*(\.\d{2})\)?\s*(CR|DR)?')

def clean_text(text: str) -> str:
    """
    Cleans the input text by removing non-printable characters,
    trimming whitespace, and normalizing spaces between words.
    """
    return ' '.join(''.join(c for c in str(text) if c in string.printable).split())

def detect_merged_rows(col_str: str) -> bool:
    """
    Detects if there are multiple parts (e.g., date, description, currency) in the column string
    or if there are merged rows that need to be split.
    """
    if DEBUG_OUTPUT:
        print(f"DEBUG_OUTPUT: detect_merged_rows input: {col_str}")
    parts = col_str.split("\n")

    # Check for specific cases of merged rows
    if len(parts) == 2:
        # Strip whitespace from both parts
        part1, part2 = [clean_text(p) for p in parts]
        
        # Check for specific text cases (make comparisons case-insensitive)
        if (part1.lower() == "transaction" and part2.lower() == "value") or \
           (part1.lower() == "deposit" and part2.lower() == "balance") or \
           (part1.lower() == "date" and part2.lower() == "date"):
            if DEBUG_OUTPUT:
                print("DEBUG_OUTPUT: detect_merged_rows output: True")
            return True

        # Check for various patterns
        pattern_combos = [
            (DATE_PATTERN, DATE_PATTERN),
            (CURRENCY_PATTERN, CURRENCY_PATTERN),
            (DATE_PATTERN, DESCRIPTION_PATTERN),
            (DESCRIPTION_PATTERN, CURRENCY_PATTERN),
            (DESCRIPTION_PATTERN, DESCRIPTION_PATTERN),
        ]
        for pattern1, pattern2 in pattern_combos:
            if pattern1.search(part1) and pattern2.search(part2):
                if DEBUG_OUTPUT:
                    print("DEBUG_OUTPUT: detect_merged_rows output: True")
                return True

    # If there are three parts, check for date-description-currency
    elif len(parts) == 3:
        # Check for specific text cases
        if col_str.lower() == "transaction\ndate\ndescription":
            if DEBUG_OUTPUT:
                print("DEBUG_OUTPUT: detect_merged_rows output: True")
            return True
        
        # Check for general pattern combinations
        pattern_combos = [DATE_PATTERN, DESCRIPTION_PATTERN, CURRENCY_PATTERN]
        if DEBUG_OUTPUT:
            print(f"DEBUG_OUTPUT: detect_merged_rows output: {all(pattern.search(part) for pattern, part in zip(pattern_combos, parts))}")
        return all(pattern.search(part) for pattern, part in zip(pattern_combos, parts))
    return False

def split_and_rebuild_row(row: Series, col_str: str, split_col_idx: int, split_columns_info: Dict[int, List[str]]) -> Series:
    """
    Splits the column string by newline and redistributes the parts into the row.
    Assigns the first part to the left subcolumn and the second part to the right subcolumn.
    If there's only one part, assigns it to the right subcolumn and sets the left to NaN.
    """
    if DEBUG_OUTPUT:
        print(f"""DEBUG_OUTPUT: split_and_rebuild_row input:
                row=pd.Series({row.tolist()!r})
                col_str={col_str!r}
                split_col_idx={split_col_idx}
                split_columns_info={split_columns_info}
                """)
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
    if DEBUG_OUTPUT:
        print(f"""DEBUG_OUTPUT: split_and_rebuild_row output:
                row=pd.Series({row.tolist()!r})
                """)
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
    if DEBUG_OUTPUT:
        print(f"DEBUG_OUTPUT: is_transaction_row input: {row}")
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

    if DEBUG_OUTPUT:
        print(f"DEBUG_OUTPUT: is_transaction_row output: {found_date and found_description and found_currency}")
    return found_date and found_description and found_currency

def clean_and_detect_transaction_table(table: DataFrame) -> Tuple[DataFrame, bool]:
    if DEBUG_OUTPUT:
        print(f"DEBUG_OUTPUT: clean_and_detect_transaction_table input: table=\n{format_dataframe_for_debug(table)}")
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
    
    if is_transaction and DEBUG_OUTPUT:
        print("\nProcessed Table:")
        print(f"\033[92m{processed_table}\033[0m")  # Green text for visibility
    
    if DEBUG_OUTPUT:
        print(f"DEBUG_OUTPUT: clean_and_detect_transaction_table output: processed_table=\n{format_dataframe_for_debug(processed_table)}, is_transaction={is_transaction}")
    return processed_table, is_transaction

def is_bank_account_table(table: pd.DataFrame) -> bool:
    # Check if the table contains headers typically found in bank account statements
    if DEBUG_OUTPUT:
        print(f"DEBUG_OUTPUT: is_bank_account_table input: \n{format_dataframe_for_debug(table)}")
    header_keywords = ['withdrawal', 'deposit', 'balance']
    header_row = table.iloc[:10].astype(str).apply(lambda x: x.str.lower())
    if DEBUG_OUTPUT:
        print(f"DEBUG_OUTPUT: is_bank_account_table output: {all(any(keyword in cell for cell in header_row.values.flatten()) for keyword in header_keywords)}")
    return all(any(keyword in cell for cell in header_row.values.flatten()) for keyword in header_keywords)

def parse_amount(amount_str):
    amount_str = amount_str.replace(',', '').replace(' ', '')
    is_negative = False

    if amount_str.startswith('('):
        is_negative = True
        amount_str = amount_str[1:]
    if amount_str.endswith(')'):
        is_negative = True
        amount_str = amount_str[:-1]

    if amount_str.endswith('CR'):
        is_negative = True
        amount_str = amount_str[:-2]
    elif amount_str.endswith('DR'):
        amount_str = amount_str[:-2]

    try:
        amount = float(amount_str)
        return -amount if is_negative else amount
    except ValueError:
        return None

def is_location(value_str):
    # Dynamic location detection using pycountry
    if DEBUG_OUTPUT:
        print(f"DEBUG_OUTPUT: is_location input: {value_str}")
    country_codes = {country.alpha_2 for country in countries}
    country_codes.update({country.alpha_3 for country in countries})
    country_names = {country.name.upper() for country in countries}
    location_keywords = country_codes.union(country_names)
    if DEBUG_OUTPUT:
        print(f"DEBUG_OUTPUT: is_location output: {value_str.upper() in location_keywords}")
    return value_str.upper() in location_keywords

NON_TRANSACTION_MARKERS = {
    'BALANCE B/F', 'BALANCE C/F', 'SUB-TOTAL', 'SUBTOTAL', 'TOTAL', 'NEW TRANSACTIONS',
    'Total Balance Carried Forward'
}

def get_additional_description(table_slice: pd.DataFrame, non_transaction_markers: Set[str]) -> str:
    if DEBUG_OUTPUT:
        print(f"DEBUG_OUTPUT: get_additional_description input: table_slice=\n{format_dataframe_for_debug(table_slice)}\nnon_transaction_markers={non_transaction_markers}")
    additional_text = []
    
    for _, row in table_slice.iterrows():
        if is_transaction_row(row) or any(
            marker in clean_text(str(val)).upper() for val in row for marker in non_transaction_markers
        ):
            break
        
        row_text = ' '.join(
            clean_text(str(val)) for val in row
            if pd.notna(val) and val != '' and not is_location(clean_text(str(val)))
        )
        
        # Ignore rows with repeated single characters or numbers
        if row_text and not all(len(word) == 1 for word in row_text.split()):
            additional_text.append(row_text)
    
    if DEBUG_OUTPUT:
        print(f"DEBUG_OUTPUT: get_additional_description output: {' '.join(additional_text)}")
    return ' '.join(additional_text)

def standardize_date(date_str, year=None):    
    # Helper function to parse date with flexible year
    def parse_date_with_year(date_str, format_str, year):
        for test_year in [year, datetime.now().year]:
            if test_year:
                try:
                    return datetime.strptime(f"{date_str} {test_year}", f"{format_str} %Y")
                except ValueError:
                    continue
        return None

    try:
        # Try parsing with day/month format
        date = parse_date_with_year(date_str, "%d/%m", year)
        if date:
            if year:
                return date.strftime("%d %B %Y")
            return date.strftime("%d %B")
    except ValueError:
        pass

    try:
        # Try parsing with day/month/year format
        date = datetime.strptime(date_str, "%d/%m/%Y")
        return date.strftime("%d %B %Y")
    except ValueError:
        pass

    try:
        # Try parsing with day month abbreviation format
        date = parse_date_with_year(date_str, "%d %b", year)
        if date:
            if year:
                return date.strftime("%d %B %Y")
            return date.strftime("%d %B")
    except ValueError:
        pass

    # If all parsing attempts fail, return the original string
    return date_str

def format_dataframe_for_debug(df):
    formatted = "pd.DataFrame({\n"
    for col in df.columns:
        formatted += f"    {col}: {df[col].tolist()},\n"
    formatted += "})"
    return formatted

def extract_bank_account_transactions(tables: List[pd.DataFrame], statement_year=None) -> List[Dict]:
    if DEBUG_OUTPUT:
        print(f"DEBUG_OUTPUT: extract_bank_account_transactions input: tables=")
        print("[")
        for table in tables:
            print(f"    {format_dataframe_for_debug(table)},")
        print("]")
        print(f"statement_year={statement_year}")
    
    transactions = []

    for table in tables:
        # Find the header row
        header_row = None
        for idx, row in table.iterrows():
            if is_header_row(row):
                header_row = row
                break
        
        if header_row is None:
            continue  # Skip this table if no header row found

        # Map headers to transaction keys
        header_mapping = {}
        for i, header in enumerate(header_row):
            header_lower = str(header).lower()
            if 'date' in header_lower:
                header_mapping['Date'] = i
            elif any(word in header_lower for word in ['withdrawal', 'debit']):
                header_mapping['Withdrawal'] = i
            elif any(word in header_lower for word in ['deposit', 'credit']):
                header_mapping['Deposit'] = i
            elif 'balance' in header_lower:
                header_mapping['Balance'] = i
            elif any(word in header_lower for word in ['description', 'transaction', 'particulars']):
                header_mapping['Description'] = i

        # Extract transactions
        current_transaction = None
        for idx, row in table.iterrows():
            if is_transaction_row(row):
                if current_transaction:
                    transactions.append(current_transaction)
                current_transaction = {}

                for key, col_idx in header_mapping.items():
                    value = clean_text(str(row.iloc[col_idx]))
                    if key == 'Date':
                        current_transaction[key] = standardize_date(value, statement_year)
                    elif key in ['Withdrawal', 'Deposit', 'Balance']:
                        current_transaction[key] = parse_amount(value)
                    else:
                        current_transaction[key] = value

                # Get the integer position of idx
                idx_pos = table.index.get_loc(idx)
                # Check next row for additional description
                additional_text = get_additional_description(table.iloc[idx_pos+1:idx_pos+11], NON_TRANSACTION_MARKERS)
                if additional_text:
                    current_transaction['Description'] += ' ' + additional_text

        if current_transaction:
            transactions.append(current_transaction)

    if DEBUG_OUTPUT:
        print(f"DEBUG_OUTPUT: extract_bank_account_transactions output: transactions={transactions}")
    return transactions

def extract_credit_card_transactions(tables: List[pd.DataFrame], statement_year=None) -> List[Dict]:
    if DEBUG_OUTPUT:
        print(f"DEBUG_OUTPUT: extract_credit_card_transactions input: tables=")
        print("[")
        for table in tables:
            print(f"    {format_dataframe_for_debug(table)},")
        print("]")
        print(f"statement_year={statement_year}")
    
    transactions = []
    excluded_pattern = re.compile(r'AUTO-PYT FROM ACCT#\d+ REF NO: \d+|PAYMENT BY GIRO')

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
                        current_transaction['Date'] = standardize_date(value_str, statement_year)
                        date_found = True
                    elif not amount_found and CURRENCY_PATTERN.search(value_str):
                        current_transaction['Amount'] = parse_amount(value_str)
                        amount_found = True
                    elif not is_location(value_str) and value_str != '':
                        description_parts.append(value_str)

                current_transaction['Description'] = ' '.join(description_parts)

                # Get the integer position of idx
                idx_pos = table.index.get_loc(idx)
                # Use the new function to get additional description
                additional_text = get_additional_description(table.iloc[idx_pos+1:idx_pos+11], NON_TRANSACTION_MARKERS)
                if additional_text:
                    current_transaction['Description'] += ' ' + additional_text
                
                # Exclude transactions matching the pattern
                if excluded_pattern.search(current_transaction['Description']):
                    current_transaction = {}
            else:
                continue  # Skip non-transaction rows

        if current_transaction:
            transactions.append(current_transaction)

    if DEBUG_OUTPUT:
        print(f"DEBUG_OUTPUT: extract_credit_card_transactions output: transactions={transactions}")
    return transactions

def extract_statement_date(table: pd.DataFrame, pdf_text: str) -> Tuple[Optional[str], Optional[str]]:
    # Patterns to match
    if DEBUG_OUTPUT:
        print(f"DEBUG_OUTPUT: extract_statement_date input: table=\n{format_dataframe_for_debug(table)}, pdf_text=\n{pdf_text!r}")
    date_patterns = [
        r'(\d{1,2}\s+[A-Za-z]+\s+\d{4})',  # e.g., "23 May 2024"
        r'(\d{1,2}\s+[A-Za-z]+\s+\d{4})\s+TO\s+(\d{1,2}\s+[A-Za-z]+\s+\d{4})',  # e.g., "1 JUL 2024 TO 31 JUL 2024"
        r'STATEMENT DATE[:\s]+(\d{1,2}\s+[A-Za-z]+\s+\d{4})',  # e.g., "STATEMENT DATE: 23 May 2024"
    ]
    
    for _, row in table.iterrows():
        for cell in row:
            if isinstance(cell, str):
                for pattern in date_patterns:
                    match = re.search(pattern, cell, re.IGNORECASE)
                    if match:
                        date_str = match.group(1)
                        try:
                            date = datetime.strptime(date_str, "%d %b %Y")
                            if 2010 <= date.year <= min(2050, datetime.now().year + 1):  # Guardrail for reasonable years
                                if DEBUG_OUTPUT:
                                    print(f"DEBUG_OUTPUT: extract_statement_date output: ({date_str}, {str(date.year)})")
                                return date_str, str(date.year)
                        except ValueError:
                            pass  # If parsing fails, continue to the next match
    
    # If no date found in the table, try to extract from the PDF content
    date_pattern = r'(\d{2}-\d{2}-\d{4})'
    match = re.search(date_pattern, pdf_text)
    if match:
        date_str = match.group(1)
        try:
            date = datetime.strptime(date_str, "%d-%m-%Y")
            if DEBUG_OUTPUT:
                print(f"DEBUG_OUTPUT: extract_statement_date output: ({date.strftime('%d %b %Y')}, {str(date.year)})")
            return date.strftime("%d %b %Y"), str(date.year)
        except ValueError:
            pass

    if DEBUG_OUTPUT:
        print("DEBUG_OUTPUT: extract_statement_date output: (None, None)")
    return None, None

def extract_pdf_text(file_path: str) -> str:
    with open(file_path, 'rb') as file:
        pdf_reader = PdfReader(file)
        return pdf_reader.pages[0].extract_text()

def main(file_path: str):
    if DEBUG_OUTPUT:
        print("\033[95m" + "=" * 80)  # Bright purple
        print(f"Processing file: {file_path}")
        print("=" * 80 + "\033[0m")  # Reset color
    
    tables = extract_tables(file_path)
    
    transaction_tables: List[pd.DataFrame] = []
    statement_date = None
    statement_year = None
    for table in tables:
        processed_table, is_transaction = clean_and_detect_transaction_table(table)
        if is_transaction:
            transaction_tables.append(processed_table)
        if not statement_date:
            pdf_text = extract_pdf_text(file_path)
            statement_date, statement_year = extract_statement_date(processed_table, pdf_text)
    
    if not statement_year:
        # If no year found in tables, try to extract from filename
        year_match = re.search(r'(20[1-4][0-9]|2050)', file_path)
        if year_match:
            statement_year = year_match.group(1)
    
    if DEBUG_OUTPUT:
        print(f"Statement Date: {statement_date}")
        print(f"Statement Year: {statement_year}")
    
    if any(is_bank_account_table(table) for table in transaction_tables):
        transactions = extract_bank_account_transactions(transaction_tables, statement_year)
    else:
        transactions = extract_credit_card_transactions(transaction_tables, statement_year)
    
    if not transactions:
        print("No transactions found")
        return []
    
    return transactions

def verify_transactions(transactions: List[Dict]) -> Dict:
    total_deposits = sum(Decimal(str(t.get('Deposit', 0) or 0)) for t in transactions)
    total_withdrawals = sum(Decimal(str(t.get('Withdrawal', 0) or 0)) for t in transactions)
    total_credit = sum(Decimal(str(t['Amount'])) for t in transactions if t.get('Amount', 0) < 0)
    total_debit = sum(abs(Decimal(str(t['Amount']))) for t in transactions if t.get('Amount', 0) > 0)
    
    # Use the same logic as is_bank_account_table
    is_bank_account = any('Balance' in t for t in transactions)
    
    if not is_bank_account:
        net_spend = total_debit + total_credit
        return {
            "total_credit": round(total_credit, 2),
            "total_debit": round(total_debit, 2),
            "net_spend": round(net_spend, 2)
        }
    else:
        first_balance = next((Decimal(str(t['Balance'])) for t in transactions if 'Balance' in t), None)
        last_balance = next((Decimal(str(t['Balance'])) for t in reversed(transactions) if 'Balance' in t), None)
        
        if first_balance is not None and last_balance is not None:
            # Adjust the first balance by reversing the first transaction
            first_transaction = transactions[0]
            first_deposit = Decimal(str(first_transaction.get('Deposit', 0) or 0))
            first_withdrawal = Decimal(str(first_transaction.get('Withdrawal', 0) or 0))
            adjusted_first_balance = first_balance - first_deposit + first_withdrawal
            
            calculated_last_balance = adjusted_first_balance + total_deposits - total_withdrawals
            balance_matches = abs(calculated_last_balance - last_balance) < Decimal('0.01')
        else:
            adjusted_first_balance = None
            calculated_last_balance = None
            balance_matches = None
        
        return {
            "total_deposits": round(total_deposits, 2),
            "total_withdrawals": round(total_withdrawals, 2),
            "starting_balance": round(adjusted_first_balance, 2) if adjusted_first_balance is not None else None,
            "ending_balance_from_file": round(last_balance, 2) if last_balance is not None else None,
            "ending_balance_from_calculations": round(calculated_last_balance, 2) if calculated_last_balance is not None else None,
            "balance_matches": balance_matches
        }

def parse_bank_statement(file_path: str, debug: bool = False, verify: bool = False) -> Dict:
    global DEBUG_OUTPUT
    DEBUG_OUTPUT = debug
    
    transactions = main(file_path)
    result = {
        "transactions": transactions,
        "verification_data": {}
    }

    if verify:
        result["verification_data"] = verify_transactions(transactions)

    return result

__all__ = ['parse_bank_statement', 'verify_transactions']