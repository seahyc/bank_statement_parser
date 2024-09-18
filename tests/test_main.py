import pytest
import pandas as pd
from src.main import (
    clean_text,
    is_transaction_row,
    standardize_date,
    parse_amount,
    is_location,
    extract_credit_card_transactions,
    detect_merged_rows,
    is_header_row,
    extract_bank_account_transactions,
    get_additional_description,
    split_and_rebuild_row,
    clean_and_detect_transaction_table,
    is_bank_account_table,
    extract_statement_date,
)

class TestMainFunctions:

    @pytest.mark.parametrize("input_str, expected", [
        ("  Hello\nWorld  ", "Hello World"),
        ("\t\nTest\r", "Test"),
        ("MERCHANT* FOOD A-123", "MERCHANT* FOOD A-123"),
        ("-1234 NET*STORE ENTER", "-1234 NET*STORE ENTER"),
        ("FOREIGN CURRENCY USD 20.00", "FOREIGN CURRENCY USD 20.00"),
        ("  -5678 RESTAURANT (MALL)  ", "-5678 RESTAURANT (MALL)"),
        ("COLL 100100123456", "COLL 100100123456"),
        ("CR CARD RECEIVABLE", "CR CARD RECEIVABLE"),
        ("via PayNow-QR Code", "via PayNow-QR Code"),
    ])
    def test_clean_text(self, input_str, expected):
        assert clean_text(input_str) == expected

    @pytest.mark.parametrize("row, expected", [
        (pd.Series(['17/08', 'MERCHANT* FOOD A-123', 'CITYVILLE', 'ABC', '1.68']), True),
        (pd.Series(['21/08', '-0315 ONLINE *SERVICE S', 'TECHCITY', 'XYZ', '27.06']), True),
        (pd.Series(['23/08', '-4887 CLOTHING STORE - I', 'FASHIONTOWN', 'FG', '109.90']), True),
        (pd.Series(['SUBTOTAL', '', '', '3,000.24']), False),
        (pd.Series(['FOREIGN CURRENCY USD 20.00', '', '', '']), False),
        (pd.Series(['30 JUL', '30 JUL', 'FUND TRANSFER', '', '3.90', '', '556,713.42']), True),
        (pd.Series(['', '', 'BALANCE C/F', '', '', '', '556,736.96']), False),
    ])
    def test_is_transaction_row(self, row, expected):
        assert is_transaction_row(row) == expected

    @pytest.mark.parametrize("date_str, year, expected", [
        ('17/08', '2024', '17 August 2024'),
        ('01/01/2023', None, '01 January 2023'),
        ('01 Jan', None, '01 January'),
        ('31/12', '2024', '31 December 2024'),
        ('29 Feb', '2024', '29 February 2024'),
        ('invalid date', None, 'invalid date'),
        ('01 AUG', '2024', '01 August 2024'),
        ('31 JUL', '2024', '31 July 2024'),
        ('20 MAY', '2024', '20 May 2024'),
        ('23 APR', '2024', '23 April 2024'),
        ('01 MAY', '2024', '01 May 2024'),
    ])
    def test_standardize_date(self, date_str, year, expected):
        assert standardize_date(date_str, year) == expected

    @pytest.mark.parametrize("amount_str, expected", [
        ('1.68', 1.68),
        ('(100.00)', -100.00),
        ('100.00CR', -100.00),
        ('100.00DR', 100.00),
        ('1,234.56', 1234.56),
        ('0.73', 0.73),
        ('N/A', None),
        ('(1,234.56', -1234.56),
        ('1,234.56CR', -1234.56),
        ('1,234.56DR', 1234.56),
        ('1,234,567.89)', -1234567.89),
        ('', None),
        ('11,357.00', 11357.00),
        ('3.90', 3.90),
        ('15,909.03 CR', -15909.03),
        ('140.85', 140.85),
        ('614.86', 614.86),
    ])
    def test_parse_amount(self, amount_str, expected):
        assert parse_amount(amount_str) == expected

    @pytest.mark.parametrize("value_str, expected", [
        ("SINGAPORE", True),
        ("SGP", True),
        ("USA", True),
        ("SG", True),
        ("UNITED STATES", True),
        ("MERCHANT* FOOD A-123", False),
        ("-4887 NET*STORE ENTER", False),
        ("UNITED OVE", False),
        ("AIRPORT", False),
        ("Company Pte Ltd", False),
        ("AUTO-PYT FROM ACCT#123456789012345", False),
        ("CUSTOMER.IO EMAIL MARK HTTPSCUSTOMER OR", False),
        ("DIGITALOCEAN.COM AMSTERDAM NL", False),
        ("U. S. DOLLAR 100.00", False),
        ("UNITED KINGDOM", True),
        ("JAPAN", True),
        ("PAYMENT RECEIVED", False),
        ("INTEREST CHARGED", False),
    ])
    def test_is_location(self, value_str, expected):
        assert is_location(value_str) == expected

    @pytest.mark.parametrize("col_str, expected", [
        ("Transaction\nValue", True),
        ("Deposit\nBalance", True),
        ("Date\nDate", True),
        ("01/07\nFAST PAYMENT\n700.00", True),
        ("BALANCE B/F", False),
        ("01 JUL", False),
        ("Transaction\nDate\nDescription", True),
        ("Amount\n(SGD)", True),
        ("TOTAL AMOUNT DUE", False),
    ])
    def test_detect_merged_rows(self, col_str, expected):
        assert detect_merged_rows(col_str) == expected

    @pytest.mark.parametrize("row, expected", [
        (pd.Series(['Date', 'Description', 'Withdrawal', 'Deposit', 'Balance']), True),
        (pd.Series(['Transaction Date', 'Value Date', 'Description', 'Amount']), True),
        (pd.Series(['01 JUL', 'FAST PAYMENT', '700.00', '', '577,169.97']), False),
        (pd.Series(['DATE', 'DESCRIPTION', 'AMOUNT (S$)']), True),
        (pd.Series(['20 MAY', 'AUTO-PYT FROM ACCT#123456789012345', '15,909.03 CR']), False),
    ])
    def test_is_header_row(self, row, expected):
        assert is_header_row(row) == expected

    @pytest.mark.parametrize("mock_data, year, expected_transactions", [
        (
            [
                pd.DataFrame({
                    0: ['SAVINGS ACCOUNT', 'Account No. 123456789012', 'Transaction', 'Date', '', '01 JUL', '', '', '', '03 JUL', '', '', '', '07 JUL', '', '', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '15 JUL', '', '', '', '18 JUL', '', '', '29 JUL', '', '', '', '30 JUL', '', '', '', '01 AUG', ''],
                    1: ['', '', 'Value', 'Date', '', '01 JUL', '', '', '', '03 JUL', '', '', '', '08 JUL', '', '', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '15 JUL', '', '', '', '18 JUL', '', '', '29 JUL', '', '', '', '30 JUL', '', '', '', '31 JUL', ''],
                    2: ['', '', '', 'Description', 'BALANCE B/F', 'FAST PAYMENT', '123456789', 'to JOHN DOE', 'OTHR - Other', 'FAST PAYMENT', 'via PayNow-Mobile', 'to JANE SMITH', 'OTHR - OTHR', 'FAST PAYMENT', '987654321', 'to ALICE JOHNSON', 'OTHR - Other', 'BONUS INTEREST', 'SALARY BONUS', 'BONUS INTEREST', 'CC SPEND BONUS', 'BONUS INTEREST', 'SAVE BONUS', 'BONUS INTEREST', 'GROW BONUS', 'POS PURCHASE    NETS', 'STORE A', 'STORE B', 'MALL', 'GIRO', 'COLL 100100123456', 'CR CARD RECEIVABLE', 'GIRO - SALARY', 'SALA', 'Company A Pte Ltd', 'Company A Pte Ltd', 'FUND TRANSFER', 'via PayNow-QR Code', 'to BOB BROWN', 'OTHR - OTHR', 'INTEREST CREDIT', 'BALANCE C/F'],
                    3: ['', '', '', 'Cheque', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    4: ['', '', '', 'Withdrawal', '', '700.00', '', '', '', '22.54', '', '', '', '3,000.00', '', '', '', '', '', '', '', '', '', '', '', '207.40', '', '', '', '2,155.03', '', '', '', '', '', '', '3.90', '', '', '', '', ''],
                    5: ['', '', '', 'Deposit', '', '', '', '', '', '', '', '', '', '', '', '', '', '205.47', '', '49.31', '', '123.28', '', '197.26', '', '', '', '', '', '', '', '', '5,357.00', '', '', '', '', '', '', '', '23.54', ''],
                    6: ['1  JUL 2024 TO 31 JUL 2024', '', '', 'Balance', '57,869.97', '57,169.97', '', '', '', '57,147.43', '', '', '', '54,147.43', '', '', '', '54,352.90', '', '54,402.21', '', '54,525.49', '', '54,722.75', '', '54,515.35', '', '', '', '52,360.32', '', '', '57,717.32', '', '', '', '57,713.42', '', '', '', '57,736.96', '57,736.96'],
                    7: ['', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 'Sample Banking Corporation Limited'],
                })
            ],
            2024,
            [
                {'Date': '01 July 2024', 'Description': 'FAST PAYMENT 123456789 to JOHN DOE OTHR - Other', 'Withdrawal': 700.0, 'Deposit': None, 'Balance': 57169.97},
                {'Date': '03 July 2024', 'Description': 'FAST PAYMENT via PayNow-Mobile to JANE SMITH OTHR - OTHR', 'Withdrawal': 22.54, 'Deposit': None, 'Balance': 57147.43},
                {'Date': '08 July 2024', 'Description': 'FAST PAYMENT 987654321 to ALICE JOHNSON OTHR - Other', 'Withdrawal': 3000.0, 'Deposit': None, 'Balance': 54147.43},
                {'Date': '09 July 2024', 'Description': 'BONUS INTEREST SALARY BONUS', 'Withdrawal': None, 'Deposit': 205.47, 'Balance': 54352.9},
                {'Date': '09 July 2024', 'Description': 'BONUS INTEREST CC SPEND BONUS', 'Withdrawal': None, 'Deposit': 49.31, 'Balance': 54402.21},
                {'Date': '09 July 2024', 'Description': 'BONUS INTEREST SAVE BONUS', 'Withdrawal': None, 'Deposit': 123.28, 'Balance': 54525.49},
                {'Date': '09 July 2024', 'Description': 'BONUS INTEREST GROW BONUS', 'Withdrawal': None, 'Deposit': 197.26, 'Balance': 54722.75},
                {'Date': '15 July 2024', 'Description': 'POS PURCHASE NETS STORE A STORE B MALL', 'Withdrawal': 207.4, 'Deposit': None, 'Balance': 54515.35},
                {'Date': '18 July 2024', 'Description': 'GIRO COLL 100100123456 CR CARD RECEIVABLE', 'Withdrawal': 2155.03, 'Deposit': None, 'Balance': 52360.32},
                {'Date': '29 July 2024', 'Description': 'GIRO - SALARY SALA Company A Pte Ltd Company A Pte Ltd', 'Withdrawal': None, 'Deposit': 5357.0, 'Balance': 57717.32},
                {'Date': '30 July 2024', 'Description': 'FUND TRANSFER via PayNow-QR Code to BOB BROWN OTHR - OTHR', 'Withdrawal': 3.9, 'Deposit': None, 'Balance': 57713.42},
                {'Date': '31 July 2024', 'Description': 'INTEREST CREDIT', 'Withdrawal': None, 'Deposit': 23.54, 'Balance': 57736.96}
            ]
        ),
        (
            [
                pd.DataFrame({
                    0: ['', '', '', '', '', '', '', '', '', '', '', '', '4', '', '', ''],
                    1: ['SAVINGS Account', 'Date', '', '19/08/2024', '', '', '', '28/08/2024', '', '', '', '31/08/2024', '', '', 'MULTIPLIER Account', 'Date'],
                    2: ['', 'Description', 'Balance Brought Forward', 'Payments / Collections via GIRO', 'CARD CENTRE', 'CARDHOLDER', '123456789012', 'FAST Payment / Receipt', 'LOAN REPAYMENT', '20240828BANKSGSGBRT1234567', 'OTHER', 'Interest Earned', '4', 'Total Balance Carried Forward:', '', 'Description'],
                    3: ['', '', '', '', '', '', '', '', '', '', '', '', '4', '', '', ''],
                    4: ['', 'Withdrawal (-)', '', '1,269.68', '', '', '', '', '', '', '', '', '', '1,269.68', '', 'Withdrawal (-)'],
                    5: ['', '', '', '', '', '', '', '', '', '', '', '', '4', '', '', ''],
                    6: ['', 'Deposit (+)', '', '', '', '', '', '1,000.00', '', '', '', '0.89', '', '1,000.89', '', 'Deposit (+)'],
                    7: ['Account No. 123-45678-9', '', '', '', '', '', '', '', '', '', '', '', '4', '', 'Account No. 987-654321-0', ''],
                    8: ['', 'Balance', '26,298.51', '25,028.83', '', '', '', '26,028.83', '', '', '', '26,029.72', '', '26,029.72', '', 'Balance'],
                })
            ],
            2024,
            [
                {'Date': '19 August 2024', 'Description': 'Payments / Collections via GIRO CARD CENTRE CARDHOLDER 123456789012', 'Withdrawal': 1269.68, 'Deposit': None, 'Balance': 25028.83},
                {'Date': '28 August 2024', 'Description': 'FAST Payment / Receipt LOAN REPAYMENT 20240828BANKSGSGBRT1234567 OTHER', 'Withdrawal': None, 'Deposit': 1000.0, 'Balance': 26028.83},
                {'Date': '31 August 2024', 'Description': 'Interest Earned', 'Withdrawal': None, 'Deposit': 0.89, 'Balance': 26029.72}
            ]
        )
    ])
    def test_extract_bank_account_transactions(self, mock_data, year, expected_transactions):
        transactions = extract_bank_account_transactions(mock_data, year)
        
        assert len(transactions) == len(expected_transactions)
        for actual, expected in zip(transactions, expected_transactions):
            assert actual == expected

    @pytest.mark.parametrize("mock_table, expected_description", [
        (pd.DataFrame({
            0: ['', '', '', '03 JUL', '', '', '', '07 JUL', '', ''],
            1: ['', '', '', '03 JUL', '', '', '', '08 JUL', '', ''],
            2: ['123456789', 'to JOHN DOE', 'OTHR - Other', 'FAST PAYMENT', 'via PayNow-Mobile', 'to JANE SMITH', 'OTHR - OTHR', 'FAST PAYMENT', '987654321', 'to ALICE JOHNSON'],
            3: ['', '', '', '', '', '', '', '', '', ''],
            4: ['', '', '', '22.54', '', '', '', '3,000.00', '', ''],
            5: ['', '', '', '', '', '', '', '', '', ''],
            6: ['', '', '', '57,7147.43', '', '', '', '54,7147.43', '', ''],
            7: ['', '', '', '', '', '', '', '', '', ''],
        }), '123456789 to JOHN DOE OTHR - Other'),
        (pd.DataFrame({
            0: ['', '', '', '07 JUL', '', '', '', '09 JUL', '', '09 JUL'],
            1: ['', '', '', '08 JUL', '', '', '', '09 JUL', '', '09 JUL'],
            2: ['via PayNow-Mobile', 'to JANE SMITH', 'OTHR - OTHR', 'FAST PAYMENT', '987654321', 'to ALICE JOHNSON', 'OTHR - Other', 'BONUS INTEREST', 'SALARY BONUS', 'BONUS INTEREST'],
            3: ['', '', '', '', '', '', '', '', '', ''],
            4: ['', '', '', '3,000.00', '', '', '', '', '', ''],
            5: ['', '', '', '', '', '', '', '205.47', '', '49.31'],
            6: ['', '', '', '54,7147.43', '', '', '', '54,7352.90', '', '54,7402.21'],
            7: ['', '', '', '', '', '', '', '', '', ''],
        }), 'via PayNow-Mobile to JANE SMITH OTHR - OTHR'),
        (pd.DataFrame({
            0: ['', '', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL'],
            1: ['', '', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL'],
            2: ['987654321', 'to ALICE JOHNSON', 'OTHR - Other', 'BONUS INTEREST', 'SALARY BONUS', 'BONUS INTEREST', 'CC SPEND BONUS', 'BONUS INTEREST', 'SAVE BONUS', 'BONUS INTEREST'],
            3: ['', '', '', '', '', '', '', '', '', ''],
            4: ['', '', '', '', '', '', '', '', '', ''],
            5: ['', '', '', '205.47', '', '49.31', '', '123.28', '', '197.26'],
            6: ['', '', '', '54,7352.90', '', '54,7402.21', '', '54,7525.49', '', '54,7722.75'],
            7: ['', '', '', '', '', '', '', '', '', ''],
        }), '987654321 to ALICE JOHNSON OTHR - Other'),
        (pd.DataFrame({
            0: ['', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL'],
            1: ['', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL'],
            2: ['SALARY BONUS', 'BONUS INTEREST', 'CC SPEND BONUS', 'BONUS INTEREST', 'SAVE BONUS', 'BONUS INTEREST', 'GROW BONUS', 'POS PURCHASE    NETS', 'STORE A', 'STORE B'],
            3: ['', '', '', '', '', '', '', '', '', ''],
            4: ['', '', '', '', '', '', '', '207.40', '', ''],
            5: ['', '49.31', '', '123.28', '', '197.26', '', '', '', ''],
            6: ['', '54,7402.21', '', '54,7525.49', '', '54,7722.75', '', '54,7515.35', '', ''],
            7: ['', '', '', '', '', '', '', '', '', ''],
        }), 'SALARY BONUS'),
        (pd.DataFrame({
            0: ['', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL'],
            1: ['', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL'],
            2: ['CC SPEND BONUS', 'BONUS INTEREST', 'SAVE BONUS', 'BONUS INTEREST', 'GROW BONUS', 'POS PURCHASE    NETS', 'STORE A', 'STORE B', 'MALL', 'GIRO'],
            3: ['', '', '', '', '', '', '', '', '', ''],
            4: ['', '', '', '', '', '207.40', '', '', '', '2,155.03'],
            5: ['', '123.28', '', '197.26', '', '', '', '', '', ''],
            6: ['', '54,7525.49', '', '54,7722.75', '', '54,7515.35', '', '', '', '54,5360.32'],
            7: ['', '', '', '', '', '', '', '', '', ''],
        }), 'CC SPEND BONUS'),
        (pd.DataFrame({
            0: ['', '15 JUL', '', '', '', '18 JUL', '', '', '29 JUL', ''],
            1: ['', '15 JUL', '', '', '', '18 JUL', '', '', '29 JUL', ''],
            2: ['GROW BONUS', 'POS PURCHASE    NETS', 'STORE A', 'STORE B', 'MALL', 'GIRO', 'COLL 100100123456', 'CR CARD RECEIVABLE', 'GIRO - SALARY', 'SALA'],
            3: ['', '', '', '', '', '', '', '', '', ''],
            4: ['', '207.40', '', '', '', '2,155.03', '', '', '', ''],
            5: ['', '', '', '', '', '', '', '', '11,357.00', ''],
            6: ['', '54,7515.35', '', '', '', '54,5360.32', '', '', '55,6717.32', ''],
            7: ['', '', '', '', '', '', '', '', '', ''],
        }), 'GROW BONUS'),
        (pd.DataFrame({
            0: ['', '', '', '18 JUL', '', '', '29 JUL', '', '', ''],
            1: ['', '', '', '18 JUL', '', '', '29 JUL', '', '', ''],
            2: ['STORE A', 'STORE B', 'MALL', 'GIRO', 'COLL 100100123456', 'CR CARD RECEIVABLE', 'GIRO - SALARY', 'SALA', 'Company A Pte Ltd', 'Company A Pte Ltd'],
            3: ['', '', '', '', '', '', '', '', '', ''],
            4: ['', '', '', '2,155.03', '', '', '', '', '', ''],
            5: ['', '', '', '', '', '', '11,357.00', '', '', ''],
            6: ['', '', '', '54,5360.32', '', '', '55,6717.32', '', '', ''],
            7: ['', '', '', '', '', '', '', '', '', ''],
        }), 'STORE A STORE B MALL'),
        (pd.DataFrame({
            0: ['', '', '29 JUL', '', '', '', '30 JUL', '', '', ''],
            1: ['', '', '29 JUL', '', '', '', '30 JUL', '', '', ''],
            2: ['COLL 100100123456', 'CR CARD RECEIVABLE', 'GIRO - SALARY', 'SALA', 'Company A Pte Ltd', 'Company A Pte Ltd', 'FUND TRANSFER', 'via PayNow-QR Code', 'to BOB BROWN', 'OTHR - OTHR'],
            3: ['', '', '', '', '', '', '', '', '', ''],
            4: ['', '', '', '', '', '', '3.90', '', '', ''],
            5: ['', '', '11,357.00', '', '', '', '', '', '', ''],
            6: ['', '', '55,6717.32', '', '', '', '55,6713.42', '', '', ''],
            7: ['', '', '', '', '', '', '', '', '', ''],
        }), 'COLL 100100123456 CR CARD RECEIVABLE'),
        (pd.DataFrame({
            0: ['', '', '', '', '', '', '', '', '4', ''],
            1: ['', '', '', '28/08/2024', '', '', '', '31/08/2024', '', ''],
            2: ['CARD CENTRE', 'CARDHOLDER', '123456789012', 'FAST Payment / Receipt', 'LOAN REPAYMENT', '20240828BANKSGSGBRT1234567', 'OTHER', 'Interest Earned', '4', 'Total Balance Carried Forward:'],
            3: ['', '', '', '', '', '', '', '', '4', ''],
            4: ['', '', '', '', '', '', '', '', '', '1,269.68'],
            5: ['', '', '', '', '', '', '', '', '4', ''],
            6: ['', '', '', '1,000.00', '', '', '', '0.89', '', '1,000.89'],
            7: ['', '', '', '', '', '', '', '', '4', ''],
            8: ['', '', '', '26,028.83', '', '', '', '26,029.72', '', '26,029.72'],
        }), 'CARD CENTRE CARDHOLDER 123456789012'),
        (pd.DataFrame({
            0: ['', '21/05', '21/05', '22/05', '23/05', '16/05', '', '', '', ''],
            1: ['FOREIGN CURRENCY USD 20.00', '-1234 FOOD COURT', '-1234 FOOD COURT', '-1234 PLANT STORE', 'SPORTS CENTRE', 'CCY CONVERSION FEE', 'FOR: 246.68 SGD', 'SUBTOTAL', 'TOTAL', 'TOTAL AMOUNT DUE'],
            2: ['', 'CITYVILLE', 'CITYVILLE', 'CITYVILLE', 'CITYVILLE', '', '', '', '', ''],
            3: ['', 'ABC', 'ABC', 'ABC', 'ABC', '', '', '', '', ''],
            4: ['', '8.40', '5.00', '54.19', '1.80', '2.47', '', '1,623.06', '1,623.06', '1,623.06'],
        }), 'FOREIGN CURRENCY USD 20.00'),
        # Examples that output an empty string
        (pd.DataFrame({
            0: ['20 MAY', '23 APR', '01 MAY'],
            1: ['AUTO-PYT FROM ACCT#123456789012345', 'CUSTOMER.IO EMAIL MARK HTTPSCUSTOMER OR', 'DIGITALOCEAN.COM AMSTERDAM NL'],
            2: ['15,909.03 CR', '140.85', '614.86']
        }), ''),
        (pd.DataFrame({
            0: ['24/04', '24/04', '25/04', '25/04', '26/04', '26/04', '26/04', '28/04', '29/04', '30/04'],
            1: ['MERCHANT* FOOD A-123', 'MERCHANT* STORE B', 'MERCHANT* FOOD C-456', 'MERCHANT* FOOD D-789', 'MERCHANT* FOOD E-012', 'MERCHANT* FOOD E-012', '-1234 BUS/MRT 123456789', '-1234 SUPERMARKET A', '-1234 RESTAURANT B', '-1234 FOOD COURT C'],
            2: ['CITYVILLE', 'CITYVILLE', 'CITYVILLE', 'CITYVILLE', 'CITYVILLE', 'CITYVILLE', 'CITYVILLE', 'CITYVILLE', 'CITYVILLE', 'CITYVILLE'],
            3: ['ABC', 'ABC', 'ABC', 'ABC', 'ABC', 'ABC', 'ABC', 'DE', 'ABC', 'ABC'],
            4: ['1.81', '62.25', '1.94', '2.33', '11.46', '0.51', '15.47', '2.55', '53.15', '7.20'],
            5: ['', '', '', '', '', '', '', '', '', ''],
        }), ''),
        (pd.DataFrame({
            0: ['23/05', '16/05', '', '', '', ''],
            1: ['SPORTS CENTRE', 'CCY CONVERSION FEE', 'FOR: 246.68 SGD', 'SUBTOTAL', 'TOTAL', 'TOTAL AMOUNT DUE'],
            2: ['CITYVILLE', '', '', '', '', ''],
            3: ['ABC', '', '', '', '', ''],
            4: ['1.80', '2.47', '', '1,623.06', '1,623.06', '1,623.06'],
        }), ''),
    ])
    def test_get_additional_description(self, mock_table, expected_description):
        non_transaction_markers = {'SUB-TOTAL', 'BALANCE C/F', 'TOTAL', 'BALANCE B/F', 'Total Balance Carried Forward', 'NEW TRANSACTIONS'}
        assert get_additional_description(mock_table, non_transaction_markers) == expected_description

    @pytest.mark.parametrize("mock_data, year, expected_transactions", [
        (
            [
                pd.DataFrame({
                    0: ['Credit Cards', 'Statement of Account', '', '(cid:2)(cid:1)(cid:3)(cid:2)(cid:4)(cid:3)(cid:1)(cid:4)(cid:2)(cid:3)(cid:2)(cid:1)(cid:4)(cid:1)(cid:4)(cid:1)(cid:4)(cid:3)(cid:1)(cid:2)(cid:4)(cid:3)(cid:2)(cid:1)(cid:4)(cid:1)(cid:4)(cid:4)(cid:1)(cid:4)', 'JOHN DOE', '123 MAIN STREET', 'CITY TOWERS', '#10-20', 'SINGAPORE 123456', '', '', 'STATEMENT DATE\nCREDIT LIMIT', '23 May 2024\n$100,000.00', '', '', '', '', '', 'This Statement serves as a TAX INVOICE if GST is charged.', 'DATE', '', '', '20 MAY', '', '', '23 APR', '', '01 MAY', '', '', '', '', '', '', '', '', '', '', 'STATEMENT', '', '', '', ''],
                    1: ['', '', '1 of 3', '', '', '', '', '', '', 'DBS Cards P.O. Box 360 S(912312)', 'Hotline: 1800 111 1111', 'MINIMUM PAYMENT\nPAYMENT DUE DATE', '$50.00\n18 Jun 2024', "Please settle this statement promptly. If minimum payment is not received by 'Payment Due Date', a late payment charge of $100 will", 'be  levied.  If  payment  is  not  made  in  full,  an  additional  fiNonece  charge  of  27.80%  per  annum  will  be  levied  on  each  outstanding', 'balance  of  each  card  account  from  the  date  each  transaction  was  effected.  No  fiNonece  charge  will  be  levied  on  new  transactions', '(except Cash Advance transactions) effected after this statement date. Please refer to the last page of statement for more details.', 'Co. Reg. No. 196800306E', 'GST Registration No: MR-8500180-3', 'DESCRIPTION', 'DBS LADIES VISA CARD NO.: 1234 5678 9012 3456', 'PREVIOUS BALANCE', 'AUTO-PYT FROM ACCT#123456789012345', 'REF NO: 11689999398715999971650', 'NEW TRANSACTIONS JOHN DOE', 'CUSTOMER.IO EMAIL MARK HTTPSCUSTOMER OR', 'U. S. DOLLAR 100.00', 'DIGITALOCEAN.COM       AMSTERDAM     NL', 'U. S. DOLLAR 436.01', '', '', 'Any Full Amount due will be deducted from bank account 123456789. GIRO deduction date: 18 Jun 2024', '', 'Please contact our Customer Service Officer immediately at 1800 111 1111 or 1800 732 8000 (for Platinum Customers), if you find any', '', 'correct.', 'DBS VISA/MASTERCARD/AMEX CARD - DBS POINTS SUMMARY (AS OF THIS STATEMENT)', '', 'ADJUSTED', '', '', 'This statement is for your information only.', ''],
                    2: [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 'AMOUNT (S$)', '', '10,500.00', '10,500.00 CR', '', '', '150.75', '', '625.50', '', 'SUB-TOTAL:\n776.25', 'TOTAL:\n776.25', '', 'GRAND TOTAL FOR ALL CARD ACCOUNTS:\n776.25', '', 'discrepancies on your statement. If no discrepancy is reported within 7 days upon receipt of statement, your statement will be considered', '', '', 'REDEEMED/\nBALANCE\nEXPIRING ON', 'EXPIRED\n30 JUN 2024', '0\n250,000\nNo Expiry', '0\n250,000\n0', '', 'PDS_CRCRDGCE_LOC_ESTMT_0d67011d0000003c_07973'],
                }),
            ],
            '2024',
            [
                {
                    'Date': '23 April 2024',
                    'Amount': 150.75,
                    'Description': 'CUSTOMER.IO EMAIL MARK HTTPSCUSTOMER OR U. S. DOLLAR 100.00'
                },
                {
                    'Date': '01 May 2024',
                    'Amount': 625.50,
                    'Description': 'DIGITALOCEAN.COM AMSTERDAM NL U. S. DOLLAR 436.01'
                }
            ]
        ),
        (
            [
                pd.DataFrame({
                    0: ['OCBC 180 CARD', 'JOHN DOE', '', '17/05', '24/04', '24/04', '25/04', '25/04', '26/04', '26/04', '26/04', '28/04', '29/04', '30/04', '30/04', '30/04', '01/05', '01/05', '01/05', '01/05', '02/05', '02/05', '02/05', '02/05', '03/05', '03/05', '03/05', '06/05', '06/05', '06/05', '07/05', '07/05'],
                    1: ['', '1234-5678-9012-3456', "LAST MONTH'S BALANCE", 'PAYMENT BY GIRO', 'AMAZE* GRAB A-6AR2I', 'AMAZE* HUMVENTURES', 'AMAZE* GRAB A-6AT77', 'AMAZE* GRAB A-6AV28', 'AMAZE* GRAB A-6BXUJ', 'AMAZE* GRAB A-6BXUJ', '-7758 BUS/MRT 429799071', '-7758 NTUC FP-BT PANJANG', '-7758 LILAC OAK', '-7758 KOUFU PTE LTD', 'AMAZE* CTCPPT BDS V', '-4887 BUS/MRT 431965453', '-7758 HELLORIDE', '-4887 ATLAS COFFEEHOUSE', '-4887 AN ACAI AFFAIR HI', '-7758 BUS/MRT 432407001', '-7758 SUPER SIMPLE ONE', 'GOJEK', '-5831 CIRCLES.LIFE', '-7758 TAKAGI RAMEN-CONN', '-7758 KOUFU PTE LTD', '-7758 KOUFU PTE LTD', '-7758 HELLORIDE', '-4887 7 ELEVEN-ONE NORT', '-4887 KOUFU PTE LTD', '-4887 BUS/MRT 433911127', '-4887 BANGKOK JAM - PLA', '-7758 BUS/MRT 435033935'],
                    2: ['', '', '', '', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', '31353135', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE'],
                    3: ['', '', '', '', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SG', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP'],
                    4: ['', '', '800.00', '(750.00', '2.00', '60.00', '2.50', '3.00', '10.00', '1.00', '15.00', '3.00', '50.00', '8.00', '(50.00', '6.00', '1.50', '9.00', '11.00', '10.00', '14.00', '25.00', '35.00', '8.50', '5.50', '6.50', '3.50', '1.50', '9.00', '20.00', '50.00', '16.00'],
                    5: ['', '', '', ')', '', '', '', '', '', '', '', '', '', '', ')', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                }),
                pd.DataFrame({
                    0: ['TRANSACTION DATE', '08/05', '10/05', '11/05', '11/05', '11/05', '11/05', '11/05', '11/05', '11/05', '12/05', '13/05', '13/05', '13/05', '13/05', '14/05', '15/05', '15/05', '16/05', '16/05', '16/05', '16/05', '17/05', '18/05', '18/05', '18/05', '18/05', '21/05', '21/05', '', '21/05', '21/05', '22/05', '23/05', '16/05', '', '', '', ''],
                    1: ['DESCRIPTION', '-7758 SUPER SIMPLE ONE', '-7758 KOUFU PTE LTD', 'AMAZE* CSPRIMER.COM', 'AMAZE* GRAB A-6DX6H', '-7758 BOTANICT', '-7758 YA KUN BT PANJANG', '-7758 STARBUCKS@BKT PJG', '-7758 FRUITS VENDING PT', '-7758 BUS/MRT 437694781', 'GOOGLE STORAGE', 'SISTIC MBS', '-4887 KOUFU PTE LTD', '-4887 KOUFU PTE LTD', '-4887 BUS/MRT 438819449', '-7758 SUPER SIMPLE ONE', '-7758 SUBWAY - ONE NORTH', '-4887 MR BEAN INT L-FUS', 'TRAVELOKA*1144026038     SINGAPORE    SG', '-7758 MR BEAN INT L-FUS', '-5250 AIRBNB * HMX283ZD', '-7758 BUS/MRT 440276407', '-7758 KOUFU PTE LTD', '-7758 HELLORIDE', '-7758 SUZUKI COFFEE', '-7758 HELLORIDE', '-7758 GET*GET*MOTHER EA', '-7758 WWW.HEARTBREAKMEL', '-0315 OPENAI *CHATGPT S', 'FOREIGN CURRENCY USD 20.00', '-7758 KOUFU PTE LTD', '-7758 KOUFU PTE LTD', '-7758 CLOVER PLANT BASE', 'ACTIVESG SPORT SINGAPO', 'CCY CONVERSION FEE', 'FOR: 250.00 SGD', 'SUBTOTAL', 'TOTAL', 'TOTAL AMOUNT DUE'],
                    2: ['', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', '6564162232', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', '', 'SINGAPORE', '653-163-1004', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SAN FRANCISCO', '', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', '', '', '', '', ''],
                    3: ['', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SG', 'SGP', '', 'SGP', 'GBR', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'USA', '', 'SGP', 'SGP', 'SGP', 'SGP', '', '', '', '', ''],
                    4: ['AMOUNT', '16.50', '7.50', '55.00', '22.00', '40.00', '3.50', '8.00', '2.50', '13.00', '3.00', '350.00', '6.50', '5.50', '14.50', '14.00', '7.00', '6.00', '230.00', '5.00', '250.00', '12.00', '6.00', '1.50', '9.50', '1.50', '25.00', '11.50', '28.00', '', '8.50', '5.50', '55.00', '2.00', '2.50', '', '1,600.00', '1,600.00', '1,600.00'],
                }),
            ],
            '2025',
            [
                {'Date': '24 April 2025', 'Amount': 2.00, 'Description': 'AMAZE* GRAB A-6AR2I'},
                {'Date': '24 April 2025', 'Amount': 60.00, 'Description': 'AMAZE* HUMVENTURES'},
                {'Date': '25 April 2025', 'Amount': 2.50, 'Description': 'AMAZE* GRAB A-6AT77'},
                {'Date': '25 April 2025', 'Amount': 3.00, 'Description': 'AMAZE* GRAB A-6AV28'},
                {'Date': '26 April 2025', 'Amount': 10.00, 'Description': 'AMAZE* GRAB A-6BXUJ'},
                {'Date': '26 April 2025', 'Amount': 1.00, 'Description': 'AMAZE* GRAB A-6BXUJ'},
                {'Date': '26 April 2025', 'Amount': 15.00, 'Description': '-7758 BUS/MRT 429799071'},
                {'Date': '28 April 2025', 'Amount': 3.00, 'Description': '-7758 NTUC FP-BT PANJANG'},
                {'Date': '29 April 2025', 'Amount': 50.00, 'Description': '-7758 LILAC OAK'},
                {'Date': '30 April 2025', 'Amount': 8.00, 'Description': '-7758 KOUFU PTE LTD'},
                {'Date': '30 April 2025', 'Amount': -50.00, 'Description': 'AMAZE* CTCPPT BDS V )'},
                {'Date': '30 April 2025', 'Amount': 6.00, 'Description': '-4887 BUS/MRT 431965453'},
                {'Date': '01 May 2025', 'Amount': 1.50, 'Description': '-7758 HELLORIDE'},
                {'Date': '01 May 2025', 'Amount': 9.00, 'Description': '-4887 ATLAS COFFEEHOUSE'},
                {'Date': '01 May 2025', 'Amount': 11.00, 'Description': '-4887 AN ACAI AFFAIR HI'},
                {'Date': '01 May 2025', 'Amount': 10.00, 'Description': '-7758 BUS/MRT 432407001'},
                {'Date': '02 May 2025', 'Amount': 14.00, 'Description': '-7758 SUPER SIMPLE ONE'},
                {'Date': '02 May 2025', 'Amount': 25.00, 'Description': 'GOJEK 31353135'},
                {'Date': '02 May 2025', 'Amount': 35.00, 'Description': '-5831 CIRCLES.LIFE'},
                {'Date': '02 May 2025', 'Amount': 8.50, 'Description': '-7758 TAKAGI RAMEN-CONN'},
                {'Date': '03 May 2025', 'Amount': 5.50, 'Description': '-7758 KOUFU PTE LTD'},
                {'Date': '03 May 2025', 'Amount': 6.50, 'Description': '-7758 KOUFU PTE LTD'},
                {'Date': '03 May 2025', 'Amount': 3.50, 'Description': '-7758 HELLORIDE'},
                {'Date': '06 May 2025', 'Amount': 1.50, 'Description': '-4887 7 ELEVEN-ONE NORT'},
                {'Date': '06 May 2025', 'Amount': 9.00, 'Description': '-4887 KOUFU PTE LTD'},
                {'Date': '06 May 2025', 'Amount': 20.00, 'Description': '-4887 BUS/MRT 433911127'},
                {'Date': '07 May 2025', 'Amount': 50.00, 'Description': '-4887 BANGKOK JAM - PLA'},
                {'Date': '07 May 2025', 'Amount': 16.00, 'Description': '-7758 BUS/MRT 435033935'},
                {'Date': '08 May 2025', 'Amount': 16.50, 'Description': '-7758 SUPER SIMPLE ONE'},
                {'Date': '10 May 2025', 'Amount': 7.50, 'Description': '-7758 KOUFU PTE LTD'},
                {'Date': '11 May 2025', 'Amount': 55.00, 'Description': 'AMAZE* CSPRIMER.COM'},
                {'Date': '11 May 2025', 'Amount': 22.00, 'Description': 'AMAZE* GRAB A-6DX6H'},
                {'Date': '11 May 2025', 'Amount': 40.00, 'Description': '-7758 BOTANICT'},
                {'Date': '11 May 2025', 'Amount': 3.50, 'Description': '-7758 YA KUN BT PANJANG'},
                {'Date': '11 May 2025', 'Amount': 8.00, 'Description': '-7758 STARBUCKS@BKT PJG'},
                {'Date': '11 May 2025', 'Amount': 2.50, 'Description': '-7758 FRUITS VENDING PT'},
                {'Date': '11 May 2025', 'Amount': 13.00, 'Description': '-7758 BUS/MRT 437694781'},
                {'Date': '12 May 2025', 'Amount': 3.00, 'Description': 'GOOGLE STORAGE'},
                {'Date': '13 May 2025', 'Amount': 350.00, 'Description': 'SISTIC MBS 6564162232'},
                {'Date': '13 May 2025', 'Amount': 6.50, 'Description': '-4887 KOUFU PTE LTD'},
                {'Date': '13 May 2025', 'Amount': 5.50, 'Description': '-4887 KOUFU PTE LTD'},
                {'Date': '13 May 2025', 'Amount': 14.50, 'Description': '-4887 BUS/MRT 438819449'},
                {'Date': '14 May 2025', 'Amount': 14.00, 'Description': '-7758 SUPER SIMPLE ONE'},
                {'Date': '15 May 2025', 'Amount': 7.00, 'Description': '-7758 SUBWAY - ONE NORTH'},
                {'Date': '15 May 2025', 'Amount': 6.00, 'Description': '-4887 MR BEAN INT L-FUS'},
                {'Date': '16 May 2025', 'Amount': 230.00, 'Description': 'TRAVELOKA*1144026038 SINGAPORE SG'},
                {'Date': '16 May 2025', 'Amount': 5.00, 'Description': '-7758 MR BEAN INT L-FUS'},
                {'Date': '16 May 2025', 'Amount': 250.00, 'Description': '-5250 AIRBNB * HMX283ZD 653-163-1004'},
                {'Date': '16 May 2025', 'Amount': 12.00, 'Description': '-7758 BUS/MRT 440276407'},
                {'Date': '17 May 2025', 'Amount': 6.00, 'Description': '-7758 KOUFU PTE LTD'},
                {'Date': '18 May 2025', 'Amount': 1.50, 'Description': '-7758 HELLORIDE'},
                {'Date': '18 May 2025', 'Amount': 9.50, 'Description': '-7758 SUZUKI COFFEE'},
                {'Date': '18 May 2025', 'Amount': 1.50, 'Description': '-7758 HELLORIDE'},
                {'Date': '18 May 2025', 'Amount': 25.00, 'Description': '-7758 GET*GET*MOTHER EA'},
                {'Date': '21 May 2025', 'Amount': 11.50, 'Description': '-7758 WWW.HEARTBREAKMEL'},
                {'Date': '21 May 2025', 'Amount': 28.00, 'Description': '-0315 OPENAI *CHATGPT S SAN FRANCISCO FOREIGN CURRENCY USD 20.00'},
                {'Date': '21 May 2025', 'Amount': 8.50, 'Description': '-7758 KOUFU PTE LTD'},
                {'Date': '21 May 2025', 'Amount': 5.50, 'Description': '-7758 KOUFU PTE LTD'},
                {'Date': '22 May 2025', 'Amount': 55.00, 'Description': '-7758 CLOVER PLANT BASE'},
                {'Date': '23 May 2025', 'Amount': 2.00, 'Description': 'ACTIVESG SPORT SINGAPO'},
                {'Date': '16 May 2025', 'Amount': 2.50, 'Description': 'CCY CONVERSION FEE FOR: 250.00 SGD'}
            ]
        ),
    ])
    def test_extract_credit_card_transactions(self, mock_data, year, expected_transactions):
        transactions = extract_credit_card_transactions(mock_data, year)
        
        assert len(transactions) == len(expected_transactions)
        for actual, expected in zip(transactions, expected_transactions):
            assert actual == expected

    @pytest.mark.parametrize("mock_data, year, expected_transactions", [
        (
            {
                0: ['TRANSACTION DATE', '02/06', '02/06'],
                1: ['DESCRIPTION', '-5250 AIRBNB * HMNEHEPE 653-163-1004', 'CCY CONVERSION FEE'],
                2: ['', '', ''],
                3: ['', '', ''],
                4: ['AMOUNT', '200.17', '2.00'],
                5: ['', '', 'FOR: 200.17 SGD']
            },
            '2024',
            [
                {
                    'Date': '02 June 2024',
                    'Description': '-5250 AIRBNB * HMNEHEPE 653-163-1004',
                    'Amount': 200.17
                },
                {
                    'Date': '02 June 2024',
                    'Description': 'CCY CONVERSION FEE FOR: 200.17 SGD',
                    'Amount': 2.00
                }
            ]
        )
    ])
    def test_extract_credit_card_transactions_with_conversion_fee(self, mock_data, year, expected_transactions):
        mock_df = pd.DataFrame(mock_data)
        
        transactions = extract_credit_card_transactions([mock_df], year)
        
        assert len(transactions) == len(expected_transactions)
        for actual, expected in zip(transactions, expected_transactions):
            assert actual == expected

    @pytest.mark.parametrize("row, col_str, split_col_idx, split_columns_info, expected", [
        (
            pd.Series(['Date\nDate', 'Description', 'Cheque', 'Withdrawal', 'Deposit', 'Balance', '', '']),
            'Date\nDate',
            0,
            {0: ['Date', 'Date']},
            pd.Series(['Date', 'Date', 'Description', 'Cheque', 'Withdrawal', 'Deposit', 'Balance', '', ''])
        ),
        (
            pd.Series(['', 'Total Withdrawals/Deposits', '', '33,088.87', '11,955.86', '', '', '']),
            '',
            0,
            {0: ['Date', 'Date']},
            pd.Series(['', '', 'Total Withdrawals/Deposits', '', '33,088.87', '11,955.86', '', '', ''])
        ),
        (
            pd.Series(['', 'Total Interest Paid This Year', '', '', '133.61', '', '', '']),
            '',
            0,
            {0: ['Date', 'Date']},
            pd.Series(['', '', 'Total Interest Paid This Year', '', '', '133.61', '', '', ''])
        ),
        (
            pd.Series(['CHECK YOUR STATEMENT', '', '', '', '', '', '', '']),
            'CHECK YOUR STATEMENT',
            0,
            {0: ['Date', 'Date']},
            pd.Series(['', 'CHECK YOUR STATEMENT', '', '', '', '', '', '', ''])
        ),
        (
            pd.Series(['', 'Please check this statement & advise us of any discrepancies within 14 days of receipt. If we do not hear from you,', '', '', '', '', '', '']),
            '',
            0,
            {0: ['Date', 'Date']},
            pd.Series(['', '', 'Please check this statement & advise us of any discrepancies within 14 days of receipt. If we do not hear from you,', '', '', '', '', '', ''])
        ),
        (
            pd.Series(['', 'UPDATING YOUR PERSONAL PARTICULARS', '', '', '', '', '', '']),
            '',
            0,
            {0: ['Date', 'Date']},
            pd.Series(['', '', 'UPDATING YOUR PERSONAL PARTICULARS', '', '', '', '', '', ''])
        ),
        (
            pd.Series(['', '', '', '', '', 'RNB05ESNG\\2', 'Oversea-Chinese Banking Corporation Limited', 'Co. Reg. No.: 193200032W']),
            '',
            0,
            {0: ['Date', 'Date']},
            pd.Series(['', '', '', '', '', '', 'RNB05ESNG\\2', 'Oversea-Chinese Banking Corporation Limited', 'Co. Reg. No.: 193200032W'])
        ),
        (
            pd.Series(['Deposit Insurance Scheme', '', '', '', '', '', '', '']),
            'Deposit Insurance Scheme',
            0,
            {0: ['Date', 'Date']},
            pd.Series(['', 'Deposit Insurance Scheme', '', '', '', '', '', '', ''])
        ),
        (
            pd.Series(['', 'Singapore dollar deposits of non-bank depositors and monies and deposits denominated in Singapore dollars under the Supplementary Retirement Scheme are insured by the Singapore Deposit Insurance Corporation,', '', '', '', '', '', '']),
            '',
            0,
            {0: ['Date', 'Date']},
            pd.Series(['', '', 'Singapore dollar deposits of non-bank depositors and monies and deposits denominated in Singapore dollars under the Supplementary Retirement Scheme are insured by the Singapore Deposit Insurance Corporation,', '', '', '', '', '', ''])
        ),
        (
            pd.Series(['DATE\nDESCRIPTION', 'AMOUNT (S$)']),
            'DATE\nDESCRIPTION',
            0,
            {0: ['DATE', 'DESCRIPTION']},
            pd.Series(['DATE', 'DESCRIPTION', 'AMOUNT (S$)'])
        ),
        (
            pd.Series(['DBS LADIES VISA CARD NO.: 8339 2030 1234 0987', '']),
            'DBS LADIES VISA CARD NO.: 8339 2030 1234 0987',
            0,
            {0: ['DATE', 'DESCRIPTION']},
            pd.Series(['', 'DBS LADIES VISA CARD NO.: 8339 2030 1234 0987', ''])
        ),
        (
            pd.Series(['PREVIOUS BALANCE', '15,909.03']),
            'PREVIOUS BALANCE',
            0,
            {0: ['DATE', 'DESCRIPTION']},
            pd.Series(['', 'PREVIOUS BALANCE', '15,909.03'])
        ),
        (
            pd.Series(['20 MAY\nAUTO-PYT FROM ACCT#81717138592938', '15,909.03 CR']),
            '20 MAY\nAUTO-PYT FROM ACCT#81717138592938',
            0,
            {0: ['DATE', 'DESCRIPTION']},
            pd.Series(['20 MAY', 'AUTO-PYT FROM ACCT#81717138592938', '15,909.03 CR'])
        ),
        (
            pd.Series(['REF NO: 11689999398715999971650', '']),
            'REF NO: 11689999398715999971650',
            0,
            {0: ['DATE', 'DESCRIPTION']},
            pd.Series(['', 'REF NO: 11689999398715999971650', ''])
        ),
        (
            pd.Series(['NEW TRANSACTIONS YING CONG', '']),
            'NEW TRANSACTIONS YING CONG',
            0,
            {0: ['DATE', 'DESCRIPTION']},
            pd.Series(['', 'NEW TRANSACTIONS YING CONG', ''])
        ),
        (
            pd.Series(['23 APR\nCUSTOMER.IO EMAIL MARK HTTPSCUSTOMER OR', '140.85']),
            '23 APR\nCUSTOMER.IO EMAIL MARK HTTPSCUSTOMER OR',
            0,
            {0: ['DATE', 'DESCRIPTION']},
            pd.Series(['23 APR', 'CUSTOMER.IO EMAIL MARK HTTPSCUSTOMER OR', '140.85'])
        ),
        (
            pd.Series(['U. S. DOLLAR 100.00', '']),
            'U. S. DOLLAR 100.00',
            0,
            {0: ['DATE', 'DESCRIPTION']},
            pd.Series(['', 'U. S. DOLLAR 100.00', ''])
        ),
        (
            pd.Series(['01 MAY\nDIGITALOCEAN.COM       AMSTERDAM     NL', '614.86']),
            '01 MAY\nDIGITALOCEAN.COM       AMSTERDAM     NL',
            0,
            {0: ['DATE', 'DESCRIPTION']},
            pd.Series(['01 MAY', 'DIGITALOCEAN.COM       AMSTERDAM     NL', '614.86'])
        ),
        (
            pd.Series(['', 'SUB-TOTAL:\n755.71']),
            '',
            0,
            {0: ['DATE', 'DESCRIPTION']},
            pd.Series(['', '', 'SUB-TOTAL:\n755.71'])
        ),
        (
            pd.Series(['Any Full Amount due will be deducted from bank account 38592938. GIRO deduction date: 18 Jun 2024', '']),
            'Any Full Amount due will be deducted from bank account 38592938. GIRO deduction date: 18 Jun 2024',
            0,
            {0: ['DATE', 'DESCRIPTION']},
            pd.Series(['', 'Any Full Amount due will be deducted from bank account 38592938. GIRO deduction date: 18 Jun 2024', ''])
        ),
    ])
    def test_split_and_rebuild_row(self, row, col_str, split_col_idx, split_columns_info, expected):
        result = split_and_rebuild_row(row, col_str, split_col_idx, split_columns_info)
        pd.testing.assert_series_equal(result, expected)

    @pytest.mark.parametrize("input_data, expected_output, expected_is_transaction", [
        (
            pd.DataFrame({
                0: ['Credit Cards', 'Statement of Account', '', '(cid:2)(cid:1)(cid:3)(cid:2)(cid:4)(cid:3)(cid:1)(cid:4)(cid:2)(cid:3)(cid:2)(cid:1)(cid:4)(cid:1)(cid:4)(cid:1)(cid:4)(cid:3)(cid:1)(cid:2)(cid:4)(cid:3)(cid:2)(cid:1)(cid:4)(cid:1)(cid:4)(cid:4)(cid:1)(cid:4)', 'JOHN DOE', '123 MAIN STREET', 'CITY TOWERS', '#12-34', 'SINGAPORE 123456', '', '', 'STATEMENT DATE\nCREDIT LIMIT', '23 May 2024\n$100,000.00', '', '', '', '', '', 'This Statement serves as a TAX INVOICE if GST is charged.', 'DATE\nDESCRIPTION', 'DBS LADIES VISA CARD NO.: 1234 5678 9012 3456', 'PREVIOUS BALANCE', '20 MAY\nAUTO-PYT FROM ACCT#123456789012345', 'REF NO: 11689999398715999971650', 'NEW TRANSACTIONS JOHN DOE', '23 APR\nCUSTOMER.IO EMAIL MARK HTTPSCUSTOMER OR', 'U. S. DOLLAR 80.00', '01 MAY\nDIGITALOCEAN.COM       AMSTERDAM     NL', 'U. S. DOLLAR 350.00', '', '', 'Any Full Amount due will be deducted from bank account 123456789012345. GIRO deduction date: 18 Jun 2024', '', 'Please contact our Customer Service Officer immediately at 1800 111 1111 or 1800 732 8000 (for Platinum Customers), if you find any', '', 'correct.', 'DBS VISA/MASTERCARD/AMEX CARD - DBS POINTS SUMMARY (AS OF THIS STATEMENT)', 'CARD NUMBER\nBALANCE AS OF LAST\nEARNED/', 'STATEMENT\nADJUSTED', '1234 5678 9012 3456\n250,000\n750', 'TOTAL\n250,000\n750', 'This statement is for your information only.', ''],
                1: ['', '', '1 of 3', '', '', '', '', '', '', 'DBS Cards P.O. Box 360 S(912312)', 'Hotline: 1800 111 1111', 'MINIMUM PAYMENT\nPAYMENT DUE DATE', '$50.00\n18 Jun 2024', "Please settle this statement promptly. If minimum payment is not received by 'Payment Due Date', a late payment charge of $100 will", 'be  levied.  If  payment  is  not  made  in  full,  an  additional  finance  charge  of  27.80%  per  annum  will  be  levied  on  each  outstanding', 'balance  of  each  card  account  from  the  date  each  transaction  was  effected.  No  finance  charge  will  be  levied  on  new  transactions', '(except Cash Advance transactions) effected after this statement date. Please refer to the last page of statement for more details.', 'Co. Reg. No. 196800306E', 'GST Registration No: MR-8500180-3', 'AMOUNT (S$)', '', '12,500.00', '12,500.00 CR', '', '', '112.00', '', '490.00', '', 'SUB-TOTAL:\n602.00', 'TOTAL:\n602.00', '', 'GRAND TOTAL FOR ALL CARD ACCOUNTS:\n602.00', '', 'discrepancies on your statement. If no discrepancy is reported within 7 days upon receipt of statement, your statement will be considered', '', '', 'REDEEMED/\nBALANCE\nEXPIRING ON', 'EXPIRED\n30 JUN 2024', '0\n250,750\nNo Expiry', '0\n250,750\n0', '', 'PDS_CRCRDGCE_LOC_ESTMT_0d67011d0000003c_07973'],
            }),
            pd.DataFrame({
                0: ['Credit Cards', 'Statement of Account', '', '(cid:2)(cid:1)(cid:3)(cid:2)(cid:4)(cid:3)(cid:1)(cid:4)(cid:2)(cid:3)(cid:2)(cid:1)(cid:4)(cid:1)(cid:4)(cid:1)(cid:4)(cid:3)(cid:1)(cid:2)(cid:4)(cid:3)(cid:2)(cid:1)(cid:4)(cid:1)(cid:4)(cid:4)(cid:1)(cid:4)', 'JOHN DOE', '123 MAIN STREET', 'CITY TOWERS', '#12-34', 'SINGAPORE 123456', '', '', 'STATEMENT DATE\nCREDIT LIMIT', '23 May 2024\n$100,000.00', '', '', '', '', '', 'This Statement serves as a TAX INVOICE if GST is charged.', 'DATE', '', '', '20 MAY', '', '', '23 APR', '', '01 MAY', '', '', '', '', '', '', '', '', '', '', 'STATEMENT', '', '', '', ''],
                1: ['', '', '1 of 3', '', '', '', '', '', '', 'DBS Cards P.O. Box 360 S(912312)', 'Hotline: 1800 111 1111', 'MINIMUM PAYMENT\nPAYMENT DUE DATE', '$50.00\n18 Jun 2024', "Please settle this statement promptly. If minimum payment is not received by 'Payment Due Date', a late payment charge of $100 will", 'be  levied.  If  payment  is  not  made  in  full,  an  additional  finance  charge  of  27.80%  per  annum  will  be  levied  on  each  outstanding', 'balance  of  each  card  account  from  the  date  each  transaction  was  effected.  No  finance  charge  will  be  levied  on  new  transactions', '(except Cash Advance transactions) effected after this statement date. Please refer to the last page of statement for more details.', 'Co. Reg. No. 196800306E', 'GST Registration No: MR-8500180-3', 'DESCRIPTION', 'DBS LADIES VISA CARD NO.: 1234 5678 9012 3456', 'PREVIOUS BALANCE', 'AUTO-PYT FROM ACCT#123456789012345', 'REF NO: 11689999398715999971650', 'NEW TRANSACTIONS JOHN DOE', 'CUSTOMER.IO EMAIL MARK HTTPSCUSTOMER OR', 'U. S. DOLLAR 80.00', 'DIGITALOCEAN.COM       AMSTERDAM     NL', 'U. S. DOLLAR 350.00', '', '', 'Any Full Amount due will be deducted from bank account 123456789012345. GIRO deduction date: 18 Jun 2024', '', 'Please contact our Customer Service Officer immediately at 1800 111 1111 or 1800 732 8000 (for Platinum Customers), if you find any', '', 'correct.', 'DBS VISA/MASTERCARD/AMEX CARD - DBS POINTS SUMMARY (AS OF THIS STATEMENT)', '', 'ADJUSTED', '', '', 'This statement is for your information only.', ''],
                2: [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, 'AMOUNT (S$)', '', '12,500.00', '12,500.00 CR', '', '', '112.00', '', '490.00', '', 'SUB-TOTAL:\n602.00', 'TOTAL:\n602.00', '', 'GRAND TOTAL FOR ALL CARD ACCOUNTS:\n602.00', '', 'discrepancies on your statement. If no discrepancy is reported within 7 days upon receipt of statement, your statement will be considered', '', '', 'REDEEMED/\nBALANCE\nEXPIRING ON', 'EXPIRED\n30 JUN 2024', '0\n250,750\nNo Expiry', '0\n250,750\n0', '', 'PDS_CRCRDGCE_LOC_ESTMT_0d67011d0000003c_07973'],
            }),
            True
        ),
        (
            pd.DataFrame({
                0: ['180 ACCOUNT', 'Account No. 123456789001', 'Transaction', 'Date', '', '01 JUL', '', '', '', '03 JUL', '', '', '', '07 JUL', '', '', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '15 JUL', '', '', '', '18 JUL', '', '', '29 JUL', '', '', '', '30 JUL', '', '', '', '01 AUG', ''],
                1: ['', '', 'Value', 'Date', '', '01 JUL', '', '', '', '03 JUL', '', '', '', '08 JUL', '', '', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '15 JUL', '', '', '', '18 JUL', '', '', '29 JUL', '', '', '', '30 JUL', '', '', '', '31 JUL', ''],
                2: ['', '', '', 'Description', 'BALANCE B/F', 'FAST PAYMENT', '987654321', 'to Jane Smith', 'OTHR - Other', 'FAST PAYMENT', 'via PayNow-Mobile', 'to Alice', 'OTHR - OTHR', 'FAST PAYMENT', '123456789', 'to John Doe', 'OTHR - Other', 'BONUS INTEREST', '360 SALARY BONUS', 'BONUS INTEREST', '360 CC SPEND BONUS', 'BONUS INTEREST', '360 SAVE BONUS', 'BONUS INTEREST', '360 GROW BONUS', 'POS PURCHASE    NETS', 'UNITED OVE', 'UNITED OVERSEAS BANK', 'AIRPORT', 'GIRO', 'COLL 100100183987', 'CR CARD RECEIVABLE', 'GIRO - SALARY', 'SALA', 'ABC Company Pte Ltd', 'ABC Company Pte Ltd', 'FUND TRANSFER', 'via PayNow-QR Code', 'to ROBERT TAN', 'OTHR - OTHR', 'INTEREST CREDIT', 'BALANCE C/F'],
                3: ['', '', '', 'Cheque', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                4: ['', '', '', 'Withdrawal', '', '650.00', '', '', '', '25.00', '', '', '', '25,000.00', '', '', '', '', '', '', '', '', '', '', '', '180.50', '', '', '', '2,000.00', '', '', '', '', '', '', '5.00', '', '', '', '', ''],
                5: ['', '', '', 'Deposit', '', '', '', '', '', '', '', '', '', '', '', '', '', '180.00', '', '45.00', '', '110.00', '', '175.00', '', '', '', '', '', '', '', '', '10,000.00', '', '', '', '', '', '', '', '20.00', ''],
                6: ['1  JUL 2024 TO 31 JUL 2024', '', '', 'Balance', '500,000.00', '499,350.00', '', '', '', '499,325.00', '', '', '', '474,325.00', '', '', '', '474,505.00', '', '474,550.00', '', '474,660.00', '', '474,835.00', '', '474,654.50', '', '', '', '472,654.50', '', '', '482,654.50', '', '', '', '482,649.50', '', '', '', '482,669.50', '482,669.50'],
                7: ['', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 'Oversea-Chinese Banking Corporation Limited'],
            }),
            pd.DataFrame({
                0: ['180 ACCOUNT', 'Account No. 123456789001', 'Transaction', 'Date', '', '01 JUL', '', '', '', '03 JUL', '', '', '', '07 JUL', '', '', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '15 JUL', '', '', '', '18 JUL', '', '', '29 JUL', '', '', '', '30 JUL', '', '', '', '01 AUG', ''],
                1: ['', '', 'Value', 'Date', '', '01 JUL', '', '', '', '03 JUL', '', '', '', '08 JUL', '', '', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '15 JUL', '', '', '', '18 JUL', '', '', '29 JUL', '', '', '', '30 JUL', '', '', '', '31 JUL', ''],
                2: ['', '', '', 'Description', 'BALANCE B/F', 'FAST PAYMENT', '987654321', 'to Jane Smith', 'OTHR - Other', 'FAST PAYMENT', 'via PayNow-Mobile', 'to Alice', 'OTHR - OTHR', 'FAST PAYMENT', '123456789', 'to John Doe', 'OTHR - Other', 'BONUS INTEREST', '360 SALARY BONUS', 'BONUS INTEREST', '360 CC SPEND BONUS', 'BONUS INTEREST', '360 SAVE BONUS', 'BONUS INTEREST', '360 GROW BONUS', 'POS PURCHASE    NETS', 'UNITED OVE', 'UNITED OVERSEAS BANK', 'AIRPORT', 'GIRO', 'COLL 100100183987', 'CR CARD RECEIVABLE', 'GIRO - SALARY', 'SALA', 'ABC Company Pte Ltd', 'ABC Company Pte Ltd', 'FUND TRANSFER', 'via PayNow-QR Code', 'to ROBERT TAN', 'OTHR - OTHR', 'INTEREST CREDIT', 'BALANCE C/F'],
                3: ['', '', '', 'Cheque', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                4: ['', '', '', 'Withdrawal', '', '650.00', '', '', '', '25.00', '', '', '', '25,000.00', '', '', '', '', '', '', '', '', '', '', '', '180.50', '', '', '', '2,000.00', '', '', '', '', '', '', '5.00', '', '', '', '', ''],
                5: ['', '', '', 'Deposit', '', '', '', '', '', '', '', '', '', '', '', '', '', '180.00', '', '45.00', '', '110.00', '', '175.00', '', '', '', '', '', '', '', '', '10,000.00', '', '', '', '', '', '', '', '20.00', ''],
                6: ['1  JUL 2024 TO 31 JUL 2024', '', '', 'Balance', '500,000.00', '499,350.00', '', '', '', '499,325.00', '', '', '', '474,325.00', '', '', '', '474,505.00', '', '474,550.00', '', '474,660.00', '', '474,835.00', '', '474,654.50', '', '', '', '472,654.50', '', '', '482,654.50', '', '', '', '482,649.50', '', '', '', '482,669.50', '482,669.50'],
                7: ['', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 'Oversea-Chinese Banking Corporation Limited'],
            }),
            True
        ),
        (
            pd.DataFrame({
                0: ['TRANSACTION CODE', 'A/C', 'A/C CLOSED', 'ATM O', 'ATM PAYMENT', 'ATM TRANSFER', 'ACU', 'ADJ', 'ADV', 'AMD', '', 'ASI', 'BANK CHARGES', 'BAL', 'C/Order', 'CHQ DP', 'CC', 'CDP', 'COM', 'CONV', 'CR', 'CRVISA', '', 'CANC TT', 'DIV', '', 'DR', '', 'DD', '', 'DVE', '', 'DDB', '', 'DCR', '', 'EPS', '', 'ES', '', 'ES FIXED FEE', '', 'ESA', '', 'EXPORT LOAN', '', 'FCY', '', 'HSE LOAN', '', 'IMPORT LOAN', 'Contact for Consumer Banking:', 'Phone Bank:', 'Email Addresss:', 'APPLICATIONS FOR INDIVIDUALS', ''],
                1: ['DESCRIPTION', 'Account', 'Account Closed', 'ATM Overseas', 'Automated Teller Machine Payment', 'Automated Teller Machine Transfer', 'Foreign Currency', 'Adjustment', 'Advance', 'Amendment', '', 'Automated Standing Instruction', 'Bank Charges', 'Balance', "Cashier's Order", 'Cheque Deposit', 'Cash Card', 'Central Depository', 'Commission', 'Conversion', 'Credit', 'VISA Electron Credit', '', 'Telegraphic Transfer Cancellation', 'Dividend', '', 'Debit', '', 'Demand Draft', '', 'VISA Electron Purchases', '', 'Direct Debit', '', 'Direct Credit', '', 'Electronic Payment of Shares', '', 'EasiSave', '', 'EasiSave Fixed Fee', '', 'Electronic Share Application', '', 'Export Loan', '', 'Foreign Currency', '', 'House Loan', '', 'Import Loan', '', '1800 363 3333', 'contactus@ocbc.com', '', ''],
                2: ['', '账户', '关闭账户', '海外自动柜员机', '自动柜员机付款', '自动柜员机转账', '外币', '调整', '预付款', '修改', '自动长期指示', '', '银行费用', '余款', '本票', '支票存入', '现金卡', '中央托收私人有限公司', '佣金', '外币兌換', '货方/存货', '维萨电子付款', '电子汇款撤消', '', '股息', '借方/付出', '', '即期汇票', '', '维萨电子', '', '直接财路付款', '', '直接财路存款', '', '电子购股付款', '', '易式储蓄户口', '', '易式账户固定费用', '', '电子股票认购', '', '出口贷款', '', '外币', '', '房屋贷款', '', '进口贷款', '', '', '', '', '', 'ATM Card'],
                3: ['', 'INT', 'I/COLLN', 'IBG', 'INB', 'INB TRANSFER', 'INS', 'L/C', 'L CHQ', 'MER', '', 'NEGN', 'NETS', 'NOM', 'O/COLLN', 'PAYMENT-MAS', 'PER LOAN', 'PHN TRANSFER', 'POS', 'POST', 'PREM', 'PS', '', 'REIMB', 'RTN GIRO', '', 'RTN CHQ', '', 'RECEIPTS-MAS', '', 'SEC', '', 'SGD', '', 'SATM', '', 'ST', '', 'SER CHARGE', '', 'SHR', '', 'T/R', '', 'TT', '', 'TRAN CHARGE', '', 'TRADE FINANCE', '', 'TRANSFER', 'Contact for Business Banking:', 'Commercial Service Centre:', 'Email Address:', '', ''],
                4: ['', 'Interest', 'Inward Collection', 'Inter-Bank GIRO', 'Internet Banking', 'Internet Banking Transfer', 'Insurance', 'Letter of Credit', 'Late Cheque', 'Merchant', '', 'Negotiation', 'NETS', 'Nominee', 'Outward Collection', 'MEPS Payment', 'Personal Loan', 'PhoneBank Transfer', 'Point of Sale', 'Postage', 'Premium', 'Power Supply', '', 'Reimbursement', 'Return GIRO', '', 'Return Cheque', '', 'MEPS Receipts', '', 'Security', '', 'Singapore Dollar', '', 'Shared ATM', '', 'SingTel', '', 'Service Charge', '', 'Share NETS', '', 'Trust Receipt', '', 'Telegraphic Transfer', '', 'Transactoin Charge', '', 'Trade Finance', '', 'Transfer', '', '6538 1111', 'Bizinteract@ocbc.com', '', 'PHONE/INTERNET/MOBILE BANKING'],
                5: ['', '利息', '进口托收', '财路', '网络银行', '网络银行转账', '保险', '信用证', '逾期支票', '信用卡特约商家', '信用证议付', '', '电子转账网络系统', '信托人', '出口托收', '货币管理(MAS)电子付款系统-付款', '个人贷款', '电话银行转账', '销售终端', '邮费', '保险费', '能源供应公司', '偿付', '', '退回财路', '退回支票', '', '货币管理局电子付款系统-存入', '', '证券', '', '新币', '', '供享自动柜员机', '', '新电信', '', '服务费', '', '供享电子转账网络系统', '', '信托收据', '', '电子付款', '', '交易费用', '', '货易融资', '', '转账', '', '', '', '', '', ''],
            }),
            pd.DataFrame({
                0: ['TRANSACTION CODE', 'A/C', 'A/C CLOSED', 'ATM O', 'ATM PAYMENT', 'ATM TRANSFER', 'ACU', 'ADJ', 'ADV', 'AMD', '', 'ASI', 'BANK CHARGES', 'BAL', 'C/Order', 'CHQ DP', 'CC', 'CDP', 'COM', 'CONV', 'CR', 'CRVISA', '', 'CANC TT', 'DIV', '', 'DR', '', 'DD', '', 'DVE', '', 'DDB', '', 'DCR', '', 'EPS', '', 'ES', '', 'ES FIXED FEE', '', 'ESA', '', 'EXPORT LOAN', '', 'FCY', '', 'HSE LOAN', '', 'IMPORT LOAN', 'Contact for Consumer Banking:', 'Phone Bank:', 'Email Addresss:', 'APPLICATIONS FOR INDIVIDUALS', ''],
                1: ['DESCRIPTION', 'Account', 'Account Closed', 'ATM Overseas', 'Automated Teller Machine Payment', 'Automated Teller Machine Transfer', 'Foreign Currency', 'Adjustment', 'Advance', 'Amendment', '', 'Automated Standing Instruction', 'Bank Charges', 'Balance', "Cashier's Order", 'Cheque Deposit', 'Cash Card', 'Central Depository', 'Commission', 'Conversion', 'Credit', 'VISA Electron Credit', '', 'Telegraphic Transfer Cancellation', 'Dividend', '', 'Debit', '', 'Demand Draft', '', 'VISA Electron Purchases', '', 'Direct Debit', '', 'Direct Credit', '', 'Electronic Payment of Shares', '', 'EasiSave', '', 'EasiSave Fixed Fee', '', 'Electronic Share Application', '', 'Export Loan', '', 'Foreign Currency', '', 'House Loan', '', 'Import Loan', '', '1800 363 3333', 'contactus@ocbc.com', '', ''],
                2: ['', '账户', '关闭账户', '海外自动柜员机', '自动柜员机付款', '自动柜员机转账', '外币', '调整', '预付款', '修改', '自动长期指示', '', '银行费用', '余款', '本票', '支票存入', '现金卡', '中央托收私人有限公司', '佣金', '外币兌換', '货方/存货', '维萨电子付款', '电子汇款撤消', '', '股息', '借方/付出', '', '即期汇票', '', '维萨电子', '', '直接财路付款', '', '直接财路存款', '', '电子购股付款', '', '易式储蓄户口', '', '易式账户固定费用', '', '电子股票认购', '', '出口贷款', '', '外币', '', '房屋贷款', '', '进口贷款', '', '', '', '', '', 'ATM Card'],
                3: ['', 'INT', 'I/COLLN', 'IBG', 'INB', 'INB TRANSFER', 'INS', 'L/C', 'L CHQ', 'MER', '', 'NEGN', 'NETS', 'NOM', 'O/COLLN', 'PAYMENT-MAS', 'PER LOAN', 'PHN TRANSFER', 'POS', 'POST', 'PREM', 'PS', '', 'REIMB', 'RTN GIRO', '', 'RTN CHQ', '', 'RECEIPTS-MAS', '', 'SEC', '', 'SGD', '', 'SATM', '', 'ST', '', 'SER CHARGE', '', 'SHR', '', 'T/R', '', 'TT', '', 'TRAN CHARGE', '', 'TRADE FINANCE', '', 'TRANSFER', 'Contact for Business Banking:', 'Commercial Service Centre:', 'Email Address:', '', ''],
                4: ['', 'Interest', 'Inward Collection', 'Inter-Bank GIRO', 'Internet Banking', 'Internet Banking Transfer', 'Insurance', 'Letter of Credit', 'Late Cheque', 'Merchant', '', 'Negotiation', 'NETS', 'Nominee', 'Outward Collection', 'MEPS Payment', 'Personal Loan', 'PhoneBank Transfer', 'Point of Sale', 'Postage', 'Premium', 'Power Supply', '', 'Reimbursement', 'Return GIRO', '', 'Return Cheque', '', 'MEPS Receipts', '', 'Security', '', 'Singapore Dollar', '', 'Shared ATM', '', 'SingTel', '', 'Service Charge', '', 'Share NETS', '', 'Trust Receipt', '', 'Telegraphic Transfer', '', 'Transactoin Charge', '', 'Trade Finance', '', 'Transfer', '', '6538 1111', 'Bizinteract@ocbc.com', '', 'PHONE/INTERNET/MOBILE BANKING'],
                5: ['', '利息', '进口托收', '财路', '网络银行', '网络银行转账', '保险', '信用证', '逾期支票', '信用卡特约商家', '信用证议付', '', '电子转账网络系统', '信托人', '出口托收', '货币管理(MAS)电子付款系统-付款', '个人贷款', '电话银行转账', '销售终端', '邮费', '保险费', '能源供应公司', '偿付', '', '退回财路', '退回支票', '', '货币管理局电子付款系统-存入', '', '证券', '', '新币', '', '供享自动柜员机', '', '新电信', '', '服务费', '', '供享电子转账网络系统', '', '信托收据', '', '电子付款', '', '交易费用', '', '货易融资', '', '转账', '', '', '', '', '', ''],
            }),
            False
        ),
        (
            pd.DataFrame({
                0: ['OCBC 180 CARD', 'JOHN DOE', '', '17/05', '24/04', '24/04', '25/04', '25/04', '26/04', '26/04', '26/04', '28/04', '29/04', '30/04', '30/04', '30/04', '01/05', '01/05', '01/05', '01/05', '02/05', '02/05', '02/05', '02/05', '03/05', '03/05', '03/05', '06/05', '06/05', '06/05', '07/05', '07/05'],
                1: ['', '1234-5678-9012-3456', "LAST MONTH'S BALANCE", 'PAYMENT BY GIRO', 'AMAZE* GRAB A-6AR2I', 'AMAZE* HUMVENTURES', 'AMAZE* GRAB A-6AT77', 'AMAZE* GRAB A-6AV28', 'AMAZE* GRAB A-6BXUJ', 'AMAZE* GRAB A-6BXUJ', '-7758 BUS/MRT 429799071', '-7758 NTUC FP-BT PANJANG', '-7758 LILAC OAK', '-7758 KOUFU PTE LTD', 'AMAZE* CTCPPT BDS V', '-4887 BUS/MRT 431965453', '-7758 HELLORIDE', '-4887 ATLAS COFFEEHOUSE', '-4887 AN ACAI AFFAIR HI', '-7758 BUS/MRT 432407001', '-7758 SUPER SIMPLE ONE', 'GOJEK', '-5831 CIRCLES.LIFE', '-7758 TAKAGI RAMEN-CONN', '-7758 KOUFU PTE LTD', '-7758 KOUFU PTE LTD', '-7758 HELLORIDE', '-4887 7 ELEVEN-ONE NORT', '-4887 KOUFU PTE LTD', '-4887 BUS/MRT 433911127', '-4887 BANGKOK JAM - PLA', '-7758 BUS/MRT 435033935'],
                2: ['', '', '', '', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', '31353135', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE'],
                3: ['', '', '', '', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SG', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP'],
                4: ['', '', '809.58', '(754.15', '1.81', '62.25', '1.94', '2.33', '11.46', '0.51', '15.47', '2.55', '53.15', '7.20', '(55.43', '5.64', '1.00', '8.18', '10.20', '9.26', '13.52', '26.60', '34.80', '7.95', '5.00', '6.20', '3.00', '1.00', '8.60', '19.40', '52.40', '16.54'],
                5: ['', '', '', ')', '', '', '', '', '', '', '', '', '', '', ')', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
            }),
            pd.DataFrame({
                0: ['OCBC 180 CARD', 'JOHN DOE', '', '17/05', '24/04', '24/04', '25/04', '25/04', '26/04', '26/04', '26/04', '28/04', '29/04', '30/04', '30/04', '30/04', '01/05', '01/05', '01/05', '01/05', '02/05', '02/05', '02/05', '02/05', '03/05', '03/05', '03/05', '06/05', '06/05', '06/05', '07/05', '07/05'],
                1: ['', '1234-5678-9012-3456', "LAST MONTH'S BALANCE", 'PAYMENT BY GIRO', 'AMAZE* GRAB A-6AR2I', 'AMAZE* HUMVENTURES', 'AMAZE* GRAB A-6AT77', 'AMAZE* GRAB A-6AV28', 'AMAZE* GRAB A-6BXUJ', 'AMAZE* GRAB A-6BXUJ', '-7758 BUS/MRT 429799071', '-7758 NTUC FP-BT PANJANG', '-7758 LILAC OAK', '-7758 KOUFU PTE LTD', 'AMAZE* CTCPPT BDS V', '-4887 BUS/MRT 431965453', '-7758 HELLORIDE', '-4887 ATLAS COFFEEHOUSE', '-4887 AN ACAI AFFAIR HI', '-7758 BUS/MRT 432407001', '-7758 SUPER SIMPLE ONE', 'GOJEK', '-5831 CIRCLES.LIFE', '-7758 TAKAGI RAMEN-CONN', '-7758 KOUFU PTE LTD', '-7758 KOUFU PTE LTD', '-7758 HELLORIDE', '-4887 7 ELEVEN-ONE NORT', '-4887 KOUFU PTE LTD', '-4887 BUS/MRT 433911127', '-4887 BANGKOK JAM - PLA', '-7758 BUS/MRT 435033935'],
                2: ['', '', '', '', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', '31353135', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE'],
                3: ['', '', '', '', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SG', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP'],
                4: ['', '', '809.58', '(754.15', '1.81', '62.25', '1.94', '2.33', '11.46', '0.51', '15.47', '2.55', '53.15', '7.20', '(55.43', '5.64', '1.00', '8.18', '10.20', '9.26', '13.52', '26.60', '34.80', '7.95', '5.00', '6.20', '3.00', '1.00', '8.60', '19.40', '52.40', '16.54'],
                5: ['', '', '', ')', '', '', '', '', '', '', '', '', '', '', ')', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
            }),
            True
        ),
        (
            pd.DataFrame({
                0: ['', 'it will be brought forward to the previous working day.', '', '4. ANNUAL FEE AND CARD REPLACEMENT FEE', '•    Unless we receive your instruction otherwise, your OCBC Credit Card', 'will be renewed automatically upon each anniversary of your', 'membership.', 'The annual fee for various types of Cards and the card replacement fee', 'are shown in the table below:', '', 'Annual Fee#', '', '(Inclusive of GST)', '', '', 'Type of Card', '', 'Principal\nSupplementary', 'Card\nCard', 'OCBC Premier Visa Infinite Credit Card\nFREE\nFREE', 'OCBC Elite World Card\nS$261.60\nFREE', '', 'OCBC 365 Credit Card', '', 'OCBC 90.N Visa / Mastercard', '', 'OCBC Titanium\nS$196.20\nS$98.10', '', 'OCBC Rewards Card', 'OCBC INFINITY Cashback Card', '', 'OCBC Arts Platinum MasterCard', '', 'OCBC NXT Credit Card', 'S$163.50\nS$81.75', 'OCBC Platinum MasterCard', 'OCBC MasterCard Gold / Visa Gold', 'FRANK Credit Card\nS$196.20\nS$98.10', 'OCBC MasterCard Standard / Visa', 'S$32.70\nS$32.70', 'Classic', 'OCBC Great Eastern Cashflo', 'S$163.50\nS$81.75', 'MasterCard', 'OCBC BEST Denki Platinum MasterCard\nS$163.50\nS$81.75', 'OCBC Visa Credit Card\nNA\nNA', '#Inclusive of GST'],
                1: ['', '', '', '', '', '', '', '', '', '', 'Minimum Spend', '', 'Requirement', '', 'for Annual Fee', '', 'Auto Waiver', '', '', 'NA', 'S$50,000', '', '', '', '', '', '', '', '', 'S$10,000', '', '', '', '', '', '', '', 'S$10,000', '', 'S$10,000', '', '', 'S$5,000', '', 'S$5,000', 'NA', ''],
                2: ['', '', '', '', '', '', '', '', '', '', '', 'Card', '', 'Replacement', '', 'Fee#', '', '', '', '', '', '', '', '', '', '', '', '', 'S$27.25', '', '', '', '', '', '', '', '', 'S$32.70', '', '', '', '', 'S$27.25', '', '', '', ''],
                3: ['This will be calculated on a daily basis from the Transaction Date until', 'full payment is received by OCBC bank.', '•    Retrieval Fees – requests for copies of sales drafts and statements', 'are subject to the following charges (inclusive of GST):', '', 'Sales Draft', 'Copy \n \n \n \n \n \nS$15 (per copy)', 'Original \n \n \n \n \n \nS$25 (per copy)', '', 'Statement', 'Current to 2 months \n \n \n \nFREE', '', '3 to 12 months   \n \n \n \nS$6 \n (per statement)', '', 'More than 12 months \n \n \nS$33 (per statement)', '', '', '•    Administrative Charges', 'Credit Refund via Cashier’s Order or \n \nS$5', 'funds transfer from account to account', 'within OCBC Bank', 'Card Conversion \n \n \n \n \n \nS$20', '', 'Returned Cheque \n \n \n \n \n \nS$30', '', 'Returned Interbank GIRO \n \n \n \nS$30', '', 'Cancellation of 0% Interest Instalment Plan \nS$150', '', '', '7. If you require assistance or information, contact us at:', '', '•    Our Customer Service Hotline: 1800 363 3333 or (65) 6363 3333 when', 'overseas.', '', '•    OCBC Online Banking', '', '', '', '', '', '', '', '', '', '', ''],
            }),
            pd.DataFrame({
                0: ['', 'it will be brought forward to the previous working day.', '', '4. ANNUAL FEE AND CARD REPLACEMENT FEE', '•    Unless we receive your instruction otherwise, your OCBC Credit Card', 'will be renewed automatically upon each anniversary of your', 'membership.', 'The annual fee for various types of Cards and the card replacement fee', 'are shown in the table below:', '', 'Annual Fee#', '', '(Inclusive of GST)', '', '', 'Type of Card', '', 'Principal\nSupplementary', 'Card\nCard', 'OCBC Premier Visa Infinite Credit Card\nFREE\nFREE', 'OCBC Elite World Card\nS$261.60\nFREE', '', 'OCBC 365 Credit Card', '', 'OCBC 90.N Visa / Mastercard', '', 'OCBC Titanium\nS$196.20\nS$98.10', '', 'OCBC Rewards Card', 'OCBC INFINITY Cashback Card', '', 'OCBC Arts Platinum MasterCard', '', 'OCBC NXT Credit Card', 'S$163.50\nS$81.75', 'OCBC Platinum MasterCard', 'OCBC MasterCard Gold / Visa Gold', 'FRANK Credit Card\nS$196.20\nS$98.10', 'OCBC MasterCard Standard / Visa', 'S$32.70\nS$32.70', 'Classic', 'OCBC Great Eastern Cashflo', 'S$163.50\nS$81.75', 'MasterCard', 'OCBC BEST Denki Platinum MasterCard\nS$163.50\nS$81.75', 'OCBC Visa Credit Card\nNA\nNA', '#Inclusive of GST'],
                1: ['', '', '', '', '', '', '', '', '', '', 'Minimum Spend', '', 'Requirement', '', 'for Annual Fee', '', 'Auto Waiver', '', '', 'NA', 'S$50,000', '', '', '', '', '', '', '', '', 'S$10,000', '', '', '', '', '', '', '', 'S$10,000', '', 'S$10,000', '', '', 'S$5,000', '', 'S$5,000', 'NA', ''],
                2: ['', '', '', '', '', '', '', '', '', '', '', 'Card', '', 'Replacement', '', 'Fee#', '', '', '', '', '', '', '', '', '', '', '', '', 'S$27.25', '', '', '', '', '', '', '', '', 'S$32.70', '', '', '', '', 'S$27.25', '', '', '', ''],
                3: ['This will be calculated on a daily basis from the Transaction Date until', 'full payment is received by OCBC bank.', '•    Retrieval Fees – requests for copies of sales drafts and statements', 'are subject to the following charges (inclusive of GST):', '', 'Sales Draft', 'Copy \n \n \n \n \n \nS$15 (per copy)', 'Original \n \n \n \n \n \nS$25 (per copy)', '', 'Statement', 'Current to 2 months \n \n \n \nFREE', '', '3 to 12 months   \n \n \n \nS$6 \n (per statement)', '', 'More than 12 months \n \n \nS$33 (per statement)', '', '', '•    Administrative Charges', 'Credit Refund via Cashier’s Order or \n \nS$5', 'funds transfer from account to account', 'within OCBC Bank', 'Card Conversion \n \n \n \n \n \nS$20', '', 'Returned Cheque \n \n \n \n \n \nS$30', '', 'Returned Interbank GIRO \n \n \n \nS$30', '', 'Cancellation of 0% Interest Instalment Plan \nS$150', '', '', '7. If you require assistance or information, contact us at:', '', '•    Our Customer Service Hotline: 1800 363 3333 or (65) 6363 3333 when', 'overseas.', '', '•    OCBC Online Banking', '', '', '', '', '', '', '', '', '', '', ''],
            }),
            False
        ),
        (
            pd.DataFrame({
                0: ['TRANSACTION DATE', '08/05', '10/05', '11/05', '11/05', '11/05', '11/05', '11/05', '11/05', '11/05', '12/05', '13/05', '13/05', '13/05', '13/05', '14/05', '15/05', '15/05', '16/05', '16/05', '16/05', '16/05', '17/05', '18/05', '18/05', '18/05', '18/05', '21/05', '21/05', '', '21/05', '21/05', '22/05', '23/05', '16/05', '', '', '', ''],
                1: ['DESCRIPTION', '-7758 SUPER SIMPLE ONE', '-7758 KOUFU PTE LTD', 'AMAZE* CSPRIMER.COM', 'AMAZE* GRAB A-6DX6H', '-7758 BOTANICT', '-7758 YA KUN BT PANJANG', '-7758 STARBUCKS@BKT PJG', '-7758 FRUITS VENDING PT', '-7758 BUS/MRT 437694781', 'GOOGLE STORAGE', 'SISTIC MBS', '-4887 KOUFU PTE LTD', '-4887 KOUFU PTE LTD', '-4887 BUS/MRT 438819449', '-7758 SUPER SIMPLE ONE', '-7758 SUBWAY - ONE NORTH', '-4887 MR BEAN INT L-FUS', 'TRAVELOKA*1144026038     SINGAPORE    SG', '-7758 MR BEAN INT L-FUS', '-5250 AIRBNB * HMX283ZD', '-7758 BUS/MRT 440276407', '-7758 KOUFU PTE LTD', '-7758 HELLORIDE', '-7758 SUZUKI COFFEE', '-7758 HELLORIDE', '-7758 GET*GET*MOTHER EA', '-7758 WWW.HEARTBREAKMEL', '-0315 OPENAI *CHATGPT S', 'FOREIGN CURRENCY USD 20.00', '-7758 KOUFU PTE LTD', '-7758 KOUFU PTE LTD', '-7758 CLOVER PLANT BASE', 'ACTIVESG SPORT SINGAPO', 'CCY CONVERSION FEE', 'FOR: 246.68 SGD', 'SUBTOTAL', 'TOTAL', 'TOTAL AMOUNT DUE'],
                2: ['', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', '6564162232', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', '', 'SINGAPORE', '653-163-1004', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SAN FRANCISCO', '', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', '', '', '', '', ''],
                3: ['', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SG', 'SGP', '', 'SGP', 'GBR', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'USA', '', 'SGP', 'SGP', 'SGP', 'SGP', '', '', '', '', ''],
                4: ['AMOUNT', '16.24', '7.20', '53.92', '21.40', '41.70', '3.00', '7.60', '2.00', '12.81', '2.79', '368.00', '6.20', '5.00', '14.04', '13.52', '6.50', '5.70', '232.28', '4.50', '246.68', '11.92', '5.50', '1.00', '9.00', '1.00', '24.90', '11.00', '27.84', '', '8.40', '5.00', '54.19', '1.80', '2.47', '', '1,623.06', '1,623.06', '1,623.06'],
            }),
            pd.DataFrame({
                0: ['TRANSACTION DATE', '08/05', '10/05', '11/05', '11/05', '11/05', '11/05', '11/05', '11/05', '11/05', '12/05', '13/05', '13/05', '13/05', '13/05', '14/05', '15/05', '15/05', '16/05', '16/05', '16/05', '16/05', '17/05', '18/05', '18/05', '18/05', '18/05', '21/05', '21/05', '', '21/05', '21/05', '22/05', '23/05', '16/05', '', '', '', ''],
                1: ['DESCRIPTION', '-7758 SUPER SIMPLE ONE', '-7758 KOUFU PTE LTD', 'AMAZE* CSPRIMER.COM', 'AMAZE* GRAB A-6DX6H', '-7758 BOTANICT', '-7758 YA KUN BT PANJANG', '-7758 STARBUCKS@BKT PJG', '-7758 FRUITS VENDING PT', '-7758 BUS/MRT 437694781', 'GOOGLE STORAGE', 'SISTIC MBS', '-4887 KOUFU PTE LTD', '-4887 KOUFU PTE LTD', '-4887 BUS/MRT 438819449', '-7758 SUPER SIMPLE ONE', '-7758 SUBWAY - ONE NORTH', '-4887 MR BEAN INT L-FUS', 'TRAVELOKA*1144026038     SINGAPORE    SG', '-7758 MR BEAN INT L-FUS', '-5250 AIRBNB * HMX283ZD', '-7758 BUS/MRT 440276407', '-7758 KOUFU PTE LTD', '-7758 HELLORIDE', '-7758 SUZUKI COFFEE', '-7758 HELLORIDE', '-7758 GET*GET*MOTHER EA', '-7758 WWW.HEARTBREAKMEL', '-0315 OPENAI *CHATGPT S', 'FOREIGN CURRENCY USD 20.00', '-7758 KOUFU PTE LTD', '-7758 KOUFU PTE LTD', '-7758 CLOVER PLANT BASE', 'ACTIVESG SPORT SINGAPO', 'CCY CONVERSION FEE', 'FOR: 246.68 SGD', 'SUBTOTAL', 'TOTAL', 'TOTAL AMOUNT DUE'],
                2: ['', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', '6564162232', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', '', 'SINGAPORE', '653-163-1004', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SAN FRANCISCO', '', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', '', '', '', '', ''],
                3: ['', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SG', 'SGP', '', 'SGP', 'GBR', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'USA', '', 'SGP', 'SGP', 'SGP', 'SGP', '', '', '', '', ''],
                4: ['AMOUNT', '16.24', '7.20', '53.92', '21.40', '41.70', '3.00', '7.60', '2.00', '12.81', '2.79', '368.00', '6.20', '5.00', '14.04', '13.52', '6.50', '5.70', '232.28', '4.50', '246.68', '11.92', '5.50', '1.00', '9.00', '1.00', '24.90', '11.00', '27.84', '', '8.40', '5.00', '54.19', '1.80', '2.47', '', '1,623.06', '1,623.06', '1,623.06'],
            }),
            True
        )
    ])
    def test_clean_and_detect_transaction_table(self, input_data, expected_output, expected_is_transaction):
        processed_table, is_transaction = clean_and_detect_transaction_table(input_data)
        assert is_transaction == expected_is_transaction
        pd.testing.assert_frame_equal(processed_table, expected_output)

    @pytest.mark.parametrize("input_data, expected_result", [
        (
            pd.DataFrame({
                0: ['180 ', 'Account No. XXXXXXXXX001', 'Transaction', 'Date', '', '01 JUL', '', '', '', '03 JUL', '', '', '', '07 JUL', '', '', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '15 JUL', '', '', '', '18 JUL', '', '', '29 JUL', '', '', '', '30 JUL', '', '', '', '01 AUG', ''],
                1: ['', '', 'Value', 'Date', '', '01 JUL', '', '', '', '03 JUL', '', '', '', '08 JUL', '', '', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '15 JUL', '', '', '', '18 JUL', '', '', '29 JUL', '', '', '', '30 JUL', '', '', '', '31 JUL', ''],
                2: ['', '', '', 'Description', 'BALANCE B/F', 'QUICK TRANSFER', '039465990', 'to John Smith', 'MISC - Other', 'QUICK TRANSFER', 'via InstaPay-Mobile', 'to Sarah', 'MISC - MISC', 'QUICK TRANSFER', '38592938', 'to Michael Lee', 'MISC - Other', 'EXTRA INTEREST', '360 PAYCHECK BONUS', 'EXTRA INTEREST', '360 CREDIT SPEND BONUS', 'EXTRA INTEREST', '360 DEPOSIT BONUS', 'EXTRA INTEREST', '360 INVEST BONUS', 'POS PURCHASE    DEBIT', 'GLOBAL MART', 'GLOBAL BANK', 'TERMINAL', 'AUTO-DEBIT', 'COLL 100100183987', 'CC RECEIVABLE', 'AUTO-DEBIT - WAGES', 'WAGE', 'TechCorp Pte Ltd', 'TechCorp Pte Ltd', 'MONEY TRANSFER', 'via InstaPay-QR Code', 'to DAVID TAN', 'MISC - MISC', 'INTEREST CREDIT', 'BALANCE C/F'],
                3: ['', '', '', 'Cheque', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                4: ['', '', '', 'Withdrawal', '', '700.00', '', '', '', '22.54', '', '', '', '30,000.00', '', '', '', '', '', '', '', '', '', '', '', '207.40', '', '', '', '2,155.03', '', '', '', '', '', '', '3.90', '', '', '', '', ''],
                5: ['', '', '', 'Deposit', '', '', '', '', '', '', '', '', '', '', '', '', '', '205.47', '', '49.31', '', '123.28', '', '197.26', '', '', '', '', '', '', '', '', '4,357.00', '', '', '', '', '', '', '', '23.54', ''],
                6: ['1  JUL 2024 TO 31 JUL 2024', '', '', 'Balance', '427,635.09', '426,935.09', '', '', '', '426,912.55', '', '', '', '396,912.55', '', '', '', '397,118.02', '', '397,167.33', '', '397,290.61', '', '397,487.87', '', '397,280.47', '', '', '', '395,125.44', '', '', '406,482.44', '', '', '', '406,478.54', '', '', '', '406,502.08', '406,502.08'],
                7: ['', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 'Oversea-Chinese Banking Corporation Limited'],
            }),
            True
        ),
        (
            pd.DataFrame({
                0: ['', '', '', '', '', '', '', '', '', '', '', '', '4', '', '', ''],
                1: ['POSB eSavings Account', 'Date', '', '19/08/2024', '', '', '', '28/08/2024', '', '', '', '31/08/2024', '', '', 'DBS Multiplier Account', 'Date'],
                2: ['', 'Description', 'Balance Brought Forward', 'Payments / Collections via GIRO', 'DBS CARD CENTRE', 'DCC (CARDHOL', '987654321098', 'FAST Payment / Receipt', 'LOAN REPAYMENT', '20240828OCBCSGSGBRT9876543', 'OTHER', 'Interest Earned', '4', 'Total Balance Carried Forward:', '', 'Description'],
                3: ['', '', '', '', '', '', '', '', '', '', '', '', '4', '', '', ''],
                4: ['', 'Withdrawal (-)', '', '13,269.68', '', '', '', '', '', '', '', '', '', '13,269.68', '', 'Withdrawal (-)'],
                5: ['', '', '', '', '', '', '', '', '', '', '', '', '4', '', '', ''],
                6: ['', 'Deposit (+)', '', '', '', '', '', '1,000.00', '', '', '', '0.89', '', '1,000.89', '', 'Deposit (+)'],
                7: ['Account No. 987-54321-0', '', '', '', '', '', '', '', '', '', '', '', '4', '', 'Account No. 654-321098-7', ''],
                8: ['', 'Balance', '26,298.51', '13,028.83', '', '', '', '14,028.83', '', '', '', '14,029.72', '', '14,029.72', '', 'Balance'],
            }),
            True
        ),
        (
            pd.DataFrame({
                0: ['Credit Cards', 'Statement of Account', '', '(cid:2)(cid:1)(cid:3)(cid:2)(cid:4)(cid:3)(cid:1)(cid:4)(cid:2)(cid:3)(cid:2)(cid:1)(cid:4)(cid:1)(cid:4)(cid:1)(cid:4)(cid:3)(cid:1)(cid:2)(cid:4)(cid:3)(cid:2)(cid:1)(cid:4)(cid:1)(cid:4)(cid:4)(cid:1)(cid:4)', 'JOHN DOE', '123 MAIN STREET', 'CITY TOWERS', '#12-34', 'SINGAPORE 123456', '', '', 'STATEMENT DATE\nCREDIT LIMIT', '23 May 2024\n$150,000.00', '', '', '', '', '', 'This Statement serves as a TAX INVOICE if GST is charged.', 'DATE', '', '', '20 MAY', '', '', '23 APR', '', '01 MAY', '', '', '', '', '', '', '', '', '', '', 'STATEMENT', '', '', '', ''],
                1: ['', '', '1 of 3', '', '', '', '', '', '', 'DBS Cards P.O. Box 360 S(912312)', 'Hotline: 1800 111 1111', 'MINIMUM PAYMENT\nPAYMENT DUE DATE', '$75.00\n18 Jun 2024', "Please settle this statement promptly. If minimum payment is not received by 'Payment Due Date', a late payment charge of $100 will", 'be  levied.  If  payment  is  not  made  in  full,  an  additional  finance  charge  of  27.80%  per  annum  will  be  levied  on  each  outstanding', 'balance  of  each  card  account  from  the  date  each  transaction  was  effected.  No  finance  charge  will  be  levied  on  new  transactions', '(except Cash Advance transactions) effected after this statement date. Please refer to the last page of statement for more details.', 'Co. Reg. No. 196800306E', 'GST Registration No: MR-8500180-3', 'DESCRIPTION', 'DBS LADIES VISA CARD NO.: 8339 2030 1234 0987', 'PREVIOUS BALANCE', 'AUTO-PYT FROM ACCT#XXXXXXXX1234', 'REF NO: 11689999398715999971650', 'NEW TRANSACTIONS JOHN', 'CUSTOMER.IO EMAIL MARK HTTPSCUSTOMER OR', 'U. S. DOLLAR 150.00', 'DIGITALOCEAN.COM       AMSTERDAM     NL', 'U. S. DOLLAR 500.00', '', '', 'Any Full Amount due will be deducted from bank account XXXXXXXX1234. GIRO deduction date: 18 Jun 2024', '', 'Please contact our Customer Service Officer immediately at 1800 111 1111 or 1800 732 8000 (for Platinum Customers), if you find any', '', 'correct.', 'DBS VISA/MASTERCARD/AMEX CARD - DBS POINTS SUMMARY (AS OF THIS STATEMENT)', '', 'ADJUSTED', '', '', 'This statement is for your information only.', ''],
                2: [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, 'AMOUNT (S$)', '', '16,500.00', '16,500.00 CR', '', '', '210.75', '', '705.00', '', 'SUB-TOTAL:\n915.75', 'TOTAL:\n915.75', '', 'GRAND TOTAL FOR ALL CARD ACCOUNTS:\n915.75', '', 'discrepancies on your statement. If no discrepancy is reported within 7 days upon receipt of statement, your statement will be considered', '', '', 'REDEEMED/\nBALANCE\nEXPIRING ON', 'EXPIRED\n30 JUN 2024', '0\n350,000\nNo Expiry', '0\n350,000\n0', '', 'PDS_CRCRDGCE_LOC_ESTMT_0d67011d0000003c_07973'],
            }),
            False
        ),
        (
            pd.DataFrame({
                0: ['OCBC 180 CARD', 'JOHN DOE', '', '17/05', '24/04', '24/04', '25/04', '25/04', '26/04', '26/04', '26/04', '28/04', '29/04', '30/04', '30/04', '30/04', '01/05', '01/05', '01/05', '01/05', '02/05', '02/05', '02/05', '02/05', '03/05', '03/05', '03/05', '06/05', '06/05', '06/05', '07/05', '07/05'],
                1: ['', '1234-5678-9012-3456', "LAST MONTH'S BALANCE", 'PAYMENT BY GIRO', 'AMAZE* GRAB A-6AR2I', 'AMAZE* HUMVENTURES', 'AMAZE* GRAB A-6AT77', 'AMAZE* GRAB A-6AV28', 'AMAZE* GRAB A-6BXUJ', 'AMAZE* GRAB A-6BXUJ', '-7758 BUS/MRT 429799071', '-7758 NTUC FP-BT PANJANG', '-7758 LILAC OAK', '-7758 KOUFU PTE LTD', 'AMAZE* CTCPPT BDS V', '-4887 BUS/MRT 431965453', '-7758 HELLORIDE', '-4887 ATLAS COFFEEHOUSE', '-4887 AN ACAI AFFAIR HI', '-7758 BUS/MRT 432407001', '-7758 SUPER SIMPLE ONE', 'GOJEK', '-5831 CIRCLES.LIFE', '-7758 TAKAGI RAMEN-CONN', '-7758 KOUFU PTE LTD', '-7758 KOUFU PTE LTD', '-7758 BUS/MRT 433579453', '-7758 HELLORIDE', '-7758 KOUFU PTE LTD', '-7758 KOUFU PTE LTD', '-7758 SUPER SIMPLE ONE', '-7758 KOUFU PTE LTD'],
                2: ['', '', '', '', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', '31353135', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE'],
                3: ['', '', '', '', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SG', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP'],
                4: ['', '', '923.47', '(812.36', '2.15', '58.70', '2.33', '2.81', '10.92', '0.62', '14.83', '3.10', '49.85', '6.80', '(52.18', '5.97', '1.20', '7.95', '9.75', '8.84', '12.98', '24.90', '32.50', '7.45', '5.30', '5.90', '3.25', '1.10', '8.20', '18.75', '50.60', '15.90'],
                5: ['', '', '', ')', '', '', '', '', '', '', '', '', '', '', ')', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
            }),
            False
        ),
        (
            pd.DataFrame({
                0: ['TRANSACTION DATE', '08/05', '10/05', '11/05', '11/05', '11/05', '11/05', '11/05', '11/05', '11/05', '12/05', '13/05', '13/05', '13/05', '13/05', '14/05', '15/05', '15/05', '16/05', '16/05', '16/05', '16/05', '17/05', '18/05', '18/05', '18/05', '18/05', '21/05', '21/05', '', '21/05', '21/05', '22/05', '23/05', '16/05', '', '', '', ''],
                1: ['DESCRIPTION', '-1234 SIMPLE STORE', '-1234 FOOD COURT', 'AMAZE* ONLINE.COM', 'AMAZE* RIDE A-1B2C3', '-1234 GARDEN CAFE', '-1234 COFFEE SHOP', '-1234 COFFEE CHAIN', '-1234 FRUIT VENDING', '-1234 BUS/MRT 123456789', 'CLOUD STORAGE', 'TICKET BOOKING', '-5678 FOOD COURT', '-5678 FOOD COURT', '-5678 BUS/MRT 987654321', '-1234 SIMPLE STORE', '-1234 SANDWICH SHOP', '-5678 BEAN STORE', 'TRAVEL*1234567890     SINGAPORE    SG', '-1234 BEAN STORE', '-9876 RENTAL * ABC123XY', '-1234 BUS/MRT 456789012', '-1234 FOOD COURT', '-1234 BIKE RENTAL', '-1234 COFFEE SHOP', '-1234 BIKE RENTAL', '-1234 GET*GET*FOOD DEL', '-1234 WWW.ONLINESHOP', '-9876 AI *CHATBOT S', 'FOREIGN CURRENCY USD 20.00', '-1234 FOOD COURT', '-1234 FOOD COURT', '-1234 VEGAN RESTAURANT', 'SPORTS BOOKING', 'CCY CONVERSION FEE', 'FOR: 235.68 SGD', 'SUBTOTAL', 'TOTAL', 'TOTAL AMOUNT DUE'],
                2: ['', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', '1234567890', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', '', 'SINGAPORE', '123-456-7890', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SAN FRANCISCO', '', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', 'SINGAPORE', '', '', '', '', ''],
                3: ['', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SG', 'SGP', '', 'SGP', 'GBR', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'SGP', 'USA', '', 'SGP', 'SGP', 'SGP', 'SGP', '', '', '', '', ''],
                4: ['AMOUNT', '15.24', '8.20', '51.92', '19.40', '39.70', '3.50', '6.60', '2.50', '11.81', '2.99', '358.00', '7.20', '6.00', '13.04', '12.52', '7.50', '4.70', '221.28', '5.50', '235.68', '10.92', '6.50', '2.00', '8.00', '2.00', '22.90', '12.00', '26.84', '', '7.40', '6.00', '52.19', '2.80', '2.36', '', '1,532.06', '1,532.06', '1,532.06'],
            }),
            False
        )
    ])
    def test_is_bank_account_table(self, input_data, expected_result):
        assert is_bank_account_table(input_data) == expected_result

    @pytest.mark.parametrize("table_data, pdf_text, expected_statement_date, expected_statement_year", [
        (
            pd.DataFrame({
                0: ['180 ACCOUNT', 'Account No. 123456789012', 'Transaction', 'Date', '', '01 JUL', '', '', '', '03 JUL', '', '', '', '07 JUL', '', '', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '15 JUL', '', '', '', '18 JUL', '', '', '29 JUL', '', '', '', '30 JUL', '', '', '', '01 AUG', ''],
                1: ['', '', 'Value', 'Date', '', '01 JUL', '', '', '', '03 JUL', '', '', '', '08 JUL', '', '', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '09 JUL', '', '15 JUL', '', '', '', '18 JUL', '', '', '29 JUL', '', '', '', '30 JUL', '', '', '', '31 JUL', ''],
                2: ['', '', '', 'Description', 'BALANCE B/F', 'FAST PAYMENT', '987654321', 'to John Smith', 'OTHR - Other', 'FAST PAYMENT', 'via PayNow-Mobile', 'to Jane Doe', 'OTHR - OTHR', 'FAST PAYMENT', '456789123', 'to Alice Johnson', 'OTHR - Other', 'BONUS INTEREST', '360 SALARY BONUS', 'BONUS INTEREST', '360 CC SPEND BONUS', 'BONUS INTEREST', '360 SAVE BONUS', 'BONUS INTEREST', '360 GROW BONUS', 'POS PURCHASE    NETS', 'GROCERY ST', 'GROCERY STORE', 'DOWNTOWN', 'GIRO', 'COLL 100100123456', 'CR CARD RECEIVABLE', 'GIRO - SALARY', 'SALA', 'Tech Corp Ltd', 'Tech Corp Ltd', 'FUND TRANSFER', 'via PayNow-QR Code', 'to BOB WILLIAMS', 'OTHR - OTHR', 'INTEREST CREDIT', 'BALANCE C/F'],
                3: ['', '', '', 'Cheque', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                4: ['', '', '', 'Withdrawal', '', '650.00', '', '', '', '25.75', '', '', '', '28,000.00', '', '', '', '', '', '', '', '', '', '', '', '185.60', '', '', '', '1,987.45', '', '', '', '', '', '', '4.50', '', '', '', '', ''],
                5: ['', '', '', 'Deposit', '', '', '', '', '', '', '', '', '', '', '', '', '', '187.32', '', '52.68', '', '115.79', '', '203.45', '', '', '', '', '', '', '', '', '5,876.00', '', '', '', '', '', '', '', '21.87', ''],
                6: ['1  JUL 2024 TO 31 JUL 2024', '', '', 'Balance', '248,432.43', '247,782.43', '', '', '', '247,756.68', '', '', '', '219,756.68', '', '', '', '219,944.00', '', '219,996.68', '', '220,112.47', '', '220,315.92', '', '220,130.32', '', '', '', '218,142.87', '', '', '228,018.87', '', '', '', '228,014.37', '', '', '', '228,036.24', '228,036.24'],
                7: ['', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 'Oversea-Chinese Banking Corporation Limited'],
            }),
            'OCBC Bank\n65 Chulia Street, OCBC Centre\nSingapore 049513\nOversea-Chinese Banking Corporation Limited\nCo. Reg. No.: 193200032W\nSingapore dollar deposits of non-bank depositors and monies and deposits denominated in Singapore dollars under the Supplementary Retirement Scheme are insured by the Singapore Deposit Insurance Corporation, \nfor up to S$100,000 in aggregate per depositor per Scheme member by law. Monies and deposits denominated in Singapore dollars under the CPF Investment Scheme and CPF Retirement Sum Scheme are aggregated \nand separately insured up to S$100,000 for each depositor per Scheme member. Foreign currency deposits, dual currency investments, structured deposits and other investment products are not insured.Deposit Insurance Scheme6796\\N\\673\\M\n1 2 3 4 5 6\nALICE  JOHNSON\n123 MAIN  STREET\n#10-20\nCITY  TOWERS\nSINGAPORE  123456STATEMENT OF ACCOUNT\nPage 1 of 2\nFor enquiries, please call\nour Customer Service Officers at 1800 363 3333\nOCBC Downtown BranchInformation\nAs part of our efforts to be a more environmentally -friendly bank , all our account\nstatements will now be printed on both sides of the page .\nRNB05ESNG\\1Transaction\nDate Date Description Cheque Withdrawal Deposit BalanceValue180 ACCOUNT\nAccount No. 1234567890121  JUL 2024 TO 31 JUL 2024\n              498,765.43 BALANCE  B/F\n01 JUL                   650.00               498,115.43 01 JUL FAST  PAYMENT\n987654321\nto John Smith\nOTHR  - Other\n03 JUL                    25.75               498,089.68 03 JUL FAST  PAYMENT\nvia PayNow -Mobile\nto Jane Doe\nOTHR  - OTHR\n07 JUL                28,000.00               470,089.68 08 JUL FAST  PAYMENT\n456789123\nto Alice Johnson\nOTHR  - Other\n09 JUL                   187.32               470,277.00 09 JUL BONUS  INTEREST\n360 SALARY  BONUS\n09 JUL                    52.68               470,329.68 09 JUL BONUS  INTEREST\n360 CC SPEND  BONUS\n09 JUL                   115.79               470,445.47 09 JUL BONUS  INTEREST\n360 SAVE  BONUS\n09 JUL                   203.45               470,648.92 09 JUL BONUS  INTEREST\n360 GROW  BONUS\n15 JUL                   185.60               470,463.32 15 JUL POS PURCHASE     NETS\nGROCERY ST\nGROCERY STORE\nDOWNTOWN\n18 JUL                 1,987.45               468,475.87 18 JUL GIRO\nCOLL  100100123456\nCR CARD  RECEIVABLE\n29 JUL                 9,876.00               478,351.87 29 JUL GIRO  - SALARY\nSALA\nTech Corp Ltd\nTech Corp Ltd\n30 JUL                     4.50               478,347.37 30 JUL FUND  TRANSFER\nvia PayNow -QR Code\nto BOB WILLIAMS\nOTHR  - OTHR\n01 AUG                    21.87               478,369.24 31 JUL INTEREST  CREDIT\n              478,369.24 BALANCE  C/F',
            '1  JUL 2024',
            '2024'
        ),
        (
            pd.DataFrame({
                0: ['Credit Cards', 'Statement of Account', '', '(cid:2)(cid:1)(cid:3)(cid:2)(cid:4)(cid:3)(cid:1)(cid:4)(cid:2)(cid:3)(cid:2)(cid:1)(cid:4)(cid:1)(cid:4)(cid:1)(cid:4)(cid:3)(cid:1)(cid:2)(cid:4)(cid:3)(cid:2)(cid:1)(cid:4)(cid:1)(cid:4)(cid:4)(cid:1)(cid:4)', 'JOHN DOE', '123 MAIN STREET', 'CITY TOWERS', '#12-34', 'SINGAPORE 123456', '', '', 'STATEMENT DATE\nCREDIT LIMIT', '23 May 2024\n$150,000.00', '', '', '', '', '', 'This Statement serves as a TAX INVOICE if GST is charged.', 'DATE', '', '', '20 MAY', '', '', '23 APR', '', '01 MAY', '', '', '', '', '', '', '', '', '', '', 'STATEMENT', '', '', '', ''],
                1: ['', '', '1 of 3', '', '', '', '', '', '', 'DBS Cards P.O. Box 360 S(912312)', 'Hotline: 1800 111 1111', 'MINIMUM PAYMENT\nPAYMENT DUE DATE', '$75.00\n18 Jun 2024', "Please settle this statement promptly. If minimum payment is not received by 'Payment Due Date', a late payment charge of $100 will", 'be  levied.  If  payment  is  not  made  in  full,  an  additional  finance  charge  of  27.80%  per  annum  will  be  levied  on  each  outstanding', 'balance  of  each  card  account  from  the  date  each  transaction  was  effected.  No  finance  charge  will  be  levied  on  new  transactions', '(except Cash Advance transactions) effected after this statement date. Please refer to the last page of statement for more details.', 'Co. Reg. No. 196800306E', 'GST Registration No: MR-8500180-3', 'DESCRIPTION', 'DBS LADIES VISA CARD NO.: 5678 9012 3456 7890', 'PREVIOUS BALANCE', 'AUTO-PYT FROM ACCT#XXXXXXXX1234', 'REF NO: 11689999398715999971650', 'NEW TRANSACTIONS JOHN', 'CUSTOMER.IO EMAIL MARK HTTPSCUSTOMER OR', 'U. S. DOLLAR 150.00', 'DIGITALOCEAN.COM       AMSTERDAM     NL', 'U. S. DOLLAR 500.00', '', '', 'Any Full Amount due will be deducted from bank account XXXXXXXX1234. GIRO deduction date: 18 Jun 2024', '', 'Please contact our Customer Service Officer immediately at 1800 111 1111 or 1800 732 8000 (for Platinum Customers), if you find any', '', 'correct.', 'DBS VISA/MASTERCARD/AMEX CARD - DBS POINTS SUMMARY (AS OF THIS STATEMENT)', '', 'ADJUSTED', '', '', 'This statement is for your information only.', ''],
                2: [pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, pd.NA, 'AMOUNT (S$)', '', '16,500.00', '16,500.00 CR', '', '', '210.75', '', '705.00', '', 'SUB-TOTAL:\n915.75', 'TOTAL:\n915.75', '', 'GRAND TOTAL FOR ALL CARD ACCOUNTS:\n915.75', '', 'discrepancies on your statement. If no discrepancy is reported within 7 days upon receipt of statement, your statement will be considered', '', '', 'REDEEMED/\nBALANCE\nEXPIRING ON', 'EXPIRED\n30 JUN 2024', '0\n350,000\nNo Expiry', '0\n350,000\n0', '', 'PDS_CRCRDGCE_LOC_ESTMT_0d67011d0000003c_07973'],
            }), 
            '\x02\x01\x03\x02\x04\x03\x01\x04\x02\x03\x02\x01\x04\x01\x04\x01\x04\x03\x01\x02\x04\x03\x02\x01\x04\x01\x04\x04\x01\x04\nJOHN DOE\n123 MAIN STREET\nCITY TOWERS\n#12-34\nSINGAPORE 123456\nSTATEMENT DATE CREDIT LIMIT MINIMUM PAYMENT PAYMENT DUE DATE\n23 May 2024 $150,000.00 $75.00 18 Jun 2024\nPlease settle this statement promptly. If minimum payment is not received by ©Payment Due Date©, a late payment charge of $100 will\nbe levied. If payment is not made in full, an additional finance charge of 27.80% per annum will be levied on each outstanding\nbalance of each card account from the date each transaction was effected. No finance charge will be levied on new transactions\n(except Cash Advance transactions) effected after this statement date. Please refer to the last page of statement for more details.\nThis Statement serves as a TAX INVOICE if GST is charged.Co. Reg. No. 196800306E\nGST Registration No: MR-8500180-3\nDATE DESCRIPTION AMOUNT (S$)\nDBS LADIES VISA CARD NO.: 8339 2030 1234 0987\nPREVIOUS BALANCE 16,500.00\n20 MAY AUTO-PYT FROM ACCT#XXXXXXXX1234\nREF NO: 1168999939871599997165016,500.00 CR\nNEW TRANSACTIONS JOHN\n23 APR CUSTOMER.IO EMAIL MARK HTTPSCUSTOMER OR\nU. S. DOLLAR 150.00210.75\n01 MAY DIGITALOCEAN.COM       AMSTERDAM     NL\nU. S. DOLLAR 500.00705.00\nSUB-TOTAL: 915.75\nTOTAL: 915.75\nAny Full Amount due will be deducted from bank account XXXXXXXX1234. GIRO deduction date: 18 Jun 2024\nGRAND TOTAL FOR ALL CARD ACCOUNTS: 915.75\nPlease contact our Customer Service Officer immediately at 1800 111 1111 or 1800 732 8000 (for Platinum Customers), if you find any\ndiscrepancies on your statement. If no discrepancy is reported within 7 days upon receipt of statement, your statement will be considered\ncorrect.\nDBS VISA/MASTERCARD/AMEX CARD - DBS POINTS SUMMARY (AS OF THIS STATEMENT)\nCARD NUMBER BALANCE AS OF LAST\nSTATEMENTEARNED/\nADJUSTEDREDEEMED/\nEXPIREDBALANCE EXPIRING ON\n30 JUN 2024\n8339 2030 1234 0987 349,170 830 0 350,000 No Expiry\nTOTAL 349,170 830 0 350,000 0\n  This statement is for your information only.\nDBS Cards P.O. Box 360 S(912312)\nHotline: 1800 111 1111\nCredit Cards  Statement of Account\n1 of 3\nPDS_CRCRDGCE_LOC_ESTMT_0d67011d0000003c_07973',
            '23 May 2024',
            '2024'
        )
    ])
    def test_extract_statement_date(self, table_data, pdf_text, expected_statement_date, expected_statement_year):
        statement_date, statement_year = extract_statement_date(table_data, pdf_text)
        assert statement_date == expected_statement_date
        assert statement_year == expected_statement_year

if __name__ == '__main__':
    pytest.main()