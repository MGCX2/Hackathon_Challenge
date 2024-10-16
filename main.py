import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from cfonts import render

# Note: Do not rename this file, it must be the entry point of your application.

# Note 2: You must read from the following environment variables:
# ADMIN_API_KEY -> "The secret API key used to call the API endpoints (the Bearer token)"
# DB_HOST -> "The hostname of the database"
# DB_PORT -> "The port of the database"
# DB_USERNAME -> "The username of the database"
# DB_PASSWORD -> "The password of the database"
# DB_NAME -> "The name of the database"
# API_BASE_URL -> "The base URL of the API your project will connect to"

# Example:
ADMIN_API_KEY = os.getenv('ADMIN_API_KEY')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
API_BASE_URL = os.getenv('API_BASE_URL')

print(render('Hello ZYLYTY!', colors=['cyan', 'magenta'], align='center', font='3d'))
print(f"Admin API Key: {ADMIN_API_KEY}")
print(f"Database Host: {DB_HOST}")
print(f"Database Port: {DB_PORT}")
print(f"Database Username: {DB_USERNAME}")
print(f"Database Password: {DB_PASSWORD}")
print(f"Database Name: {DB_NAME}")
print(f"API Base URL: {API_BASE_URL}")


headers = {
    'Authorization' : f'Bearer {ADMIN_API_KEY}'
}


#   Function to download CVSs
def download_csv(endpoint, filename):
    url = f"{API_BASE_URL}{endpoint}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open(filename, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded {filename} successfully.")
        return filename
    else:
        print(f"Failed to download {filename}. Status code: {response.status_code}")

#   Function to download paginated Transactions in JSON
def get_transactions(page):
    url = f"{API_BASE_URL}/transactions?page={page}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        transactions = response.json()  # Expecting a list directly
        #print(f"Transactions response (page {page}): {transactions}")  # For debugging
        return transactions  # Return the list of transactions
    else:
        print(f"Failed to get transactions. Status code: {response.status_code}")
        return None

# Example main, modify at will
def main():
    accounts_csv = download_csv('/download/accounts.csv', 'accounts.csv')
    clients_csv = download_csv('/download/clients.csv', 'clients.csv')

    all_transactions = []
    page = 0

    # Get transactions
    while page !=22:
        page = 0
        all_transactions = []
        number_of_transactions = 0
        while True:

            transactions = get_transactions(page)

            if transactions is None or not transactions:
                break

            print(f"Fetched {len(transactions)} transactions on page {page}")
            number_of_transactions += len(transactions)

            all_transactions.extend(transactions)

            page += 1


    # Convert to DataFrames
    accounts_original_df = pd.read_csv(accounts_csv)
    clients_original_df = pd.read_csv(clients_csv)
    transactions_original_df = pd.DataFrame(all_transactions)

    print(accounts_original_df)
    print(clients_original_df)
    print(transactions_original_df.head())

    # Data Transformation____________________
        # Remove rows with null IDs and drop duplicate transactions
    valid_accounts_df = accounts_original_df[accounts_original_df['account_id'].notnull() & accounts_original_df['client_id'].notnull()]
    valid_accounts_df['client_id'] = valid_accounts_df['client_id'].astype(str)
    transactions_original_df = transactions_original_df[transactions_original_df['account_id'].notnull() & transactions_original_df['transaction_id'].notnull()]
    transactions_original_df.drop_duplicates(inplace=True)

        # Convert the 'amount' column to numeric, coercing invalid values to NaN
    transactions_original_df['amount'] = pd.to_numeric(transactions_original_df['amount'], errors='coerce')

        # Drop rows with null amount
    transactions_cleaned_df = transactions_original_df.dropna(subset=['amount'])

        # Convert amount column to int
    transactions_cleaned_df = transactions_cleaned_df.copy()
    transactions_cleaned_df['amount'] = transactions_cleaned_df['amount'].astype(int)

        # Convert account id to int
    transactions_cleaned_df['account_id'] = transactions_cleaned_df['account_id'].astype(int)

        # Convert timestamp column to datetime
    transactions_cleaned_df['timestamp'] = pd.to_datetime(transactions_cleaned_df['timestamp'])

    transactions_cleaned_df['type'] = transactions_cleaned_df['type'].map({'True': True, 'False': False})

    # PRINT NUMBER OF ROWS OF EACH TABLE GOING INTO THE DATABASE
    print(
        f"ZYLYTY Data Import Completed [{len(valid_accounts_df)}, {len(clients_original_df)}, {len(transactions_cleaned_df)}]")

    print(valid_accounts_df.dtypes)
"""    # Connect to PostgreSQL database
    connection_string = f"postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_string)
    clients_original_df.to_sql('Clients', engine, if_exists='replace', index=False)
    valid_accounts_df.to_sql('Accounts', engine, if_exists='replace', index=False)
    transactions_cleaned_df.to_sql('Transactions', engine, if_exists='replace', index=False)"""

if __name__ == "__main__":
    main()
