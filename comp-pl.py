import os
import pandas as pd
from bs4 import BeautifulSoup
import requests

def create_pg_engine():
    """Create a SQLAlchemy engine for PostgreSQL."""
    # PostgreSQL part removed
    return None

def fetch_login_csrf_token(session, login_url):
    """Fetch CSRF token from the login page."""
    login_page = session.get(login_url)
    soup = BeautifulSoup(login_page.content, 'html.parser')
    return soup.find('input', {'name': 'csrfmiddlewaretoken'})['value']

def login(session, login_url, username, password, csrf_token):
    """Perform login and return session response."""
    login_payload = {
        'username': username,
        'password': password,
        'csrfmiddlewaretoken': csrf_token
    }
    headers = {
        'Referer': login_url,
        'User-Agent': 'Mozilla/5.0'
    }
    return session.post(login_url, data=login_payload, headers=headers)

def fetch_data(session, search_url):
    """Fetch data from the search URL and return BeautifulSoup object."""
    search_response = session.get(search_url)
    if search_response.status_code == 200:
        print(f"Data retrieved successfully from {search_url}.")
        return BeautifulSoup(search_response.content, 'html.parser')
    else:
        print(f"Failed to retrieve data from {search_url}. Status Code: {search_response.status_code}")
        return None

def parse_table(soup):
    """Parse the table from BeautifulSoup object and return DataFrame."""
    table = soup.find('section', {'id': 'profit-loss'}).find('table')
    if table:
        headers = [th.text.strip() or f'Column_{i}' for i, th in enumerate(table.find_all('th'))]
        rows = table.find_all('tr')
        row_data = []

        for row in rows[1:]:
            cols = [col.text.strip() for col in row.find_all('td')]
            if len(cols) == len(headers):
                row_data.append(cols)
            else:
                print(f"Row data length mismatch: {cols}")

        df = pd.DataFrame(row_data, columns=headers)
        if not df.empty:
            df.columns = ['Narration'] + df.columns[1:].tolist()
        df = df.reset_index(drop=True)
        return df
    else:
        print("Failed to find the data table.")
        return None

def save_to_csv(df, company_name, all_data_list):
    """Append transposed DataFrame to a list with company name."""
    if df is not None:
        df_transposed = df.set_index('Narration').T  # Transpose the DataFrame
        df_transposed.reset_index(inplace=True)
        df_transposed.rename(columns={'index': 'Date'}, inplace=True)  # Rename index column to 'Date'

        # Clean and convert columns to numeric (except 'Date')
        for col in df_transposed.columns:
            if col != 'Date':
                # Remove commas and percentage symbols, then convert to numeric
                df_transposed[col] = df_transposed[col].replace({',': '', '%': ''}, regex=True)
                df_transposed[col] = pd.to_numeric(df_transposed[col], errors='coerce')

        # Fill NaN values
        df_transposed = df_transposed.fillna(0)

        # Clean column names: lowercase, replace spaces and symbols with underscores
        df_transposed.columns = [col.lower().replace(' ', '_').replace('+', '').replace('%', 'percent') for col in df_transposed.columns]
        df_transposed.rename(columns=lambda x: x.strip(), inplace=True)

        # Add company name as a new column
        df_transposed['company_name'] = company_name

        all_data_list.append(df_transposed)  # Append DataFrame to the list

def main():
    """Main function to execute the script."""
    username = "rahul.acharya@godigitaltc.com"    #os.getenv("USERNAME")
    password = "st0cksrahul@1"    #os.getenv("PASSWORD")

    # List of companies to scrape
    companies = [
            "HINDUNILVR", "ITC", "JYOTHYLAB", "BRITANNIA", "TATACONSUM",
            "DABUR", "GODREJCP", "MARICO", "ZYDUSWELL", "EMAMILTD"
        ]

    all_data_list = []

    # Fetch CSRF token and login
    session = requests.Session()
    login_url = "https://www.screener.in/login/?"
    csrf_token = fetch_login_csrf_token(session, login_url)
    response = login(session, login_url, username, password, csrf_token)

    if response.url == "https://www.screener.in/dash/":
        for company in companies:
            search_url = f"https://www.screener.in/company/{company}/consolidated/"
            soup = fetch_data(session, search_url)

            if soup:
                df = parse_table(soup)
                if df is not None:
                    save_to_csv(df, company, all_data_list)
    
        # Concatenate all DataFrames and save to a single CSV file
        if all_data_list:
            merged_df = pd.concat(all_data_list, ignore_index=True)
            merged_df.to_csv("all_companies_profit_loss.csv", index=False)
            print("All data successfully saved to CSV: all_companies_profit_loss.csv")
    else:
        print("Login failed.")

if __name__ == "__main__":
    main()
