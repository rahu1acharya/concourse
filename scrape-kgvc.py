import os
import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import requests

def create_pg_engine():
    """Create a SQLAlchemy engine for PostgreSQL."""
    try:
        pg_user = os.getenv('PG_USER', 'concourse_user')
        pg_password = os.getenv('PG_PASSWORD', 'concourse_pass')
        pg_host = '192.168.56.1'
        pg_database = os.getenv('PG_DATABASE', 'concourse')
        pg_port = os.getenv('PG_PORT', '5432')

        engine = create_engine(f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}')
        print("PostgreSQL engine created successfully.")
        return engine
    except Exception as e:
        print(f"Error creating PostgreSQL engine: {e}")
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
        print("Data retrieved successfully.")
        return BeautifulSoup(search_response.content, 'html.parser')
    else:
        print(f"Failed to retrieve data. Status Code: {search_response.status_code}")
        return None

def parse_table(soup):
    """Parse the table from BeautifulSoup object and return DataFrame in the desired format."""
    table = soup.find('section', {'id': 'profit-loss'}).find('table')
    if table:
        # Extract headers
        headers = [th.text.strip() for th in table.find_all('th')]

        # Initialize data containers
        data = {header: [] for header in headers[1:]}  # Excluding 'Narration'

        # Process rows
        rows = table.find_all('tr')
        for row in rows[1:]:  # Skip the header row
            cols = [col.text.strip() for col in row.find_all('td')]
            if cols:
                narration = cols[0]
                values = cols[1:]

                # Append data to the corresponding column
                for i, value in enumerate(values):
                    data[headers[i + 1]].append(value)

        # Convert to DataFrame
        df_data = {'Date': headers[1:]}  # Date headers
        for key, values in data.items():
            df_data[key] = values

        df = pd.DataFrame(df_data)
        df = df.rename(columns={'Date': 'Date'})
        
        # Convert columns to numeric and remove symbols
        for col in df.columns[1:]:
            df[col] = df[col].str.replace(r'[%,\'\"]', '', regex=True)  # Remove %, ', and "
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to numeric

        df.fillna('', inplace=True)  # Fill NaN values with empty strings
        df = df.reset_index(drop=True)

        return df
    else:
        print("Failed to find the data table.")
        return None


def save_to_csv(df, file_path):
    """Save DataFrame to CSV file."""
    print(df.head())
    df.to_csv(file_path, index=False)
    print(f"Data successfully saved to CSV: {file_path}")

def load_to_postgres(df, engine, table_name):
    """Load DataFrame into PostgreSQL."""
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print("Data successfully loaded into PostgreSQL.")
    except Exception as e:
        print(f"Error loading data into PostgreSQL: {e}")

def main():
    """Main function to execute the script."""
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    
    # Create PostgreSQL engine
    engine = create_pg_engine()
    if not engine:
        return
    
    # Fetch CSRF token and login
    session = requests.Session()
    login_url = "https://www.screener.in/login/?"
    csrf_token = fetch_login_csrf_token(session, login_url)
    response = login(session, login_url, username, password, csrf_token)

    if response.url == "https://www.screener.in/dash/":
        search_url = "https://www.screener.in/company/RELIANCE/consolidated/"
        soup = fetch_data(session, search_url)

        if soup:
            df = parse_table(soup)
            if df is not None:
                csv_file_path = "reliance_data_transposed.csv"
                save_to_csv(df, csv_file_path)
                load_to_postgres(df, engine, 'reliance_data_transposed')
    else:
        print("Login failed.")

if __name__ == "__main__":
    main()
