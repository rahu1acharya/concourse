import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine

# Function to fetch OHLC data for multiple companies
def fetch_ohlc_data(companies, period="1mo"):
    all_data = pd.DataFrame()
    
    for name, symbol in companies.items():
        stock = yf.Ticker(symbol)
        data = stock.history(period=period)
        data['Company'] = name
        all_data = pd.concat([all_data, data])
    
    # Reset index so Date becomes a column
    all_data.reset_index(inplace=True)
    
    cols = ['Date', 'Company'] + [col for col in all_data.columns if col not in ['Date', 'Company']]
    all_data = all_data[cols]
    
    return all_data

# Function to save data to CSV
def save_to_csv(data, filename):
    data.to_csv(filename, index=False)
    print(f"Data saved to {filename}")

# Function to load data into PostgreSQL
def load_to_postgresql(data, db_details, table_name):
    connection_string = f"postgresql://{db_details['user']}:{db_details['password']}@{db_details['host']}:{db_details['port']}/{db_details['dbname']}"
    engine = create_engine(connection_string)
    data.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data loaded into PostgreSQL table '{table_name}'.")

# Main function to orchestrate the process
def main():
    companies = {
        "HINDUNILVR": "HINDUNILVR.NS",
        "ITC": "ITC.NS",
        "JYOTHYLAB": "JYOTHYLAB.NS",
        "BRITANNIA": "BRITANNIA.NS",
        "TATACONSUM": "TATACONSUM.NS",
        "DABUR": "DABUR.NS",
        "GODREJCP": "GODREJCP.NS",
        "MARICO": "MARICO.NS",
        "ZYDUSWELL": "ZYDUSWELL.NS",
        "EMAMILTD": "EMAMILTD.NS"
    }
    
    # Fetch OHLC data
    all_data = fetch_ohlc_data(companies)
    
    # Save to CSV
    save_to_csv(all_data, 'combined_ohlc_data.csv')
    
    # PostgreSQL database details
    db_details = {
        'host': '192.168.56.1',
        'port': '5432',
        'dbname': 'concourse',
        'user': 'concourse_user',
        'password': 'concourse_pass'
    }
    
    # Load data into PostgreSQL
    load_to_postgresql(all_data, db_details, 'ohlc_data')

# Run the process
if __name__ == "__main__":
    main()
