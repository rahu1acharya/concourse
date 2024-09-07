# import yfinance as yf
# import pandas as pd
# from sqlalchemy import create_engine

# # Function to fetch OHLC data for multiple companies
# def fetch_ohlc_data(companies, period="1mo"):
#     all_data = pd.DataFrame()
    
#     for name, symbol in companies.items():
#         stock = yf.Ticker(symbol)
#         data = stock.history(period=period)[['Open', 'High', 'Low', 'Close']]
#         data['Company'] = name
#         all_data = pd.concat([all_data, data])
    
#     # Reset index so Date becomes a column
#     all_data.reset_index(inplace=True)
    
#     cols = ['Date', 'Company'] + [col for col in all_data.columns if col not in ['Date', 'Company']]
#     all_data = all_data[cols]
    
#     return all_data

# # Function to save data to CSV
# def save_to_csv(data, filename):
#     data.to_csv(filename, index=False)
#     print(f"Data saved to {filename}")

# # Function to load data into PostgreSQL
# def load_to_postgresql(data, db_details, table_name):
#     connection_string = f"postgresql://{db_details['user']}:{db_details['password']}@{db_details['host']}:{db_details['port']}/{db_details['dbname']}"
#     engine = create_engine(connection_string)
#     data.to_sql(table_name, engine, if_exists='replace', index=False)
#     print(f"Data loaded into PostgreSQL table '{table_name}'.")

# # Main function to orchestrate the process
# def main():
#     companies = {
#         "HINDUNILVR": "HINDUNILVR.NS",
#         "ITC": "ITC.NS",
#         "JYOTHYLAB": "JYOTHYLAB.NS",
#         "BRITANNIA": "BRITANNIA.NS",
#         "TATACONSUM": "TATACONSUM.NS",
#         "DABUR": "DABUR.NS",
#         "GODREJCP": "GODREJCP.NS",
#         "MARICO": "MARICO.NS",
#         "ZYDUSWELL": "ZYDUSWELL.NS",
#         "EMAMILTD": "EMAMILTD.NS"
#     }
    
#     # Fetch OHLC data
#     all_data = fetch_ohlc_data(companies)
    
#     # Save to CSV
#     save_to_csv(all_data, 'combined_ohlc_data.csv')
    
#     # PostgreSQL database details
#     db_details = {
#         'host': '192.168.56.1',
#         'port': '5432',
#         'dbname': 'concourse',
#         'user': 'concourse_user',
#         'password': 'concourse_pass'
#     }
    
#     # Load data into PostgreSQL
#     load_to_postgresql(all_data, db_details, 'ohlc_data')

# # Run the process
# if __name__ == "__main__":
#     main()


# import yfinance as yf
# import pandas as pd
# from sqlalchemy import create_engine
# from datetime import datetime, timedelta

# # Function to fetch OHLC data for the closest working day after March 1st
# def fetch_ohlc_data(companies, start_year=2013, end_year=2024):
#     all_data = pd.DataFrame()

#     for name, symbol in companies.items():
#         stock = yf.Ticker(symbol)

#         for year in range(start_year, end_year + 1):
#             # Start from March 1st
#             start_date = datetime(year, 3, 1)
            
#             # Attempt to find data for the closest working day after March 1st
#             data_found = False
#             while not data_found:
#                 # Convert the date to string format
#                 start_str = start_date.strftime('%Y-%m-%d')
#                 end_date = start_date + timedelta(days=1)  # Check 1 day interval
                
#                 try:
#                     data = stock.history(start=start_str, end=end_date.strftime('%Y-%m-%d'))[['Open', 'High', 'Low', 'Close']]
                    
#                     if not data.empty:
#                         data['Date'] = data.index.strftime('%b %Y')
#                         data['Company'] = name
#                         data['Year'] = year
#                         all_data = pd.concat([all_data, data])
#                         data_found = True  # Exit the loop if data is found
#                     else:
#                         print(f"No data for {name} ({symbol}) on {start_str}, checking the next day...")
#                         start_date += timedelta(days=1)  # Move to the next day
                        
#                 except Exception as e:
#                     print(f"Error fetching data for {name} ({symbol}) on {start_str}: {e}")
#                     start_date += timedelta(days=1)  # Move to the next day if an error occurs

#     # Reset index so Date becomes a column
#     all_data.reset_index(drop=True, inplace=True)

#     cols = ['Date', 'Company', 'Year'] + [col for col in all_data.columns if col not in ['Date', 'Company', 'Year']]
#     all_data = all_data[cols]

#     return all_data

# # Function to save data to CSV
# def save_to_csv(data, filename):
#     data.to_csv(filename, index=False)
#     print(f"Data saved to {filename}")

# # Function to load data into PostgreSQL
# def load_to_postgresql(data, db_details, table_name):
#     connection_string = f"postgresql://{db_details['user']}:{db_details['password']}@{db_details['host']}:{db_details['port']}/{db_details['dbname']}"
#     engine = create_engine(connection_string)
#     data.to_sql(table_name, engine, if_exists='replace', index=False)
#     print(f"Data loaded into PostgreSQL table '{table_name}'.")

# # Main function to orchestrate the process
# def main():
#     companies = {
#         "HINDUNILVR": "HINDUNILVR.NS",
#         "ITC": "ITC.NS",
#         "JYOTHYLAB": "JYOTHYLAB.NS",
#         "BRITANNIA": "BRITANNIA.NS",
#         "TATACONSUM": "TATACONSUM.NS",
#         "DABUR": "DABUR.NS",
#         "GODREJCP": "GODREJCP.NS",
#         "MARICO": "MARICO.NS",
#         "ZYDUSWELL": "ZYDUSWELL.NS",
#         "EMAMILTD": "EMAMILTD.NS"
#     }

#     # Fetch OHLC data for March 1st of each year from 2013 to 2024
#     all_data = fetch_ohlc_data(companies, start_year=2013, end_year=2024)

#     # Save to CSV
#     save_to_csv(all_data, 'march1_ohlc_data_with_closest_working_day.csv')

#     # PostgreSQL database details
#     db_details = {
#         'host': '192.168.56.1',
#         'port': '5432',
#         'dbname': 'concourse',
#         'user': 'concourse_user',
#         'password': 'concourse_pass'
#     }

#     # Load data into PostgreSQL
#     load_to_postgresql(all_data, db_details, 'ohlc_march')

# # Run the process
# if __name__ == "__main__":
#     main()


import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Function to fetch OHLC and Volume data for fiscal year (April 1 to March 31)
def fetch_ohlc_data(companies, start_year=2013, end_year=2024):
    all_data = pd.DataFrame()

    for name, symbol in companies.items():
        stock = yf.Ticker(symbol)

        for year in range(start_year, end_year + 1):
            # Fiscal year starts from April 1 of the previous year to March 31 of the current year
            start_date = datetime(year - 1, 4, 1)
            end_date = datetime(year, 3, 31)

            try:
                # Fetch data for the fiscal year period, including Volume
                data = stock.history(start=start_date, end=end_date)[['Open', 'High', 'Low', 'Close', 'Volume']]

                if not data.empty:
                    # Aggregate OHLC data using first, max, min, and last for the period
                    yearly_data = {
                        'Open': data['Open'].iloc[0],   # First Open of the period
                        'High': data['High'].max(),     # Maximum High of the period
                        'Low': data['Low'].min(),       # Minimum Low of the period
                        'Close': data['Close'].iloc[-1], # Last Close of the period
                        'Volume': data['Volume'].sum()  # Total Volume over the fiscal year
                    }

                    # Convert it into a DataFrame
                    yearly_data_df = pd.DataFrame([yearly_data], index=[f"FY {year-1}-{year}"])
                    
                    # Add columns for Company and Date (Fiscal Year ending in March)
                    yearly_data_df['Company'] = name
                    yearly_data_df['Date'] = f"Mar {year}"

                    all_data = pd.concat([all_data, yearly_data_df])

                else:
                    print(f"No data for {name} ({symbol}) for the period {start_date} to {end_date}.")

            except Exception as e:
                print(f"Error fetching data for {name} ({symbol}) for the period {start_date} to {end_date}: {e}")

    # Reset index so Fiscal_Year becomes a column
    all_data.reset_index(drop=True, inplace=True)

    # Reorder columns for better readability
    cols = ['Date', 'Company'] + [col for col in all_data.columns if col not in ['Date', 'Company']]
    all_data = all_data[cols]

    return all_data

# Function to save data to CSV
def save_to_csv(data, filename):
    data.to_csv(filename, index=False)
    print(f"Data saved to {filename}")

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

    # Fetch OHLC and Volume data for fiscal years from April 1 to March 31
    all_data = fetch_ohlc_data(companies, start_year=2013, end_year=2024)

    # Save to CSV
    save_to_csv(all_data, 'fiscal_year_ohlc_data.csv')

    # PostgreSQL database details
    db_details = {
        'host': '192.168.56.1',
        'port': '5432',
        'dbname': 'concourse',
        'user': 'concourse_user',
        'password': 'concourse_pass'
    }

    # Load data into PostgreSQL
    load_to_postgresql(all_data, db_details, 'fiscal_year_ohlc')

# Run the process
if __name__ == "__main__":
    main()



