import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd
from sqlalchemy import create_engine
from scrapy.http import FormRequest

class RelianceSpider(scrapy.Spider):
    name = 'reliance'
    login_url = 'https://www.screener.in/login/?'
    search_url = 'https://www.screener.in/company/RELIANCE/consolidated/'
    
    def start_requests(self):
        username = self.settings.get('USERNAME')
        password = self.settings.get('PASSWORD')
        print(username, password)
        # Start with the login request
        yield scrapy.Request(self.login_url, callback=self.login, meta={'username': username, 'password': password})
    
    def login(self, response):
        username = response.meta['username']
        password = response.meta['password']
        
        csrf_token = response.css('input[name="csrfmiddlewaretoken"]::attr(value)').get()
        yield FormRequest(
            self.login_url,
            formdata={
                'username': username,
                'password': password,
                'csrfmiddlewaretoken': csrf_token
            },
            callback=self.after_login
        )
    
    def after_login(self, response):
        if response.url == "https://www.screener.in/dash/":
            yield scrapy.Request(self.search_url, callback=self.parse_table)
        else:
            self.logger.error("Login failed.")

    def parse_table(self, response):
        rows = response.css('section#profit-loss table tr')
        headers = [header.css('::text').get().strip() or f'Column_{i}' for i, header in enumerate(rows[0].css('th'))]
        
        data = []
        for row in rows[1:]:
            cols = [col.css('::text').get().strip() for col in row.css('td')]
            if len(cols) == len(headers):
                data.append(cols)
            else:
                self.logger.error(f"Row data length mismatch: {cols}")

        df = pd.DataFrame(data, columns=headers)
        if not df.empty:
            df.columns = ['Narration'] + df.columns[1:].tolist()
        df = df.reset_index(drop=True)
        csv_file_path = "reliance_data1.csv"
        df.to_csv(csv_file_path, index=False)
        self.logger.info(f"Data successfully saved to CSV: {csv_file_path}")
        
        # Load to PostgreSQL
        self.load_to_postgres(df, 'reliance_data1')
    
    def load_to_postgres(self, df, table_name):
        engine = self.create_pg_engine()
        if not engine:
            return
        
        try:
            df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            self.logger.info("Data successfully loaded into PostgreSQL.")
        except Exception as e:
            self.logger.error(f"Error loading data into PostgreSQL: {e}")
    
    def create_pg_engine(self):
        try:
            pg_user = self.settings.get('PG_USER', 'concourse_user')
            pg_password = self.settings.get('PG_PASSWORD', 'concourse_pass')
            pg_host = '192.168.56.1'
            pg_database = self.settings.get('PG_DATABASE', 'concourse')
            pg_port = self.settings.get('PG_PORT', '5432')

            engine = create_engine(f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}')
            self.logger.info("PostgreSQL engine created successfully.")
            return engine
        except Exception as e:
            self.logger.error(f"Error creating PostgreSQL engine: {e}")
            return None

if __name__ == "__main__":
    settings = {
        'USERNAME': 'your_username',
        'PASSWORD': 'your_password',
        'PG_USER': 'concourse_user',
        'PG_PASSWORD': 'concourse_pass',
        'PG_DATABASE': 'concourse',
        'PG_PORT': '5432',
        'LOG_LEVEL': 'INFO',
    }

    process = CrawlerProcess(settings)
    process.crawl(RelianceSpider)
    process.start()
