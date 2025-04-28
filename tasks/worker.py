import os
from celery import Celery
import time
from datetime import datetime
import pandas as pd
import sqlite3
import os

# Pobierz parametry połączenia z RabbitMQ z zmiennych środowiskowych lub użyj domyślnych
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = os.environ.get('RABBITMQ_PORT', '5672')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS', 'guest')

# URL brokera i backendu
broker_url = f'amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}:{RABBITMQ_PORT}//'

# Użyj Redis jako backendu, jeśli jest dostępny, w przeciwnym razie użyj RPC
if os.environ.get('REDIS_URL'):
    backend_url = os.environ.get('REDIS_URL')
else:
    backend_url = 'rpc://'

# Inicjalizacja aplikacji Celery
app = Celery('financial_dashboard',
             broker=broker_url,
             backend=backend_url)

# Konfiguracja Celery
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Europe/Warsaw',
    enable_utc=True,
)

@app.task
def generate_portfolio_report(user_id, report_type, period):
    """
    Zadanie generowania raportu portfolio w tle
    """
    # Zapewnienie, że katalog reports istnieje
    if not os.path.exists("reports"):
        os.makedirs("reports")
    
    # Symulacja długotrwałego zadania
    time.sleep(5)  # Symulacja obliczeń
    
    # Połączenie z bazą danych
    conn = sqlite3.connect("portfolio.db", check_same_thread=False)
    
    try:
        # Pobieranie danych dla raportu
        if report_type == "portfolio_summary":
            # Sprawdź czy tabela istnieje
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='portfolio'")
            if cursor.fetchone() is None:
                # Tabela nie istnieje, utwórz przykładowe dane
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS portfolio 
                    (id INTEGER PRIMARY KEY, ticker TEXT, shares INTEGER, price REAL, date TEXT)
                ''')
                conn.commit()
                
            df = pd.read_sql("SELECT * FROM portfolio", conn)
            
            # Utworzenie raportu
            report_path = f"reports/portfolio_summary_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(report_path)
            
        elif report_type == "balance_history":
            # Sprawdź czy tabela istnieje
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='balance_history'")
            if cursor.fetchone() is None:
                # Tabela nie istnieje, utwórz przykładowe dane
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS balance_history 
                    (id INTEGER PRIMARY KEY, date TEXT, balance REAL, notes TEXT)
                ''')
                conn.commit()
                
            df = pd.read_sql("SELECT * FROM balance_history", conn)
            
            # Utworzenie raportu
            report_path = f"reports/balance_history_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(report_path)
    finally:
        conn.close()
        
    return {"status": "completed", "report_path": report_path}

@app.task
def generate_market_analysis(tickers, period, interval):
    """
    Zadanie analizy rynku dla wybranych spółek
    """
    # Zapewnienie, że katalog reports istnieje
    if not os.path.exists("reports"):
        os.makedirs("reports")
    
    # Symulacja długotrwałego zadania
    time.sleep(10)  # Dłuższa symulacja obliczeń
    
    # Tutaj można dodać import yfinance i rzeczywiste pobieranie danych
    import yfinance as yf
    
    # Używamy autentycznych danych z Yahoo Finance
    data = None
    try:
        data = yf.download(tickers, period=period, interval=interval)
        
        # Przekształcenie danych do formatu CSV
        if isinstance(data.columns, pd.MultiIndex):
            # Mamy wiele tickerów - spłaszczamy MultiIndex
            data = data.stack(level=0).reset_index()
            data.columns = ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
        else:
            # Mamy pojedynczy ticker
            data = data.reset_index()
            data['Ticker'] = tickers[0]  # Dodajemy kolumnę z tickerem
    
    except Exception as e:
        # W przypadku błędu zwracamy informację o błędzie
        return {"status": "error", "message": str(e)}
    
    if data is None or data.empty:
        return {"status": "error", "message": "Nie udało się pobrać danych dla wybranych spółek"}
    
    # Utworzenie raportu
    report_path = f"reports/market_analysis_{'-'.join(tickers)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    data.to_csv(report_path)
    
    return {"status": "completed", "report_path": report_path} 
#