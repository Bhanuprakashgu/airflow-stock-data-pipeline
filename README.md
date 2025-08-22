# 📊 Dockerized Stock Data Pipeline with Airflow  

## 🔎 What this project does  
This project is about building a **data pipeline** that:  
1. Fetches daily stock prices (AAPL, MSFT, GOOGL… or any you set in `.env`).  
2. Processes the data (open, high, low, close, volume).  
3. Saves it into a PostgreSQL database.  
4. Runs automatically using **Airflow** (scheduler + UI to monitor).  

Everything runs inside **Docker containers**, so setup is just one command.  

---

## 📂 Project files  
- **`docker-compose.yml`** → runs all services (Airflow + Postgres).  
- **`dags/stock_pipeline_dag.py`** → the pipeline logic (fetch → parse → save).  
- **`.env`** → stores secrets and config (API key, DB creds, stock symbols).  
- **`README.md`** → setup and usage guide (this file).  

---

## ⚙️ Setup (Step by Step)  

### 1. Clone and move into folder
```bash
git clone git remote add origin https://github.com/Bhanuprakashgu/airflow-stock-data-pipeline.git
```

### 2. Edit `.env` file
Add your own values (especially API key):  
```env
POSTGRES_USER=admin
POSTGRES_PASSWORD=password
POSTGRES_DB=stock_db
ALPHA_VANTAGE_API_KEY=YOUR_API_KEY
STOCK_SYMBOLS=AAPL,MSFT,GOOGL
REQUEST_INTERVAL_SEC=15
```

👉 Get your free API key from [Alpha Vantage](https://www.alphavantage.co/support/#api-key).  

---

### 3. Build and start containers
```bash
docker-compose up -d --build
```

This starts:  
- PostgreSQL database  
- Airflow scheduler  
- Airflow webserver (UI)  

---

### 4. Initialize Airflow database (only once)
Run this the first time:  
```bash
docker-compose run airflow-webserver airflow db init
docker-compose up -d
```

---

### 5. Open Airflow UI
Go to: [http://localhost:8080](http://localhost:8080)  

Login with:  
- Username: `admin`  
- Password: `admin`  

You will see the DAG: **`stock_data_pipeline`**.  

---

### 6. Run the pipeline
- Switch the DAG **ON**.  
- Trigger manually (⚡ button) or wait for schedule.  
- Tasks turn **green** if successful, **red** if failed.  

---

## 🛠 Checking the data in Postgres  

Open the database:  
```bash
docker exec -it postgres_db psql -U admin -d stock_db
```

Inside psql, try:  
```sql
\dt;   -- list tables
SELECT COUNT(*) FROM stock_data;  -- total rows
SELECT * FROM stock_data ORDER BY record_date DESC LIMIT 5;  -- latest 5 rows
SELECT symbol, COUNT(*) FROM stock_data GROUP BY symbol;  -- rows per stock
\q  -- exit
```

---

## 🔒 Security  
- No secrets inside code.  
- API keys and DB credentials are in `.env`.  
- Airflow reads them automatically.  

---

## ✅ Why this pipeline is good  
- Easy to set up with **one Docker command**.  
- Can fetch **multiple stocks**.  
- Handles **API retries** if rate-limited.  
- No duplicate rows (uses UPSERT).  
- Airflow UI makes monitoring simple.  

---

## 📈 What you can add next  
- Add another API like Yahoo Finance as backup.  
- Build dashboards with Superset or Metabase.  
- Add tests to check data quality.  

---

🙌 That’s it! With this README, anyone can set up and run your pipeline without confusion.  
