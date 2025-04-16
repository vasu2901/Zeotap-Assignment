# 🔄 ClickHouse ↔ Flat File Ingestion UI

A lightweight, web-based interface for **bi-directional data ingestion** between **ClickHouse** and **CSV files**. Built using **Django**, with `clickhouse_connect` for database interaction and **Pandas** for efficient file processing.

---

## ✨ Features

- 🔐 Securely connect to a ClickHouse instance  
- 📋 Browse database tables and view their columns  
- 📤 Export ClickHouse tables to CSV  
- 📥 Upload CSV files and ingest into ClickHouse  
- 👀 Preview top rows from ClickHouse tables or uploaded files  
- 🧼 Clean and simple frontend using vanilla HTML + JavaScript  

---

## 🛠️ Tech Stack

- **Backend**: Django, ClickHouse Connect, Pandas  
- **Frontend**: HTML, Vanilla JavaScript  
- **Storage**: Django's file system (`default_storage`)  
- **Database**: ClickHouse  

---

## 🚀 Setup Instructions

1. **Clone the repository**

```bash
git clone https://github.com/your-username/zeotap-assignment.git
cd zeotap-assignment
```

2. **Install dependencies**

```bash
pip install -r requirements.txt
```

3. **Run the Django development server**

```bash
python manage.py runserver
```

4. **Access the Web UI**

Visit: [http://localhost:8000/](http://localhost:8000/)

---

## 🧪 API Endpoints

| Method | Endpoint             | Description                                 |
|--------|----------------------|---------------------------------------------|
| POST   | `/connect/`          | Test ClickHouse connection and return tables |
| POST   | `/get_schema/`       | Retrieve table or CSV column metadata       |
| POST   | `/preview_data/`     | Preview data from a ClickHouse table        |
| POST   | `/preview-flatfile/` | Preview data from a flat file               |
| POST   | `/upload/`           | Upload a CSV file to the server             |
| POST   | `/ingest/`           | Ingest data: ClickHouse ↔ File              |

---

## 📁 File Uploads

Uploaded files are saved in the `/uploads/` directory using Django's `default_storage`.

---

## 🧠 Notes

- Use the correct port — typically `8443` for secure HTTPS with ClickHouse  
- If you encounter `InvalidChunkLength` errors, try setting `compression=False` in the ClickHouse client  
- The CSV output is saved as `output.csv` by default (configurable)

---

## 🙌 Credits

Developed as part of a technical challenge. Core logic, UI integration, and ingestion features were implemented independently, with light support from ChatGPT for solving edge-case errors and refining UX behavior.

---