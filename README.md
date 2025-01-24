# Adventure Works Data Extraction Pipeline 🪈

An automated data extraction system that pulls data from multiple sources (SQL Database, Data Lake, and Parquet files) in parallel.

## 📋 Table of Contents
- [Overview](#overview)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Scheduling](#scheduling)
- [Logging](#logging)
- [Contributing](#contributing)

## 🔍 Overview

This project implements an automated data extraction pipeline that processes data from three different sources:
- SQL Database extraction
- Azure Data Lake storage
- Parquet files processing

All extractions run in parallel using Python's multiprocessing to optimize performance.

## 📂 Project Structure
```
.
├── data/                      # Data storage directory
│   ├── machine_learning/
│   ├── nlp_data/
│   └── product_eval/
├── logs/                      # Log files directory
├── scripts/
│   └── run_extraction.sh      # Shell script for cron execution
├── src/
│   ├── extract_from_db.py
│   ├── extract_from_datalake.py
│   ├── process_parquet_files.py
│   ├── main_extract.py
│   └── utils.py
├── .env                       # Environment variables
└── README.md
```

## ⚙️ Prerequisites

- Python 3.8+
- Azure Storage Account (for Data Lake access)
- SQL Server instance
- Required Python packages (see Installation section)

## 🛠️ Installation

1. Clone the repository:
```bash
git clone https://github.com/your-username/data-extraction-pipeline.git
cd data-extraction-pipeline
```

2. Install ODBC Driver (required for SQL Server connection):
```bash
# On Ubuntu/Debian
./scripts/install_odbc.sh

# On Windows
# Download and install the driver from:
# https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
```

3. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows, use .venv\Scripts\activate
```

4. Install the required Python packages:
```bash
pip install -r requirements.txt
```

## ⚡ Configuration

Create a `.env` file in the project root, similar to `.env.example`.
Specify the SQL Server credentials and Azure Storage Account key in the `.env` file.

## 🚀 Usage
To run the extraction pipeline manually:
```bash
python src/main_extract.py
```

To schedule the pipeline to run at a specific time, use the `run_extraction.sh` script:
```bash
./scripts/run_extraction.sh
```

## ⏰ Scheduling
The pipeline is configured to run daily at 8 PM using cron:

1. Make the shell script executable:
```bash
chmod +x scripts/run_extraction.sh
```

2. Add the script to the crontab:
```bash
crontab -e
```

3. Add the following line to schedule the pipeline:
```bash
0 20 /full/path/to/your/project/scripts/run_extraction.sh >> /full/path/to/your/project/logs/cron.log 2>&1
```

## 📝 Logging

Logs are stored in the `logs/` directory with separate log files for each component:
- `main_extract.log`: Overall pipeline execution
- `extract_from_db.log`: SQL extraction logs
- `extract_from_datalake.log`: Data Lake extraction logs
- `process_parquet_files.log`: Parquet processing logs

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Credits
Made with ❤️ with the help of [Cursor](https://www.cursor.com/).