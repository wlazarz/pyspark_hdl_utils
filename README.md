# Hadoop Data Lake Utilities

This repository provides a collection of modular, production-ready Python utilities designed to simplify working with Hadoop-based Data Lakes using PySpark. It includes robust interfaces for interacting with HDFS, Hive, and MSSQL systems, as well as reusable helpers for logging and performance tracking.

---

## 📦 Components

### 🔹 `HDFSUtils`
Utilities for interacting with HDFS via PySpark:
- List, rename, move, and delete files and directories
- Read and write XML/JSON to/from HDFS
- Save bytes or strings as files
- Read PySpark schemas from HDFS

### 🔹 `HiveUtils`
Simplifies interactions with Hive metastore:
- Create Hive tables (with or without partitions)
- Insert data conditionally or with overwrite
- Check for table existence
- Drop partitions dynamically

### 🔹 `MSSQLConnector`
Lightweight connector for SQL Server:
- Execute queries (DDL, DML, SELECT)
- Check for table existence
- Insert data from Python dictionaries
- Parameterized query support

### 🔹 `Logger`
Custom logger using PySpark's Log4j with optional Jupyter support:
- Supports `INFO`, `WARNING`, `ERROR` levels
- Automatically adapts to notebook vs. cluster environments

### 🔹 `Timer`
Simple utility to measure and format elapsed execution time in human-readable format:
- Track total or step-by-step durations
- Returns string like `0h1m10s245000ms`

---

## ⚙️ Requirements

- Python 3.7+
- PySpark (tested on Spark 3.x)
- Hadoop with HDFS access
- Hive metastore configured (for `HiveUtils`)
- pymssql (for MSSQL support)

Install Python dependencies:
```bash
pip install pymssql
