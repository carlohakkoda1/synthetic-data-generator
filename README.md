# Synthetic Data Generator

Utilities to generate realistic synthetic data for SAP S/4HANA migration mock-loads across multiple domains.

## Table of Contents

1. [Overview](#overview)  
2. [Features](#features)  
3. [Prerequisites](#prerequisites)  
4. [Installation](#installation)  

---

## 1. Overview

This library generates realistic synthetic data for SAP S/4HANA migration mock-loads across domains like Materials, Equipment, Vendors, Customers, and Employees. It streamlines data creation and exports CSV files for testing and development.

## 2. Features

- **Config Folder** (`config/`):  
  - `table_config.xlsx` (and copy): Defines which tables to generate, row counts, and creation order.  
- **Definitions Folder** (`definitions/`):  
  - One Excel file per domain (materials.xlsx, equipment.xlsx, …).  
  - Lists tables, columns, “fake” rule names, and descriptions for each domain.  
  - Contains `custom_rules/` subfolder with domain rule modules (e.g. `materials_rules.py`).  
- **Generators** (`core/data_generator.py` & `generators/`):  
  - Domain-specific functions implementing logic referenced by definitions.  
- **Foreign-Key Utilities** (`core/foreign_key_util.py`):  
  - `get_foreign_values()` to build and resolve referential links across extracts.  
- **Chunked CSV Export** (`core/schema_loader.py`):  
  - `generate_table_chunked()` writes large datasets into manageable CSV chunks.  
- **Multi-Domain Support**:  
  - Covers Materials, Equipment, Vendors, Customers, and Employees in one codebase.  
- **CSV Output**:  
  - Writes all generated tables into a configurable `output/` directory.

## 3. Prerequisites

1. **Python**  
   - Version **3.8** or newer  

2. **Python Packages**  
   Install all dependencies with:
   ```bash
   pip install -r requirements.txt
