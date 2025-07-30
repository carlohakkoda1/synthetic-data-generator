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

Before you can run the Synthetic Data Generator, make sure you have the following in place:

1. **Python**  
   - Version **3.8** or newer  
   - Verify with:  
     ```bash
     python --version
     ```

2. **Dependencies**  
   - All third-party packages are listed in `requirements.txt`.  
   - Install them with:
     ```bash
     pip install -r requirements.txt
     ```  

3. **Project Files & Folders**  
   Ensure your project root contains the following structure and files:

```
.
├── config/
│   ├── table_config.xlsx
│   └── table_config_copy.xlsx
├── core/
│   ├── data_generator.py
│   ├── domain_utils.py
│   ├── foreign_key_util.py
│   └── schema_loader.py
├── definitions/
│   ├── base_rules.py
│   ├── customer.xlsx
│   ├── employee.xlsx
│   ├── equipment.xlsx
│   ├── material.xlsx
│   ├── vendor.xlsx
│   └── custom_rules/
│       ├── customer_rules.py
│       ├── employee_rules.py
│       ├── equipment_rules.py
│       ├── material_rules.py
│       └── vendor_rules.py
├── generators/
│   ├── materials_rules.py
│   ├── equipment_rules.py
│   └── …  
├── resources/
│   ├── address_data.json
│   ├── communication_records.json
│   ├── employee_addresses_data.json
│   ├── equipment_descriptions.json
│   ├── person_info.json
│   └── product_descriptions.json
├── main.py
└── requirements.txt
```
## 4. Installation

Follow these steps to get the Synthetic Data Generator up and running:

1. **Clone the repository**  
   ```bash
   git clone https://github.com/your-org/synthetic-data-generator.git
   cd synthetic-data-generator

2. **Create & activate a virtual environment**
   ```
   python3 -m venv .venv
   source .venv/bin/activate     # macOS/Linux
   .venv\Scripts\Activate.ps1    # Windows PowerShell
  
4. **Install dependencies**
  ```
  pip install -r requirements.txt

