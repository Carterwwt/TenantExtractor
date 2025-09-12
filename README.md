# JSON to Database Converter

A Python tool that extracts JSON files from ZIP archives and converts them into relational databases (SQLite and MySQL). This tool is particularly useful for processing structured JSON data and creating queryable database tables with proper relationships.

## Features

- **GUI Interface**: User-friendly file selection dialogs
- **ZIP File Processing**: Automatically extracts and processes JSON files from ZIP archives
- **Flexible Configuration**: JSON-based configuration system for defining table schemas
- **SQLite Export**: Creates local SQLite databases for immediate use
- **MySQL Export**: Optional export to MySQL databases for production environments
- **Relationship Management**: Handles parent-child relationships and data flattening
- **Module ID Transformation**: Automatically converts Module IDs to readable Module Names
- **Batch Processing**: Efficiently processes large datasets

## Prerequisites

- Python 3.13.3 or higher
- Virtual environment (recommended)

## Installation

1. **Clone or download the project**
   ```bash
   git clone <repository-url>
   cd json-to-database-converter
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv .venv

   # On Windows
   .venv\Scripts\activate

   # On macOS/Linux
   source .venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Basic Usage

1. **Run the application**
   ```bash
   python main.py
   ```

2. **Select ZIP file**
   - A file dialog will open
   - Select the ZIP file containing your JSON data files

3. **Choose output location**
   - Select where you want to save the SQLite database file
   - The file will be created with a `.db` extension

4. **Processing**
   - The tool will automatically extract JSON files from the ZIP
   - Process them according to the configuration
   - Create database tables and import data

5. **MySQL Export (Optional)**
   - After SQLite processing completes, you'll be asked if you want to export to MySQL
   - If yes, provide MySQL connection details:
     - Host (default: localhost)
     - Port (default: 3306)
     - Database name
     - Username
     - Password

### Configuration

The tool uses JSON configuration files to define how JSON data should be mapped to database tables. Configuration files should be placed in the project directory.

#### Configuration Structure

```json
{
  "table": "table_name",
  "fields": ["field1", "field2", "field3"],
  "children": {
    "child_key": {
      "table": "child_table",
      "fields": ["child_field1", "child_field2"],
      "parent_id": "parent_id_column"
    }
  },
  "filters": {
    "field_name": "required_value"
  }
}
