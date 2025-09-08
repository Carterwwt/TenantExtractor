import sqlite3
from typing import Dict, Any, List, Union

class SimpleJsonProcessor:
    """Simple JSON to SQL processor."""

    def __init__(self, db_connection: sqlite3.Connection):
        self.conn = db_connection
        self.cursor = db_connection.cursor()

    def create_table(self, table_name: str, fields: List[str], parent_id: str = None):
        """Create a table with the specified fields."""
        columns = [f"{field} TEXT" for field in fields]
        if parent_id and parent_id not in fields:
            columns.append(f"{parent_id} TEXT")

        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
        self.cursor.execute(sql)
        print(f"Created table: {table_name}")

    def insert_data(self, table_name: str, fields: List[str], data: Dict[str, Any], parent_value: str = None, parent_id: str = None):
        """Insert data into table."""
        values = {}

        # Add regular fields
        for field in fields:
            values[field] = data.get(field)

        # Add parent relationship if specified
        if parent_id and parent_value is not None:
            values[parent_id] = parent_value

        if values:
            columns = list(values.keys())
            placeholders = ', '.join(['?' for _ in columns])
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            self.cursor.execute(sql, list(values.values()))

    def process_data(self, config: Dict[str, Any], json_data: Union[Dict, List], parent_value: str = None):
        """Process JSON data according to configuration."""
        table_name = config["table"]
        fields = config.get("fields", [])
        children = config.get("children", {})
        parent_id = config.get("parent_id")

        # Create table
        self.create_table(table_name, fields, parent_id)

        # Handle list of items
        if isinstance(json_data, list):
            for item in json_data:
                if isinstance(item, dict):
                    self._process_single_item(table_name, fields, children, item, parent_value, parent_id)

        # Handle single item
        elif isinstance(json_data, dict):
            self._process_single_item(table_name, fields, children, json_data, parent_value, parent_id)

    def _process_single_item(self, table_name: str, fields: List[str], children: Dict[str, Any], 
                           data: Dict[str, Any], parent_value: str, parent_id: str):
        """Process a single data item."""

        # Insert main record
        self.insert_data(table_name, fields, data, parent_value, parent_id)

        # Get the ID for this record (for child relationships)
        current_id = data.get("Id") or data.get("id")

        # Process children
        for child_key, child_config in children.items():
            child_data = self._find_child_data(data, child_key)

            if child_data is not None:
                if child_config.get("parent_id"):
                    # Child has parent relationship - create separate table
                    self.process_data(child_config, child_data, current_id)
                else:
                    # No parent relationship - flatten into child's own table
                    self._flatten_child_data(child_key, child_config, child_data, data)

    def _find_child_data(self, data: Dict[str, Any], child_key: str):
        """Find child data in JSON, checking common nested locations."""
        # Direct child
        if child_key in data:
            return data[child_key]

        # In ConfigJson (common pattern)
        if "ConfigJson" in data and child_key in data["ConfigJson"]:
            return data["ConfigJson"][child_key]

        return None

    def _flatten_child_data(self, child_key: str, child_config: Dict[str, Any], child_data: Union[Dict, List], parent_data: Dict[str, Any]):
        """Flatten child data into a new table when no parent_id is defined."""
        child_table = child_config["table"]
        child_fields = child_config.get("fields", [])
        parent_fields = list(parent_data.keys())

        # Combine parent and child fields for the flattened table
        all_fields = list(set(parent_fields + child_fields))

        # Create the child table with combined fields
        self.create_table(child_table, all_fields)

        if isinstance(child_data, list):
            for item in child_data:
                if isinstance(item, dict):
                    # Combine parent and child data
                    flattened = {**parent_data, **item}
                    self.insert_data(child_table, all_fields, flattened)

        elif isinstance(child_data, dict):
            # Single child item
            flattened = {**parent_data, **child_data}
            self.insert_data(child_table, all_fields, flattened)
