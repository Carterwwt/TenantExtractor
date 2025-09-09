import sqlite3
from typing import Dict, Any, List, Union

class SimpleJsonProcessor:
    """Simple JSON to SQL processor."""

    def __init__(self, db_connection: sqlite3.Connection):
        self.conn = db_connection
        self.cursor = db_connection.cursor()

    def _table_exists(self, table_name: str) -> bool:
        """Check if table exists in database."""
        self.cursor.execute("""
                            SELECT name 
                            FROM sqlite_master 
                            WHERE type = 'table' 
                              AND name = ?;""", (table_name,))
        return self.cursor.fetchone() is not None

    def _create_table(self, table_name: str, fields: List[str], parent_id: str = None):
        """Create a table with the specified fields."""
        if self._table_exists(table_name):
            return

        columns = [f"{field} TEXT" for field in fields]
        if parent_id and parent_id not in fields:
            columns.append(f"{parent_id} TEXT")

        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
        self.cursor.execute(sql)
        print(f"Created table: {table_name}")
        
    def _alter_table(self, table_name: str, fields: List[str]):
        """Add new fields to existing table."""
        # Get existing columns
        self.cursor.execute(f"PRAGMA table_info({table_name})")
        existing_columns = {row[1] for row in self.cursor.fetchall()}

        # Add new columns if they don't exist
        for field in fields:
            if field not in existing_columns:
                sql = f"ALTER TABLE {table_name} ADD COLUMN {field} TEXT"
                self.cursor.execute(sql)
                print(f"Added column {field} to table {table_name}")

    def _insert_data(self, table_name: str, fields: List[str], data: Dict[str, Any], parent_id: str = None):
        """Insert data into table."""
        values = {}

        if parent_id:
            fields.append(parent_id)

        # Add regular fields
        for field in fields:
            values[field] = data.get(field, "")

        if values:
            columns = list(values.keys())
            placeholders = ', '.join(['?' for _ in columns])
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            self.cursor.execute(sql, list(values.values()))
        

    def process_data(self, config: Union[Dict[str, Any], List[Dict[str, Any]]], json_data: Union[Dict, List]):
        """Process JSON data according to configuration(s)."""
        # Handle multiple schema definitions for the same file
        if isinstance(config, list):
            for schema_config in config:
                self._process_single_schema(schema_config, json_data)
        else:
            self._process_single_schema(config, json_data)

    def _process_single_schema(self, config: Dict[str, Any], json_data: Union[Dict, List]):
        """Process JSON data according to a single configuration schema."""
        table_name = config["table"]
        fields = config.get("fields", [])
        children = config.get("children", {})
        parent_id = config.get("parent_id")

        # Create table
        self._create_table(table_name, fields, parent_id)

        # Handle list of items
        if isinstance(json_data, list):
            for item in json_data:
                if isinstance(item, dict):
                    self._process_single_item(table_name, fields, children, item, parent_id)

        # Handle single item
        elif isinstance(json_data, dict):
            self._process_single_item(table_name, fields, children, json_data, parent_id)

    def _process_single_item(self, table_name: str, fields: List[str], children: Dict[str, Any], 
                           data: Dict[str, Any], parent_id: str):
        """Process a single data item."""

        # Process children
        for child_key, child_config in children.items():
            child_data = self._find_child_data(data, child_key)

            if child_data is not None:
                if child_config.get("parent_id"):
                    self._insert_data(table_name, fields, data, parent_id)
                    # Child has parent relationship - create separate table
                    # Recursively process child data
                    self.process_data(child_config, child_data)
                else:
                    # No parent relationship - flatten into child's own table
                    self._flatten_child_data(child_key, child_config, child_data, data, fields)
            else:
                self._insert_data(table_name, fields, data, parent_id)

    def _find_child_data(self, data: Dict[str, Any], child_key: str):
        """Find child data in JSON, checking common nested locations."""
        # Direct child
        keys = child_key.split(".")
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current

    def _flatten_child_data(self, child_key: str, child_config: Dict[str, Any], child_data: Union[Dict, List],
                            parent_data: Dict[str, Any], parent_fields: List[str]):
        """Flatten child data into a new table when no parent_id is defined."""
        parent_table = child_config["table"]
        child_fields = child_config.get("fields", [])
        nested_children = child_config.get("children", {})

        # Sanitize child_key for use in SQL column names
        sanitized_child_key = child_key.replace(".", "_").replace("-", "_").replace(" ", "_")

        # Collect all fields including nested ones
        all_flattened_fields = []

        if isinstance(child_data, list):
            for item in child_data:
                if isinstance(item, dict):
                    # Start with parent data
                    flattened = {**parent_data}
                    current_fields = list(parent_fields)

                    # Add current level fields
                    for field in child_fields:
                        prefixed_field = f"{sanitized_child_key}_{field}"
                        flattened[prefixed_field] = item.get(field, "")
                        if prefixed_field not in current_fields:
                            current_fields.append(prefixed_field)

                    # Recursively flatten nested children
                    self._flatten_nested_data(item, nested_children, flattened, current_fields, sanitized_child_key)

                    # Ensure table has all necessary columns
                    self._alter_table(parent_table, current_fields)

                    # Insert the fully flattened row
                    self._insert_data(parent_table, current_fields, flattened)

        elif isinstance(child_data, dict):
            # Start with parent data
            flattened = {**parent_data}
            current_fields = list(parent_fields)

            # Add current level fields
            for field in child_fields:
                prefixed_field = f"{sanitized_child_key}_{field}"
                flattened[prefixed_field] = child_data.get(field, "")
                if prefixed_field not in current_fields:
                    current_fields.append(prefixed_field)

            # Recursively flatten nested children
            self._flatten_nested_data(child_data, nested_children, flattened, current_fields, sanitized_child_key)

            # Ensure table has all necessary columns
            self._alter_table(parent_table, current_fields)

            # Insert the fully flattened row
            self._insert_data(parent_table, current_fields, flattened)

    def _flatten_nested_data(self, data: Dict[str, Any], nested_children: Dict[str, Any], 
                           flattened: Dict[str, Any], current_fields: List[str], parent_prefix: str):
        """Recursively flatten nested children data into the same record."""
        for nested_key, nested_config in nested_children.items():
            nested_data = self._find_child_data(data, nested_key)
            if nested_data is not None:
                nested_fields = nested_config.get("fields", [])
                sanitized_nested_key = f"{parent_prefix}_{nested_key}".replace(".", "_").replace("-", "_").replace(" ", "_")

                if isinstance(nested_data, dict):
                    # Add nested fields to the same flattened record
                    for field in nested_fields:
                        prefixed_field = f"{sanitized_nested_key}_{field}"
                        flattened[prefixed_field] = nested_data.get(field, "")
                        if prefixed_field not in current_fields:
                            current_fields.append(prefixed_field)

                    # Continue recursively if there are deeper nested children
                    deeper_children = nested_config.get("children", {})
                    if deeper_children:
                        self._flatten_nested_data(nested_data, deeper_children, flattened, current_fields, sanitized_nested_key)

                elif isinstance(nested_data, list):
                    # For lists, we'll take the first item for now (could be enhanced to handle multiple items)
                    for item in nested_data:
                        if isinstance(item, dict):
                            for field in nested_fields:
                                prefixed_field = f"{sanitized_nested_key}_{field}"
                                flattened[prefixed_field] = item.get(field, "")
                                if prefixed_field not in current_fields:
                                    current_fields.append(prefixed_field)

                            # Continue recursively if there are deeper nested children
                            deeper_children = nested_config.get("children", {})
                            if deeper_children:
                                self._flatten_nested_data(item, deeper_children, flattened, current_fields, sanitized_nested_key)
                            break  # Only process first item for now
