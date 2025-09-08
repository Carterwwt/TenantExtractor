import json
from pathlib import Path
from typing import Dict, Any, List, Optional

class ConfigManager:
    """Simple configuration manager using JSON format."""

    def __init__(self):
        self.config_file = Path(__file__).parent / "tenant_config.json"
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        try:
            if self.config_file.exists():
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                # Create default config file
                default_config = {
                    "json_files": {
                        "workspaces.json": {
                            "table": "workspaces",
                            "fields": ["WorkspaceId", "Title", "Name"],
                            "children": {
                                "PageGroups": {
                                    "table": "page_groups",
                                    "fields": ["Id", "Title", "Name"],
                                    "parent_id": "WorkspaceId"
                                }
                            }
                        }
                    }
                }
                with open(self.config_file, 'w', encoding='utf-8') as f:
                    json.dump(default_config, f, indent=2)
                return default_config
        except Exception as e:
            print(f"Error loading config: {e}")
            return {"json_files": {}}

    def get_config(self, json_filename: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a JSON file."""
        return self.config.get("json_files", {}).get(json_filename)

    def get_supported_files(self) -> List[str]:
        """Get list of supported JSON files."""
        return list(self.config.get("json_files", {}).keys())
