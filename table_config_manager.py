#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Table Configuration Manager for JSON to SQL processing.
Simple hardcoded mappings between JSON files and their YAML configurations.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from typing import Dict, Any, Optional


class TableConfigManager:
    """
    Simple configuration manager with hardcoded JSON-to-YAML mappings.
    """

    # Hardcoded mappings between JSON files and their YAML configuration files
    CONFIG_MAPPINGS = {
        "workspaces.json": "workspaces.yaml",
        # "rule_groups.json": "rule_groups.yaml",
        "action_buttons.json": "action_buttons_ibpl_rules.yaml",
        # "modules.json": "modules.yaml",
        # "measures.json": "measures.yaml",
        # "rule_group_labels.json": "rule_group_labels.yaml",
        # "measure_groups.json": "measure_groups.yaml",
        # "plans.json": "plans.yaml",
        # "dimensions.json": "dimensions.yaml",
        # "dimension_attributes.json": "dimension_attributes.yaml",
        # "configured_global_plugins.json": "configured_global_plugins.yaml"
    }

    def __init__(self):
        """Initialize the configuration manager."""
        # Use configs folder relative to this script
        self.config_dir = Path(__file__).parent / "configs"
        self._loaded_configs: Dict[str, Dict[str, Any]] = {}

    def get_config(self, json_filename: str) -> Optional[Dict[str, Any]]:
        """
        Get the parsed YAML configuration for a JSON file.

        Args:
            json_filename: Name of the JSON file

        Returns:
            Parsed YAML configuration or None if not found
        """
        # Check if already loaded in cache
        if json_filename in self._loaded_configs:
            return self._loaded_configs[json_filename]

        # Check if mapping exists
        if json_filename not in self.CONFIG_MAPPINGS:
            return None

        # Get the yaml filename from mapping
        yaml_filename = self.CONFIG_MAPPINGS[json_filename]

        # If mapping is empty string, return None (indicates multi-config should be used)
        if not yaml_filename or yaml_filename.strip() == "":
            return None

        # Load and cache the configuration
        try:
            yaml_path = self.config_dir / yaml_filename

            if not yaml_path.exists():
                print(f"Configuration file not found: {yaml_path}")
                return None

            with open(yaml_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)
                self._loaded_configs[json_filename] = config
                return config
        except Exception as e:
            print(f"Error loading configuration for {json_filename}: {e}")
            return None

    def has_config(self, json_filename: str) -> bool:
        """Check if a configuration exists for the given JSON file."""
        return json_filename in self.CONFIG_MAPPINGS

    def list_supported_files(self) -> list[str]:
        """Get a list of all supported JSON file names."""
        return list(self.CONFIG_MAPPINGS.keys())
