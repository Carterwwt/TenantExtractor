import sqlite3
import json
import zipfile
import tempfile
import shutil
import sys
from pathlib import Path
from tkinter import Tk, filedialog, messagebox
from config_manager import ConfigManager
from simple_json_processor import SimpleJsonProcessor

# Fix dialog blurriness on high DPI displays
try:
    from ctypes import windll
    windll.shcore.SetProcessDpiAwareness(1)
except:
    pass

def select_zip_file() -> Path:
    """Let user select a ZIP file"""
    root = Tk()
    root.tk.call('tk', 'scaling', 1.0)
    root.attributes('-topmost', True)
    root.lift()
    root.focus_force()
    root.withdraw()

    zip_path = filedialog.askopenfilename(
        title="Select ZIP file to process",
        filetypes=[("ZIP files", "*.zip"), ("All files", "*.*")]
    )
    root.destroy()

    if not zip_path:
        print("No ZIP file selected. Exiting...")
        sys.exit(0)
    return Path(zip_path)

def select_output_db() -> Path:
    """Let user select output database file path"""
    root = Tk()
    root.tk.call('tk', 'scaling', 1.0)
    root.attributes('-topmost', True)
    root.lift()
    root.focus_force()
    root.withdraw()

    db_path = filedialog.asksaveasfilename(
        title="Select output database file location",
        defaultextension=".db",
        filetypes=[("SQLite Database", "*.db"), ("All files", "*.*")]
    )
    root.destroy()

    if not db_path:
        print("No output database file selected. Exiting...")
        sys.exit(0)
    return Path(db_path)

def extract_zip_to_temp(zip_path: Path) -> Path:
    """Extract ZIP file to temporary directory"""
    temp_dir = Path(tempfile.mkdtemp())
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        return temp_dir
    except Exception as e:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise Exception(f"Failed to extract ZIP file: {e}")

def find_json_files(extract_dir: Path, supported_files: list[str]) -> list[Path]:
    """Find supported JSON files in the extracted directory"""
    found_files = []
    for json_filename in supported_files:
        json_path = extract_dir / json_filename
        if json_path.exists():
            found_files.append(json_path)
            print(f"Found file: {json_filename}")
        else:
            print(f"File not found: {json_filename}")
    return found_files

def main():
    try:
        # GUI file selection
        print("Please select ZIP file to process...")
        zip_path = select_zip_file()
        print(f"Selected ZIP file: {zip_path}")

        print("Please select output database file location...")
        db_path = select_output_db()
        print(f"Output database: {db_path}")

        # Delete existing database if it exists
        if db_path.exists():
            print(f"Deleting existing database: {db_path}")
            db_path.unlink()

        # Load configuration
        config_manager = ConfigManager()

        # Extract ZIP file
        print("Extracting ZIP file...")
        extract_dir = extract_zip_to_temp(zip_path)

        try:
            # Find supported JSON files
            print("Searching for JSON files...")
            json_files = find_json_files(extract_dir, config_manager.get_supported_files())

            if not json_files:
                print("No supported JSON files found")
                return

            print(f"Found {len(json_files)} JSON files, starting processing...")

            # Process files
            conn = sqlite3.connect(str(db_path))
            processor = SimpleJsonProcessor(conn)

            try:
                for json_path in json_files:
                    json_filename = json_path.name
                    config = config_manager.get_config(json_filename)

                    if config is None:
                        print(f"Skipping {json_filename} - no configuration found")
                        continue

                    print(f"Processing {json_filename}...")

                    # Load JSON data
                    with open(json_path, "r", encoding="utf-8") as f:
                        data = json.load(f)

                    # Process data
                    processor.process_data(config, data)
                    print(f"Completed processing: {json_filename}")

                # Commit changes
                conn.commit()
                print(f"All data successfully imported to database: {db_path}")

                # Show success message
                root = Tk()
                root.tk.call('tk', 'scaling', 1.0)
                root.attributes('-topmost', True)
                root.lift()
                root.focus_force()
                root.withdraw()
                messagebox.showinfo("Processing Complete", 
                                  f"Successfully processed {len(json_files)} files\nDatabase saved to: {db_path}")
                root.destroy()

            except Exception as e:
                print(f"Error during processing: {e}")
                conn.rollback()
            finally:
                conn.close()

        finally:
            # Clean up
            shutil.rmtree(extract_dir, ignore_errors=True)
            print("Cleaned up temporary files")

    except Exception as e:
        print(f"Program execution failed: {e}")
        root = Tk()
        root.tk.call('tk', 'scaling', 1.0)
        root.attributes('-topmost', True)
        root.lift()
        root.focus_force()
        root.withdraw()
        messagebox.showerror("Error", f"Program execution failed: {e}")
        root.destroy()

if __name__ == "__main__":
    main()
