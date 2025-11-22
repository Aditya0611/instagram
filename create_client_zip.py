"""
Script to create a zip file for client delivery.
Excludes cache files, logs, virtual environments, and existing zip files.
"""
import os
import zipfile
from pathlib import Path
from datetime import datetime

def create_client_zip():
    """Create a zip file with all necessary files for client delivery."""
    
    # Get the project root directory
    project_root = Path(__file__).parent
    
    # Define the zip file name with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = f"instagram_client_delivery_{timestamp}.zip"
    
    # Files and directories to exclude
    exclude_patterns = [
        '__pycache__',
        '.git',
        'vrnv',
        '*.log',
        '*.zip',
        '.github',
        'create_client_zip.py',  # Exclude this script itself
    ]
    
    # Files to explicitly include
    files_to_include = [
        'main.py',
        'main copy.py',
        'requirements.txt',
        'README.md',
        'CLIENT_DOCUMENTATION.md',
        'CODE_IMPROVEMENTS.md',
    ]
    
    def should_exclude(file_path):
        """Check if a file should be excluded from the zip."""
        path_str = str(file_path)
        
        # Check against exclude patterns
        for pattern in exclude_patterns:
            if pattern.startswith('*'):
                # Pattern like *.log
                if path_str.endswith(pattern[1:]):
                    return True
            elif pattern in path_str:
                return True
        
        return False
    
    # Create the zip file
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Add files from the files_to_include list
        for file_name in files_to_include:
            file_path = project_root / file_name
            if file_path.exists():
                zipf.write(file_path, file_name)
                print(f"✓ Added: {file_name}")
            else:
                print(f"⚠ Skipped (not found): {file_name}")
        
        # Also add any other Python files in root (excluding this script)
        for file_path in project_root.glob('*.py'):
            if not should_exclude(file_path) and file_path.name != 'create_client_zip.py':
                if file_path.name not in files_to_include:
                    zipf.write(file_path, file_path.name)
                    print(f"✓ Added: {file_path.name}")
        
        # Add any other markdown files
        for file_path in project_root.glob('*.md'):
            if file_path.name not in files_to_include:
                zipf.write(file_path, file_path.name)
                print(f"✓ Added: {file_path.name}")
    
    zip_path = project_root / zip_filename
    file_size_mb = zip_path.stat().st_size / (1024 * 1024)
    
    print(f"\n{'='*60}")
    print(f"✅ Zip file created successfully!")
    print(f"📦 File: {zip_filename}")
    print(f"📊 Size: {file_size_mb:.2f} MB")
    print(f"📁 Location: {zip_path}")
    print(f"{'='*60}")
    
    return zip_filename

if __name__ == "__main__":
    create_client_zip()

