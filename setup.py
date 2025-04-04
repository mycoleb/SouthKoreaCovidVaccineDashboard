#!/usr/bin/env python
"""
Setup script for South Korea COVID-19 Vaccination Analysis.
This script ensures all necessary directories exist and checks for dependencies.
"""

import os
import sys
import subprocess
from pathlib import Path

def check_python_version():
    """Check if Python version is 3.8 or higher."""
    required_version = (3, 8)
    current_version = sys.version_info
    
    if current_version < required_version:
        print(f"Error: Python {required_version[0]}.{required_version[1]} or higher is required.")
        print(f"Current Python version: {current_version[0]}.{current_version[1]}.{current_version[2]}")
        return False
    
    print(f"Python version check passed: {current_version[0]}.{current_version[1]}.{current_version[2]}")
    return True

def check_and_install_dependencies():
    """Check and install required packages."""
    print("Checking and installing dependencies...")
    
    try:
        # Check if pip is installed
        subprocess.check_call([sys.executable, "-m", "pip", "--version"])
        
        # Install required packages
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("Dependencies installed successfully.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error installing dependencies: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def create_directories():
    """Create necessary directories for the project."""
    print("Creating necessary directories...")
    
    directories = [
        "data",
        "data/raw",
        "data/processed",
        "output",
        "logs"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"Created directory: {directory}")
    
    return True

def main():
    """Main function to set up the project."""
    print("Setting up South Korea COVID-19 Vaccination Analysis project...")
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Create directories
    if not create_directories():
        sys.exit(1)
    
    # Install dependencies
    if not check_and_install_dependencies():
        sys.exit(1)
    
    print("\nSetup completed successfully!")
    print("You can now run the analysis with: python main.py")

if __name__ == "__main__":
    main()