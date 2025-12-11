#!/bin/bash
#
# verify_linux_compatibility.sh - Verify that all files are Linux compatible
#

set -e

echo "=================================================="
echo "✅ Linux Compatibility Verification"
echo "=================================================="
echo ""

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

issues_found=0

# Function to check for Windows path references
check_windows_paths() {
    echo "🔍 Checking for Windows path references..."
    
    files_with_windows_paths=$(grep -r "C:\\" . --include="*.py" --include="*.sh" --include=".env" 2>/dev/null || true)
    if [ ! -z "$files_with_windows_paths" ]; then
        echo "⚠️  Found Windows paths:"
        echo "$files_with_windows_paths"
        issues_found=$((issues_found+1))
    else
        echo "✅ No Windows paths found"
    fi
}

# Function to check for localhost instead of container names
check_localhost_references() {
    echo ""
    echo "🔍 Checking for localhost references in Python files..."
    
    files_with_localhost=$(grep -r "localhost" Producer/*.py Consumer/*.py GUI_Dashboard/backend/*.py 2>/dev/null || true)
    if [ ! -z "$files_with_localhost" ]; then
        echo "⚠️  Found localhost references (these should use container names for Docker):"
        echo "$files_with_localhost"
        issues_found=$((issues_found+1))
    else
        echo "✅ No problematic localhost references found"
    fi
}

# Function to check shell scripts
check_shell_scripts() {
    echo ""
    echo "🔍 Checking shell scripts..."
    
    for script in start_*.sh stop_*.sh; do
        if [ -f "$script" ]; then
            if [ -x "$script" ]; then
                echo "✅ $script is executable"
            else
                echo "⚠️  $script is NOT executable"
                issues_found=$((issues_found+1))
            fi
        fi
    done
}

# Function to check environment variables
check_env_variables() {
    echo ""
    echo "🔍 Checking .env file..."
    
    if [ ! -f ".env" ]; then
        echo "❌ .env file not found"
        issues_found=$((issues_found+1))
    else
        echo "✅ .env file exists"
        
        # Check for required variables
        required_vars=("KAFKA_BOOTSTRAP_SERVERS" "POSTGRES_HOST" "PARQUET_BASE_PATH" "CHECKPOINT_BASE")
        for var in "${required_vars[@]}"; do
            if grep -q "^$var=" .env; then
                echo "✅ $var is defined"
            else
                echo "⚠️  $var is not defined"
                issues_found=$((issues_found+1))
            fi
        done
    fi
}

# Function to check Python imports
check_python_imports() {
    echo ""
    echo "🔍 Checking Python imports..."
    
    python_files=("Producer/IotSystem_Version1.1.py" "Consumer/Spark_Transformation_v1.1.py" "GUI_Dashboard/backend/main.py")
    
    for file in "${python_files[@]}"; do
        if [ -f "$file" ]; then
            if grep -q "import os" "$file"; then
                echo "✅ $file has 'import os'"
            else
                echo "⚠️  $file might be missing 'import os'"
            fi
        fi
    done
}

# Run all checks
check_windows_paths
check_localhost_references
check_shell_scripts
check_env_variables
check_python_imports

# Final report
echo ""
echo "=================================================="
if [ $issues_found -eq 0 ]; then
    echo "✅ All checks passed! System is Linux compatible."
    echo "=================================================="
    exit 0
else
    echo "⚠️  Found $issues_found potential issues."
    echo "Please review the warnings above."
    echo "=================================================="
    exit 1
fi
