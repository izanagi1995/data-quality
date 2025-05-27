#!/bin/bash
set -e

# AI-NOTE: Build script for creating distributable wheel using Poetry
# This script builds the data-qa-lib package for Databricks deployment

echo "🔨 Building data-qa-lib wheel package..."

# Change to lib directory (script location)
cd "$(dirname "$0")/.."

# Check if poetry is installed
if ! command -v poetry &> /dev/null; then
    echo "❌ Poetry is not installed. Please install Poetry first:"
    echo "   curl -sSL https://install.python-poetry.org | python3 -"
    exit 1
fi

# Clean previous builds
echo "🧹 Cleaning previous builds..."
rm -rf dist/ build/ *.egg-info/

# Install dependencies if needed
echo "📦 Installing dependencies..."
poetry install --only main

# Build the wheel
echo "🏗️  Building wheel package..."
poetry build

# Show build results
echo "✅ Build completed! Output files:"
ls -la dist/

echo ""
echo "📋 Installation instructions for Databricks:"
echo "1. Upload the .whl file to your Databricks workspace"
echo "2. Install in notebook: %pip install /path/to/$(ls dist/*.whl | head -1 | xargs basename)"
echo "3. Or add to cluster libraries via Databricks UI"