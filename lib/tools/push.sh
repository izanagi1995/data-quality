#!/bin/bash
set -e

# AI-NOTE: Script to upload wheel package to Databricks workspace using streaming upload API
# Uses dbfs/create, dbfs/add-block, dbfs/close for files larger than 1MB

echo "📤 Uploading data-qa-lib wheel to Databricks..."

# Change to lib directory (script location)
cd "$(dirname "$0")/.."

# Load environment variables
if [ ! -f ".env" ]; then
    echo "❌ .env file not found in lib directory"
    echo "Please create lib/.env with DATABRICKS_API_KEY and DATABRICKS_HOST"
    exit 1
fi

# Source environment variables
source .env

# Check required environment variables
if [ -z "$DATABRICKS_API_KEY" ]; then
    echo "❌ DATABRICKS_API_KEY not set in .env file"
    exit 1
fi

if [ -z "$DATABRICKS_HOST" ]; then
    echo "❌ DATABRICKS_HOST not set in .env file (e.g., https://your-workspace.cloud.databricks.com)"
    exit 1
fi

# Find the wheel file
WHEEL_FILE=$(ls dist/*.whl 2>/dev/null | head -1)
if [ -z "$WHEEL_FILE" ]; then
    echo "❌ No wheel file found in dist/ directory"
    echo "Run ./tools/build.sh first to build the package"
    exit 1
fi

WHEEL_NAME=$(basename "$WHEEL_FILE")
DBFS_PATH="/FileStore/wheels/$WHEEL_NAME"

echo "📁 Uploading $WHEEL_NAME to DBFS path: $DBFS_PATH"

# Step 1: Create file handle
echo "🔧 Creating file handle..."
HANDLE_RESPONSE=$(curl -s -X POST \
    "$DATABRICKS_HOST/api/2.0/dbfs/create" \
    -H "Authorization: Bearer $DATABRICKS_API_KEY" \
    -H "Content-Type: application/json" \
    -d "{
        \"path\": \"$DBFS_PATH\",
        \"overwrite\": true
    }")

HANDLE=$(echo "$HANDLE_RESPONSE" | grep -o '"handle":[0-9]*' | cut -d':' -f2)
if [ -z "$HANDLE" ]; then
    echo "❌ Failed to create file handle: $HANDLE_RESPONSE"
    exit 1
fi

echo "📝 File handle created: $HANDLE"

# Step 2: Upload file in chunks using streaming API
echo "📦 Uploading file in chunks..."
CHUNK_SIZE=1048576  # 1MB chunks

# Read file and upload in chunks
OFFSET=0
while IFS= read -r -n$CHUNK_SIZE CHUNK; do
    if [ -n "$CHUNK" ]; then
        ENCODED_CHUNK=$(echo -n "$CHUNK" | base64 | tr -d '\n')
        
        curl -s -X POST \
            "$DATABRICKS_HOST/api/2.0/dbfs/add-block" \
            -H "Authorization: Bearer $DATABRICKS_API_KEY" \
            -H "Content-Type: application/json" \
            -d "{
                \"handle\": $HANDLE,
                \"data\": \"$ENCODED_CHUNK\"
            }" > /dev/null
        
        OFFSET=$((OFFSET + ${#CHUNK}))
        echo "  Uploaded $OFFSET bytes..."
    fi
done < "$WHEEL_FILE"

# Step 3: Close the file
echo "🔒 Closing file..."
curl -s -X POST \
    "$DATABRICKS_HOST/api/2.0/dbfs/close" \
    -H "Authorization: Bearer $DATABRICKS_API_KEY" \
    -H "Content-Type: application/json" \
    -d "{
        \"handle\": $HANDLE
    }" > /dev/null

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Upload completed successfully!"
    echo ""
    echo "📋 Installation instructions:"
    echo "1. In your Databricks notebook, run:"
    echo "   %pip install /dbfs$DBFS_PATH"
    echo ""
    echo "2. Or add to cluster libraries:"
    echo "   - Go to your cluster configuration"
    echo "   - Add library: Python Whl"
    echo "   - Path: dbfs:$DBFS_PATH"
    echo ""
    echo "3. Import in your code:"
    echo "   from data_qa_lib import DataQAClient, TableMetrics"
else
    echo "❌ Upload failed. Check your API key and host configuration."
    exit 1
fi