#!/bin/zsh

set -euo pipefail

# Always run from project root so relative paths behave consistently
cd "/Users/haleybridges/Desktop/carolina-compliance-v1"

# Activate virtual environment
source "/Users/haleybridges/Desktop/carolina-compliance-v1/venv/bin/activate"

# Run full COI pipeline
echo "Starting email step..."
if ! python3 email_monitor.py; then
    echo "Email step failed — stopping pipeline"
    exit 1
fi
echo "Email step complete"

echo "Starting extraction step..."
if ! python3 extractor.py; then
    echo "Extractor failed — stopping pipeline"
    exit 1
fi
echo "Extraction complete"

echo "Starting import step..."
if ! python3 airtable_importer.py; then
    echo "Import step failed — stopping pipeline"
    exit 1
fi
echo "Import complete"

echo "Starting processing step..."
if ! python3 processor.py; then
    echo "Processing step failed — stopping pipeline"
    exit 1
fi
echo "Processing complete"

echo "Pipeline complete"