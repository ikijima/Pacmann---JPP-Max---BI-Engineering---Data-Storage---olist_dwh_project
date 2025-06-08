#!/bin/bash

echo "========== Start Orchestration Process =========="

# Set Conda Environment Name
CONDA_ENV="conda_general"

# Activate Conda Environment
conda activate "$CONDA_ENV"

# Set Python script
PYTHON_SCRIPT="C:\\Users\\LENOVO THINKPAD\\Documents\\Github\\Pacmann---Data-Storage_-Warehouse--Mart---Lake\\mentoring 2\\olist-dwh-project\\elt_main.py"

# Run Python Script
python "$PYTHON_SCRIPT"

echo "========== End of Orchestration Process =========="