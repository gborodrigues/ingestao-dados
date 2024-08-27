#!/bin/bash

# Initialize DBT project if it doesn't exist
if [ ! -d "/usr/app/dbt_project" ]; then
    dbt init dbt_project
    cd dbt_project
    
    # Configure profiles.yml
    cat << EOF > profiles.yml
dbt_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /usr/app/output.db
EOF

    # Create a sample model
    mkdir -p models/example
    cat << EOF > models/example/my_first_dbt_model.sql
-- Replace this with your actual query
SELECT *
FROM your_table
WHERE some_condition
EOF

else
    cd dbt_project
fi

# Run DBT
dbt run

# Run the Python script to send results to MySQL
python /usr/app/send_to_mysql.py