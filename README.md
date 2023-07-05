# Sales Data Analysis With PySpark

This project leverages PySpark to perform sales data analysis. It reads data from a CSV file, performs some transformations, and saves the output as CSV files.

## Project Structure

```bash
├── data 
    └── sales_data.csv 
├── output 
    ├── sales_data
    ├── total_revenue_by_store
    └── total_revenue_by_salesperson 
└── src 
    └── pipeline.py
```

- The `data` directory contains the source CSV data file `sales_data.csv`.
- The `output` directory is where the transformed data will be written to.
- The `src` directory contains the PySpark script `pipeline.py`.

## Getting Started

To get started, you will need to have Python and PySpark installed on your machine.

1. Clone this repository.
2. Install the necessary Python libraries.
3. Run the `pipeline.py` script.

## Data Analysis Steps

The `pipeline.py` script does the following:

1. Loads the sales data from a CSV file.
2. Converts the 'date' column from string to date type.
3. Creates a new column 'revenue' which is 'quantity' * 'price'.
4. Aggregates the total revenue by store and salesperson.
5. Writes the transformed DataFrames as CSV files to the `output` directory.
