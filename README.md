# Py Data Transformer Tool

A versatile command-line utility written in pure Python for performing various transformations on structured data files (CSV or JSON). It allows users to define a pipeline of operations to clean, reshape, filter, and augment data, outputting the result to a new file.

## What is this project?

This tool processes raw data from CSV or JSON files, applies a user-defined sequence of transformations, and saves the modified data. It's designed to handle common data manipulation tasks without requiring external libraries like Pandas, showcasing core Python skills.

## Where to use?

This script is useful for:
*   **Data Cleaning:** Filtering out unwanted rows, selecting relevant columns.
*   **Data Preparation:** Reshaping data for analysis or import into other systems (e.g., renaming columns, adding calculated fields).
*   **Data Extraction:** Creating subsets of data based on specific criteria.
*   **Automation:** Integrating into scripts for repeatable data processing tasks.
*   **Learning:** Demonstrates file I/O, CSV/JSON handling, data structures, string manipulation, regular expressions (implicitly, if used in future filters), `argparse` for complex CLIs, `logging`, and designing a modular data processing pipeline.

## Features

*   **Reads and Writes CSV & JSON:** Seamlessly works with both common data formats.
*   **Chained Transformations:** Apply multiple transformations in a specified order from the command line.
*   **Supported Transformations:**
    *   **Select Columns:** Keep only specified columns.
    *   **Filter Rows:** Filter data based on conditions (e.g., `Age > 30`, `Department == Engineering`). Supports common comparison operators and basic string operations like `contains`, `startswith`, `endswith`.
    *   **Rename Columns:** Change the names of one or more columns.
    *   **Sort Data:** Sort data by one or more columns, with ascending or descending order for each.
    *   **Add/Modify Column (Calculated/Literal):** Create new columns or update existing ones based on:
        *   Concatenation of two existing columns (e.g., `FirstName + LastName`).
        *   Simple arithmetic between an existing column and a literal number (e.g., `Salary * 0.1`).
        *   Assignment of a literal string or number.
*   **Automatic Type Conversion (from CSV):** Attempts to convert string values from CSV files that look like numbers into actual integers or floats upon loading.
*   **Command-Line Interface:** All operations are controlled via command-line arguments using `argparse`.
*   **Logging:** Detailed logging of operations, transformations, successes, warnings, and errors to both the console and a `transformer.log` file for auditing and debugging.

## Requirements

*   Python 3.7+ (or adjust for `datetime.timezone.utc` and f-string usage if targeting older versions).
*   Standard Python libraries: `csv`, `json`, `os`, `argparse`, `logging`.

## Setup

1.  **Save the Script:**
    Save the main Python script as `transformer.py`.
2.  **Prepare Input Data:**
    Have your input CSV or JSON files ready. The script infers the input/output file type from the extension (`.csv` or `.json`).

## How to Use

Open your terminal or command prompt, navigate to the directory containing `transformer.py`, and run the script using the following structure:

```bash
python transformer.py <input_file_path> <output_file_path> [--transform ACTION:PARAMETERS]...


Positional Arguments:

input_file_path: (Required) Path to the input data file (e.g., data.csv, products.json).
output_file_path: (Required) Path where the transformed data will be saved.

Optional Arguments:

-h, --help: Show this help message and exit.
--transform ACTION:PARAMETERS: (Can be used multiple times) Specifies a transformation to apply. Transformations are applied in the order they appear on the command line.

Available ACTION:PARAMETERS formats:

select:col1,col2,...
Selects specified columns.
Example: --transform "select:ID,Name,Price"
filter:column,operator,value
Filters rows based on a condition.

Supported operators:

For numbers (or strings convertible to numbers): >, <, >=, <=, ==, !=
For strings: ==, !=, contains, startswith, endswith

Examples:

--transform "filter:Age,>,30"
--transform "filter:Department,==,Engineering"
--transform "filter:ProductName,contains,Laptop"
rename:old_name1:new_name1,old_name2:new_name2,...
Renames one or more columns.

Example: --transform "rename:ID:EmployeeID,Salary:AnnualSalary"
sort:col1:asc,col2:desc,...
Sorts data by one or more columns. asc for ascending, desc for descending.

Example: --transform "sort:Department:asc,Salary:desc"
addcol:new_col_name=expression
Adds a new column or modifies an existing one.

Simple Expressions Supported:

String Concatenation: ColumnA+ColumnB (e.g., FirstName+LastName)
Numeric (Column & Literal): ColumnName*Number or ColumnName+Number (e.g., Price*1.2 or Quantity+5)
Literal String: \"Your String\" (e.g., Status=\"Active\")
Literal Number: 123 or 3.14 (e.g., TaxRate=0.05)

Examples:

--transform "addcol:FullName=FirstName+LastName"
--transform "addcol:DiscountPrice=Price*0.9"
--transform "addcol:Country=\"USA\""

Examples:

Select specific columns from a CSV and save as JSON:
python transformer.py employees.csv selected_employees.json --transform "select:ID,FirstName,Department"

Filter CSV data, then sort, then output as CSV:

python transformer.py sales_data.csv filtered_sorted_sales.csv \
    --transform "filter:Region,==,North" \
    --transform "sort:OrderDate:desc"

Complex chain of operations on JSON data, outputting to CSV:

python transformer.py products.json processed_products.csv \
    --transform "filter:Category,==,Electronics" \
    --transform "addcol:PriceWithTax=Price*1.075" \
    --transform "rename:ID:ProductID,Stock:AvailableStock" \
    --transform "select:ProductID,Product,PriceWithTax,AvailableStock" \
    --transform "sort:PriceWithTax:asc"

Output

Console: Log messages detailing the operations, transformations applied, number of rows processed/affected, and any warnings or errors.
Output File: The transformed data will be saved to the specified output_file_path in the chosen format (CSV or JSON).
Log File: A transformer.log file will be created (or appended to) in the directory where transformer.py is run. This file contains a detailed history of all operations.

How It Works Internally

Logging & Argument Parsing: Initializes logging and parses command-line arguments.
Data Loading: Reads the input file (CSV or JSON). For CSVs, it attempts to convert numeric-looking string values to int or float.

Transformation Pipeline:

Iterates through each --transform argument provided by the user.
Parses the ACTION:PARAMETERS string to identify the transformation type and its specific arguments.
Calls the corresponding transformation function (e.g., transform_select_columns, transform_filter_rows, etc.) with the current state of the data. The output of one transformation becomes the input for the next.
Data Writing: Saves the final transformed data to the specified output file.

Limitations & Future Improvements

addcol Expression Complexity: The addcol transformation currently supports very simple expressions. A more robust solution would require a proper expression parsing library.
Error Handling in Transformations: While basic error handling exists, more granular reporting for issues within specific transformation steps could be added.
Configuration File for Transformations: For very long or complex transformation pipelines, a configuration file (e.g., JSON or YAML) to define the steps would be more user-friendly than long command lines.
More Transformation Types: Could be extended with more operations (e.g., group by, aggregate, pivot, merge/join - though these become complex without libraries like Pandas).
Streaming for Large Files: For very large files that don't fit in memory, the current approach of loading all data first might be problematic. A streaming approach (processing row by row without loading everything) would be needed.

---
