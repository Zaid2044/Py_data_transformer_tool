import csv
import json
import os
import argparse
import logging
from datetime import datetime 

# --- Logging Setup ---
def setup_logging():
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger('data_transformer') 
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = logging.FileHandler('transformer.log')
        file_handler.setFormatter(log_formatter)
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)
    return logger

logger = setup_logging() 

# --- Utility for Type Conversion ---
def _try_convert_type(value):
    """Tries to convert a string value to int or float, otherwise returns original."""
    if isinstance(value, (int, float)):
        return value
    if not isinstance(value, str): 
        return value
        
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value

def _preprocess_data_types(data):
    """Iterates through data (list of dicts) and tries to convert string values to numbers."""
    if not data:
        return []
    processed_data = []
    for row in data:
        processed_row = {key: _try_convert_type(value) for key, value in row.items()}
        processed_data.append(processed_row)
    return processed_data

# --- Data Loading Functions ---
def load_csv_data(filepath):
    data = []
    try:
        with open(filepath, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row_dict in reader:
                data.append(row_dict)
        logger.info(f"Successfully read {len(data)} rows from CSV: {filepath}")
        return _preprocess_data_types(data) 
    except FileNotFoundError:
        logger.error(f"CSV file not found at '{filepath}'")
        return None
    except Exception as e:
        logger.error(f"Error reading CSV file '{filepath}': {e}", exc_info=True)
        return None

def load_json_data(filepath):
    try:
        with open(filepath, mode='r', encoding='utf-8') as jsonfile:
            data = json.load(jsonfile)
        if isinstance(data, list):
            if not data or all(isinstance(item, dict) for item in data):
                logger.info(f"Successfully read {len(data)} records from JSON: {filepath}")
                return data 
            else:
                logger.error(f"JSON file '{filepath}' does not contain a list of objects (dictionaries).")
                return None
        else:
            logger.error(f"JSON file '{filepath}' does not contain a list.")
            return None
    except FileNotFoundError:
        logger.error(f"JSON file not found at '{filepath}'")
        return None
    except json.JSONDecodeError:
        logger.error(f"Could not decode JSON from '{filepath}'. Invalid JSON format.")
        return None
    except Exception as e:
        logger.error(f"Error reading JSON file '{filepath}': {e}", exc_info=True)
        return None

# --- Data Writing Functions ---
def write_csv_data(filepath, data, fieldnames=None):
    if not data:
        logger.warning(f"No data provided to write to CSV: {filepath}")
        return False
    if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
        logger.error(f"Data for CSV output must be a list of dictionaries. Path: {filepath}")
        return False
    if fieldnames is None:
        if data: fieldnames = list(data[0].keys())
        else: 
            logger.warning(f"Cannot infer fieldnames from empty data for CSV: {filepath}")
            return False 
    try:
        with open(filepath, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(data)
        logger.info(f"Successfully wrote {len(data)} rows to CSV: {filepath}")
        return True
    except IOError as e:
        logger.error(f"IOError writing CSV file '{filepath}': {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Error writing CSV file '{filepath}': {e}", exc_info=True)
        return False

def write_json_data(filepath, data):
    if not isinstance(data, list): 
        logger.error(f"Data for JSON output must be a list. Path: {filepath}")
        return False
    try:
        with open(filepath, mode='w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, indent=4) 
        logger.info(f"Successfully wrote {len(data)} records to JSON: {filepath}")
        return True
    except IOError as e:
        logger.error(f"IOError writing JSON file '{filepath}': {e}", exc_info=True)
        return False
    except TypeError as e: 
        logger.error(f"Data not serializable for JSON in '{filepath}': {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Error writing JSON file '{filepath}': {e}", exc_info=True)
        return False

# --- Transformation Functions ---
def transform_select_columns(data, columns_to_keep):
    if not data: return []
    if not columns_to_keep:
        logger.warning("No columns specified for selection. Returning original data.")
        return data
    transformed_data = []
    logger.info(f"Applying transformation: Select Columns - Keeping {columns_to_keep}")
    for original_row in data:
        new_row = {col: original_row[col] for col in columns_to_keep if col in original_row}
        if new_row: transformed_data.append(new_row)
    logger.info(f"Select Columns transformation complete. {len(transformed_data)} rows processed.")
    return transformed_data

def transform_filter_rows(data, column_name, operator, value_to_compare):
    if not data: return []
    filtered_data = []
    logger.info(f"Applying transformation: Filter Rows - Where '{column_name}' {operator} '{value_to_compare}'")
    numeric_operators = ['>', '<', '>=', '<=', '==', '!='] # Simplified, rely on pre-converted types
    # String specific operators can be added if needed, e.g. 'contains'
    
    try: # Try to convert value_to_compare to match potential numeric data
        comp_value = _try_convert_type(value_to_compare)
    except Exception: # Should not happen with _try_convert_type
        comp_value = value_to_compare 

    for row in data:
        if column_name not in row:
            continue
        
        actual_value_in_row = row[column_name] # This should be pre-converted if from CSV
        match = False
        try:
            if operator == '>' and actual_value_in_row > comp_value: match = True
            elif operator == '<' and actual_value_in_row < comp_value: match = True
            elif operator == '>=' and actual_value_in_row >= comp_value: match = True
            elif operator == '<=' and actual_value_in_row <= comp_value: match = True
            elif operator == '==' and actual_value_in_row == comp_value: match = True
            elif operator == '!=' and actual_value_in_row != comp_value: match = True
            elif operator == 'contains' and isinstance(actual_value_in_row, str) and isinstance(comp_value, str) and comp_value in actual_value_in_row: match = True
            elif operator == 'startswith' and isinstance(actual_value_in_row, str) and isinstance(comp_value, str) and actual_value_in_row.startswith(comp_value): match = True
            elif operator == 'endswith' and isinstance(actual_value_in_row, str) and isinstance(comp_value, str) and actual_value_in_row.endswith(comp_value): match = True
            else:
                if operator not in numeric_operators + ['contains', 'startswith', 'endswith']:
                    logger.warning(f"Unsupported operator '{operator}' for filter. Row skipped for this condition.")
        except TypeError: # Handles comparing incompatible types, e.g., string with number if conversion failed
            # logger.debug(f"Type error during filter comparison: '{actual_value_in_row}' ({type(actual_value_in_row)}) {operator} '{comp_value}' ({type(comp_value)}). Row skipped.")
            pass
        except Exception as e:
            logger.error(f"Unexpected error during filter comparison: {e}", exc_info=True)

        if match: filtered_data.append(row)
    logger.info(f"Filter Rows transformation complete. {len(filtered_data)} rows matched criteria.")
    return filtered_data

def transform_rename_columns(data, rename_map):
    """rename_map is a dict like {'old_name1': 'new_name1', 'old_name2': 'new_name2'}"""
    if not data or not rename_map: return data
    transformed_data = []
    logger.info(f"Applying transformation: Rename Columns - Map: {rename_map}")
    for original_row in data:
        new_row = original_row.copy() # Start with a copy
        for old_name, new_name in rename_map.items():
            if old_name in new_row:
                new_row[new_name] = new_row.pop(old_name)
        transformed_data.append(new_row)
    logger.info(f"Rename Columns transformation complete. {len(transformed_data)} rows processed.")
    return transformed_data

def transform_sort_data(data, sort_keys):
    """sort_keys is a list of tuples like [('ColumnName', 'asc'), ('AnotherCol', 'desc')]"""
    if not data or not sort_keys: return data
    logger.info(f"Applying transformation: Sort Data - Keys: {sort_keys}")
    
    current_data = list(data) 
    for key_info in reversed(sort_keys):
        if len(key_info) != 2:
            logger.warning(f"Invalid sort key format: {key_info}. Expected ('column', 'asc'/'desc'). Skipping.")
            continue
        column_name, order = key_info
        reverse_order = (order.lower() == 'desc')
        
        current_data.sort(key=lambda row: (
            row.get(column_name) is None,
            row.get(column_name) 
        ), reverse=reverse_order)
        
    logger.info(f"Sort Data transformation complete. {len(current_data)} rows processed.")
    return current_data


def transform_add_modify_column(data, new_col_name, expression):
    """
    Adds or modifies a column based on a simple expression.
    Supported expressions:
    - "<ExistingCol1> + <ExistingCol2>" (string concatenation)
    - "<ExistingCol> * <literal_number>" (numeric multiplication)
    - "<ExistingCol> + <literal_number>" (numeric addition)
    - "<literal_string>" (assigns a fixed string)
    - "<literal_number>" (assigns a fixed number)
    """
    if not data: return []
    logger.info(f"Applying transformation: Add/Modify Column '{new_col_name}' with expression '{expression}'")
    transformed_data = []
    
    for original_row in data:
        new_row = original_row.copy()
        try:
            if '+' in expression and not any(c.isdigit() for c in expression.split('+')[1].strip()) and not expression.split('+')[1].strip().startswith('"'): # Heuristic
                parts = [p.strip() for p in expression.split('+', 1)]
                if len(parts) == 2:
                    val1_expr, val2_expr = parts
                    val1 = str(new_row.get(val1_expr, val1_expr if not val1_expr.isalnum() else ""))
                    val2 = str(new_row.get(val2_expr, val2_expr if not val2_expr.isalnum() else ""))
                    new_row[new_col_name] = val1 + val2
                else: 
                    new_row[new_col_name] = _try_convert_type(expression)

            elif '*' in expression or ('+' in expression and any(c.isdigit() for c in expression.split('+')[1].strip())):
                op_char = None
                if '*' in expression: op_char = '*'
                elif '+' in expression: op_char = '+'

                if op_char:
                    parts = [p.strip() for p in expression.split(op_char, 1)]
                    if len(parts) == 2:
                        col_operand_name, literal_operand_str = parts
                        if col_operand_name in new_row:
                            col_val = _try_convert_type(new_row[col_operand_name])
                            literal_val = _try_convert_type(literal_operand_str)
                            if isinstance(col_val, (int, float)) and isinstance(literal_val, (int, float)):
                                if op_char == '*': new_row[new_col_name] = col_val * literal_val
                                elif op_char == '+': new_row[new_col_name] = col_val + literal_val
                            else:
                                logger.warning(f"Cannot perform arithmetic for '{expression}' on row. Operands not numeric: {col_val}, {literal_val}")
                                new_row[new_col_name] = None 
                        else:
                            logger.warning(f"Column '{col_operand_name}' not found for arithmetic in expression '{expression}'.")
                            new_row[new_col_name] = None
                    else: 
                         new_row[new_col_name] = _try_convert_type(expression)
                else: 
                    new_row[new_col_name] = _try_convert_type(expression)

            else:
                if (expression.startswith('"') and expression.endswith('"')) or \
                   (expression.startswith("'") and expression.endswith("'")):
                    new_row[new_col_name] = expression[1:-1]
                else: 
                    new_row[new_col_name] = _try_convert_type(expression)
            
        except Exception as e:
            logger.error(f"Error applying add/modify column for expression '{expression}' on row: {e}", exc_info=False) 
            new_row[new_col_name] = None 
        transformed_data.append(new_row)

    logger.info(f"Add/Modify Column transformation complete. {len(transformed_data)} rows processed.")
    return transformed_data


# --- Main Application Logic ---
def main():
    parser = argparse.ArgumentParser(
        description="CSV/JSON Data Transformer Tool.",
        formatter_class=argparse.RawTextHelpFormatter 
    )
    parser.add_argument("input_file", help="Path to the input data file (CSV or JSON).")
    parser.add_argument("output_file", help="Path to save the transformed data file (CSV or JSON).")
    
    parser.add_argument(
        "--transform",
        action="append",
        metavar="ACTION:PARAMETERS",
        help="""Apply a transformation. Can be used multiple times. Transformations are applied in order.
Available actions and their parameter formats:
  select:col1,col2,...                   (e.g., --transform "select:ID,FirstName,Age")
  filter:column,operator,value           (e.g., --transform "filter:Age,>,30")
                                           Supported filter operators: >, <, >=, <=, ==, != (for numbers or strings)
                                                                     contains, startswith, endswith (for strings)
  rename:old_name1:new_name1,old_name2:new_name2 (e.g., --transform "rename:ID:EmployeeID,Salary:AnnualSalary")
  sort:col1:asc,col2:desc                (e.g., --transform "sort:Department:asc,Salary:desc")
  addcol:new_col_name=expression         (e.g., --transform "addcol:FullName=FirstName+LastName"
                                                --transform "addcol:Bonus=Salary*0.1"
                                                --transform "addcol:Status=\"Active\"" )
                                           Simple expressions: <ColA>+<ColB> (concat), <Col>*<Num> (multiply),
                                                              <Col>+<Num> (add), "<LiteralStr>", <LiteralNum>
"""
    )

    args = parser.parse_args()

    input_filepath = args.input_file
    _, input_ext = os.path.splitext(input_filepath)
    input_ext = input_ext.lower()

    loaded_data = None
    if input_ext == '.csv':
        loaded_data = load_csv_data(input_filepath)
    elif input_ext == '.json':
        loaded_data = load_json_data(input_filepath)
    else:
        logger.error(f"Unsupported input file type: {input_ext}. Please use .csv or .json.")
        return

    if loaded_data is None:
        logger.error(f"Failed to load data from {input_filepath}. Exiting.")
        return

    current_data = loaded_data 
    
    if args.transform:
        for trans_str in args.transform:
            try:
                action, params_str = trans_str.split(":", 1)
                action = action.strip().lower()

                if action == "select":
                    columns_to_keep = [col.strip() for col in params_str.split(',')]
                    current_data = transform_select_columns(current_data, columns_to_keep)
                elif action == "filter":
                    filter_parts = [part.strip() for part in params_str.split(',', 2)]
                    if len(filter_parts) == 3:
                        col, op, val = filter_parts
                        current_data = transform_filter_rows(current_data, col, op, val)
                    else:
                        logger.error(f"Invalid filter params: '{params_str}'. Expected 'column,operator,value'.")
                elif action == "rename":
                    rename_map = {}
                    pairs = params_str.split(',')
                    for pair in pairs:
                        old, new = pair.split(':', 1)
                        rename_map[old.strip()] = new.strip()
                    current_data = transform_rename_columns(current_data, rename_map)
                elif action == "sort":
                    sort_keys_info = []
                    keys = params_str.split(',')
                    for key_pair in keys:
                        col, order = key_pair.split(':', 1)
                        sort_keys_info.append((col.strip(), order.strip().lower()))
                    current_data = transform_sort_data(current_data, sort_keys_info)
                elif action == "addcol":
                    if '=' not in params_str:
                        logger.error(f"Invalid addcol params: '{params_str}'. Expected 'new_col_name=expression'.")
                        continue
                    new_col_name, expression = params_str.split('=', 1)
                    current_data = transform_add_modify_column(current_data, new_col_name.strip(), expression.strip())
                else:
                    logger.warning(f"Unknown transformation action: '{action}'. Skipping.")

                if not current_data and action != "filter": 
                    logger.warning(f"Transformation '{action}:{params_str}' resulted in no data. Subsequent transformations might be affected or fail.")

            except ValueError as ve: 
                 logger.error(f"Error parsing transformation string '{trans_str}': {ve}. Ensure format is 'action:params'.")
            except Exception as e:
                 logger.error(f"Error applying transformation '{trans_str}': {e}", exc_info=True)


    output_filepath = args.output_file
    _, output_ext = os.path.splitext(output_filepath)
    output_ext = output_ext.lower()

    if not current_data:
        logger.info("No data to write after transformations.")

        if output_ext == '.csv':
            write_csv_data(output_filepath, [], fieldnames=[]) 
        elif output_ext == '.json':
            write_json_data(output_filepath, []) 
        return

    final_fieldnames = None
    if current_data:
        final_fieldnames = list(current_data[0].keys())


    if output_ext == '.csv':
        write_csv_data(output_filepath, current_data, fieldnames=final_fieldnames)
    elif output_ext == '.json':
        write_json_data(output_filepath, current_data)
    else:
        logger.error(f"Unsupported output file type: {output_ext}. Please use .csv or .json.")

if __name__ == "__main__":
    main() 