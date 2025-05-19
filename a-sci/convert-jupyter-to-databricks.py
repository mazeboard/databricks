import argparse
import json
import nbformat

def convert_ipynb_to_databricks(ipynb_file, dbc_file):
    # Read the Jupyter notebook
    with open(ipynb_file, 'r') as f:
        notebook = nbformat.read(f, as_version=4)

    # Initialize an empty list to hold the formatted notebook content
    databricks_cells = []

    # Process each cell in the Jupyter notebook
    for cell in notebook.cells:
        if cell.cell_type == 'code':
            # Create a code cell with Databricks delimiter
            code = cell.source.strip()
            if code:
                databricks_cells.append(f"# COMMAND ----------\n{code}")

    # Join all code cells with the Databricks delimiter
    databricks_content = "\n\n".join(databricks_cells)

    # Write the Databricks notebook file
    with open(dbc_file, 'w') as f:
        f.write(databricks_content)

    print(f"Converted {ipynb_file} to {dbc_file}")

if __name__ == "__main__":
    # Set up the argument parser
    parser = argparse.ArgumentParser(description="Convert a Jupyter .ipynb notebook to a Databricks .py notebook")
    parser.add_argument("input_file", help="The path to the Databricks exported .py file")
    parser.add_argument("output_file", help="The path to save the output .py file")

    # Parse the arguments
    args = parser.parse_args()

    # Run the conversion function with the provided arguments
    convert_ipynb_to_databricks(args.input_file, args.output_file)

