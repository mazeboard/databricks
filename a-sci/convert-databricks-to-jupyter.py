import argparse
import nbformat as nbf

def convert_databricks_to_jupyter(input_py_file, output_ipynb_file):
    # Read the input .py file
    with open(input_py_file, 'r') as f:
        lines = f.readlines()

    # Create a new Jupyter notebook
    notebook = nbf.v4.new_notebook()

    # Initialize an empty cell content list
    cell_content = []

    # Iterate through each line in the .py file
    for line in lines:
        # If we encounter a cell divider, we store the current cell content and start a new cell
        if line.strip() == "# COMMAND ----------":
            if cell_content:
                notebook.cells.append(nbf.v4.new_code_cell(''.join(cell_content).strip()))
                cell_content = []
        else:
            cell_content.append(line)

    # Add the last cell if there's any remaining content
    if cell_content:
        notebook.cells.append(nbf.v4.new_code_cell(''.join(cell_content).strip()))

    # Write the notebook to a .ipynb file
    with open(output_ipynb_file, 'w') as f:
        nbf.write(notebook, f)

    print(f"Converted {input_py_file} to {output_ipynb_file}")

if __name__ == "__main__":
    # Set up the argument parser
    parser = argparse.ArgumentParser(description="Convert a Databricks .py notebook to a Jupyter .ipynb notebook")
    parser.add_argument("input_file", help="The path to the Databricks exported .py file")
    parser.add_argument("output_file", help="The path to save the output .ipynb file")

    # Parse the arguments
    args = parser.parse_args()

    # Run the conversion function with the provided arguments
    convert_databricks_to_jupyter(args.input_file, args.output_file)

