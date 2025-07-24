import argparse
import yaml
import json
from pathlib import Path
from .utils.template_engine import TemplateEngine
from .utils.adapter_factory import AdapterFactory
from .utils.config_validator import ConfigValidator


def load_variables_from_file(file_path: str) -> dict:
    """Load variables from YAML/JSON file"""
    import os

    cwd = os.getcwd()  # Get the current working directory (cwd)
    files = os.listdir(cwd)  # Get all the files in that directory
    print("Files in %r: %s" % (cwd, files))
    path = Path(file_path)
    if path.suffix == '.yaml' or path.suffix == '.yml':
        with open(path, 'r') as f:
            return yaml.safe_load(f)
    elif path.suffix == '.json':
        with open(path, 'r') as f:
            return json.load(f)
    else:
        raise ValueError("Unsupported file format. Use YAML or JSON.")


def parse_inline_variables(raw_vars: list) -> dict:
    """Convert --var key=value pairs to a dictionary"""
    variables = {}
    if raw_vars:
        for var_pair in raw_vars[0]:
            key, value = var_pair.split('=')
            variables[key.strip()] = value.strip()
    return variables

def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Universal Ingestion Agent")
    parser.add_argument("--template", required=True, help="Adapter template path")
    parser.add_argument("--vars-file", required=True, help="Variables config file (YAML/JSON)")
    args = parser.parse_args()

    print(f"Running with template: {args.template}")

    # Load variables
    variables = {}
    if args.vars_file:
        variables.update(load_variables_from_file(args.vars_file))
    elif args.var:
        variables.update(parse_inline_variables(args.var))

    # Generate and validate config
    config = TemplateEngine.generate_config(args.template, variables)
    ConfigValidator.validate(config["config"], config["type"])

    # Execute adapter
    adapter = AdapterFactory.create(config)
    adapter.connect()
    data = adapter.normalize()

    print(f"Fetched {len(data)} records")

    # Apply transformations

    # Save data

    # Save data to parquet TODO add a proper sink function
    print(data.head())
    #data.to_parquet("output.parquet")


if __name__ == "__main__":
    main()