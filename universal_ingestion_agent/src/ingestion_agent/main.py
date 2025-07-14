import argparse
import yaml
import json
from pathlib import Path
from utils.template_engine import TemplateEngine
from utils.adapter_factory import AdapterFactory
from utils.config_validator import ConfigValidator


def load_variables_from_file(file_path: str) -> dict:
    """Load variables from YAML/JSON file"""
    path = Path(file_path)
    if path.suffix == '.yaml' or path.suffix == '.yml':
        with open(path, 'r') as f:
            return yaml.safe_load(f)
    elif path.suffix == '.json':
        with open(path, 'r') as f:
            return json.load(f)
    else:
        raise ValueError("Unsupported file format. Use YAML or JSON.")


def parse_cli_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description="Universal Ingestion Agent")
    parser.add_argument(
        "--template",
        type=str,
        required=True,
        help="Path to adapter template (e.g., templates/api_adapter.yaml)"
    )
    parser.add_argument(
        "--vars-file",
        type=str,
        help="Path to variables config file (YAML/JSON)"
    )
    parser.add_argument(
        "--var",
        action='append',
        nargs='*',
        help="Inline variables (e.g., --var CLIENT_ID=123)"
    )
    return parser.parse_args()


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
    args = parse_cli_args()

    # Load variables
    variables = {}
    if args.vars_file:
        variables.update(load_variables_from_file(args.vars_file))
    if args.var:
        variables.update(parse_inline_variables(args.var))

    # Generate and validate config
    config = TemplateEngine.generate_config(args.template, variables)
    ConfigValidator.validate(config["config"], config["type"])

    # Execute adapter
    adapter = AdapterFactory.create(config)
    adapter.connect()
    data = adapter.normalize()

    print(f"Fetched {len(data)} records")
    data.to_parquet("output.parquet")


if __name__ == "__main__":
    main()