# template_engine.py
from jinja2 import Template
import yaml


class TemplateEngine:
    @staticmethod
    def generate_config(template_path: str, variables: dict) -> dict:
        with open(template_path, 'r') as f:
            template = Template(f.read())

        rendered = template.render(**variables)
        return yaml.safe_load(rendered)