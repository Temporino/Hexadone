from template_engine import TemplateEngine
import tempfile
import os

class TestTemplateEngine:
    def test_generate_config_from_yaml(self):
        with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False) as f:
            f.write(b"type: api\nconfig:\n  auth:\n    type: '{{AUTH_TYPE}}'")
            f.close()
            config = TemplateEngine.generate_config(f.name, {"AUTH_TYPE": "oauth2"})
            assert config["config"]["auth"]["type"] == "oauth2"
            os.unlink(f.name)