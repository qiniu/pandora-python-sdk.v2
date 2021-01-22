from pdr_python_sdk.tools.create import create_pandora_app


def test_create_pandora_app():
    create_pandora_app("--name pdr_app --title pdr_app")
