import shutil
import os
from pdr_python_sdk.tools.create import create_pandora_app
from pdr_python_sdk.tools.create import TEMP_SDK_DIR


def test_create_pandora_app():
    app_name = "test_pdr_app"
    shutil.rmtree(os.sep.join([os.curdir, TEMP_SDK_DIR]), ignore_errors=True)
    shutil.rmtree(os.sep.join([os.curdir, app_name]), ignore_errors=True)
    create_pandora_app(f"--name {app_name} --title pdr_app")
