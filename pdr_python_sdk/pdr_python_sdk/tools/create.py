import argparse
import os
import shutil
import git
import json

DEFAULT_APP_NAME = "pandora_app"
TEMP_SDK_DIR = ".pandora_sdk_tmp_dir"


def create_pandora_app(mockargs=""):
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default=DEFAULT_APP_NAME, help="Input your application name")
    parser.add_argument("--title", default="A Template App", help="Input your application title")
    if len(mockargs) > 0:
        args = parser.parse_args(mockargs.split(" "))
    else:
        args = parser.parse_args()

    appname = args.name
    if not args.name or len(args.name) <= 0:
        appname = DEFAULT_APP_NAME

    if not os.path.exists(TEMP_SDK_DIR):
        os.mkdir(TEMP_SDK_DIR)
    else:
        print(f"please delete {TEMP_SDK_DIR} folder")
        return
    git.Git(TEMP_SDK_DIR).clone("https://github.com/qiniu/pandora-python-sdk.v2.git")
    template_dir = os.sep.join([TEMP_SDK_DIR, "pandora-python-sdk.v2", "template"])
    app_dir = os.sep.join([os.curdir, appname])
    shutil.move(template_dir, app_dir)
    shutil.rmtree(TEMP_SDK_DIR)

    config_file = os.sep.join([app_dir, "app.json"])

    # replace app name & title in app.json
    with open(config_file, 'r') as f:
        config = json.loads(f.read())
        config["name"] = appname
        config["title"] = args.title

    with open(config_file, 'w') as f:
        f.write(json.dumps(config, ensure_ascii=False))

    # replace app name in package.json
    package_file = os.sep.join([app_dir, "package.json"])
    if not os.path.exists(package_file):
        config = {
            "name": appname,
            "version": "0.1.0",
            "scripts": {
                "package": "bash ./run.sh package"
            }
        }
    else:
        with open(package_file, 'r') as f:
            config = json.loads(f.read())
            config["name"] = appname

    with open(package_file, 'w') as f:
        f.write(json.dumps(config, ensure_ascii=False))
