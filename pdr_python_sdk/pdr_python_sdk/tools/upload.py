import argparse
import os
from pdr_python_sdk.client import connect


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("app_path", help="app_path: input your app_path")
    parser.add_argument("--host", default=os.getenv("PANDORA_HOST"), help="host: your pandora host")
    parser.add_argument("--scheme", default=os.getenv("PANDORA_SCHEME"), help="scheme: [http|https]")
    parser.add_argument("--port", default=os.getenv("PANDORA_PORT"), help="port: your pandora port")
    parser.add_argument("--token", default=os.getenv("PANDORA_TOKEN"), help="token: input your token")
    parser.add_argument("--overwrite", action='store_true')
    return parser.parse_args()


def upload():
    args = parse_args()
    params = {
        "scheme": args.scheme,
        "host": args.host,
        "port": args.port,
        "token": args.token,
    }
    conn = connect(**params)
    return conn.app_install(args.app_path, args.overwrite)
