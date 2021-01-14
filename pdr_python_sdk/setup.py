from setuptools import setup, find_packages

setup(
    name="pdr-python-sdk",
    version="1.0.9",
    keywords=["pip", "qiniu", "pandora", "sdk"],
    description="pandora python sdk, include customapi, customoperation, kvstore",
    license="MIT Licence",

    author="qiniu",
    author_email="pandora@qiniu.com",

    packages=find_packages(),
    include_package_data=True,
    platforms="any",
    install_requires=[
        "urllib3",
        "pyyaml",
        "pandas"
    ]
)
