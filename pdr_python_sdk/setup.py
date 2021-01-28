from setuptools import setup, find_packages

setup(
    name="pdr-python-sdk",
    version="1.0.13",
    keywords=["qiniu", "pandora", "sdk"],
    description="pandora python sdk, simple to integrate with pandora",
    license="Apache-2.0",
    url="https://github.com/qiniu/pandora-python-sdk.v2",
    project_urls={
        "Documentation": "https://developer.qiniu.com/express",
        "Source": "https://github.com/qiniu/pandora-python-sdk.v2",
        "Tracker": "https://github.com/qiniu/pandora-python-sdk.v2/issues",
    },
    entry_points={
        'console_scripts': [
            'upload_app=pdr_python_sdk.tools.upload:upload',
            'create_pandora_app=pdr_python_sdk.tools.create:create_pandora_app',
        ],
    },

    author="qiniu",
    author_email="pandora@qiniu.com",

    packages=find_packages(include=('pdr_python_sdk', 'pdr_python_sdk.*')),
    include_package_data=True,
    platforms="any",
    install_requires=[
        "urllib3",
        "pyyaml",
        "GitPython",
        "pandas"
    ]
)
