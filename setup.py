from setuptools import setup

with open('VERSION', 'r') as f:
    VERSION = f.read()

with open('README.md', 'r') as f:
    LONG_DESCRIPTION = f.read()

setup(
    name='registry',
    version=VERSION,
    description='This is the SDK for the registry of models, datasets and '
    'workflows.',
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    author="DP Technology",
    license="LGPLv3",
    package_dir={'': 'src'},
    packages=[
        "registry",
    ],
    python_requires='>=3.6',
    install_requires=[
        "requests",
        "oss2",
    ]
)
