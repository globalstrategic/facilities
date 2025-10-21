from setuptools import setup, find_packages

setup(
    name="talloy",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.3.0",
        "requests>=2.26.0",
        "openai>=1.0.0",
        "pycountry>=22.0.0",
    ],
    dependency_links=[
        "git+https://github.com/microprediction/goodgleif.git#egg=goodgleif",
    ],
    entry_points={
        'console_scripts': [
            'talloy=talloy.company_identifier:main',
            'talloy-aggregate=talloy.aggregate_config:main',
        ],
    },
    python_requires=">=3.7",
)
