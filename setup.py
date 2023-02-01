from setuptools import setup, find_packages
from pathlib import Path

# Load the package's VERSION file as a dictionary.
about = {}
ROOT_DIR = Path(__file__).resolve().parent
REQUIREMENTS_DIR = ROOT_DIR / 'requirements'
PACKAGE_DIR = ROOT_DIR / 'src' / 'bitz'
with open(PACKAGE_DIR / "VERSION") as f:
    _version = f.read().strip()
    about["__version__"] = _version

setup(
    name='bitz',
    version=about["__version__"],

    description='Utilities for Bitz Analytics',
    #long_description=long_description,

    # More metadata

    packages=find_packages(where="src"),

    #python_requires='>=3.8'
)
