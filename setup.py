from setuptools import setup, find_packages
from pathlib import Path

# Load the package's VERSION file as a dictionary.
about = {}
ROOT_DIR = Path(__file__).resolve().parent
PACKAGE_DIR = ROOT_DIR / 'src' / 'bitz'
with open(PACKAGE_DIR / "VERSION") as f:
    _version = f.read().strip()
    about["__version__"] = _version
    
# What packages are required for this module to be executed?
def list_reqs(fname="requirements.txt"):
    with open(ROOT_DIR / fname) as fd:
        return fd.read().splitlines()

setup(
    name='bitz',
    version=about["__version__"],

    description='Utilities for Bitz Analytics',
    #long_description=long_description,

    # More metadata

    packages=find_packages(where="src"),

    #python_requires='>=3.8'
)
