from setuptools import setup, find_packages
from pathlib import Path

current_dir = Path(__file__).parent
requirements_path = current_dir / 'requirements.txt'
install_requires = requirements_path.read_text().splitlines()

setup(
    name='index_db',
    version='0.1',
    packages=find_packages(),
    install_requires=install_requires,
)
