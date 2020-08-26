"""Setuptools module"""
from setuptools import setup, find_packages

setup(
    name="romulis",
    version='0.1',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "Click>=7.1.2",
    ],
    entry_points={"console_scripts": ["romulis = romulis.cli:main"]},
)
