from setuptools import setup, find_packages

setup(
    name='pandasdb-events-connector',  # Change this to the desired name
    version='0.1.0',
    description='Your package description here',
    author='Your Name',
    author_email='your.email@example.com',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[],  # Add dependencies here
)

