import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="deltaflow-evaneldemachki",
    version="0.0.1",
    author="Evan Eldemachki",
    description="Efficient storage and version control for pandas dataframes",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=[
        'certifi==2022.12.7',
        'chardet==3.0.4',
        'dnspython==1.16.0',
        'fastparquet==0.3.3',
        'guppy3==3.0.9',
        'idna==2.9',
        'llvmlite==0.31.0',
        'numba==0.48.0',
        'numpy==1.18.1',
        'pandas==1.0.1',
        'psutil==5.6.7',
        'python-dateutil==2.8.1',
        'pytz==2019.3',
        'requests==2.23.0',
        'six==1.14.0',
        'thrift==0.13.0',
        'urllib3==1.25.8'
    ],
    packages=setuptools.find_packages(),
    python_requires='>=3.6'
)