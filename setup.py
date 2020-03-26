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
    packages=setuptools.find_packages(),
    python_requires='>=3.6'
)