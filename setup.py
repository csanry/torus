from setuptools import find_packages, setup

setup(
    name="pipelines",
    version="0.1.0",
    author="Clarence San",
    author_email="clarencesan@gmail.com",
    description="Final project packages for MLE",
    license="MIT",
    packages=find_packages(where="pipelines"),
    python_requires=">=3.6",
)
