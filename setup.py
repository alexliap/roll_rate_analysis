from setuptools import find_packages, setup

version = "0.1.7"

install_requires = ["numpy", "polars>=0.20.5", "pandas"]

extras_require = {"dev": ["pytest", "pre-commit"]}

long_description = """
Roll Rate Analysis python package. Both month over month and snapshot roll rate functionalities are supported.
It utilizes polars library for optimizations and speed.
"""

setup(
    name="roll_rate_analysis",
    version=version,
    package_dir={"roll_rate_analysis": "src/roll_rate_analysis"},
    author="Alexandros Liapatis",
    author_email="alexanrosliapates@gmail.com",
    packages=find_packages(where="src/", exclude=["tests"]),
    install_requires=install_requires,
    extras_require=extras_require,
    platforms="any",
    python_requires=">=3.8",
    classifiers=[
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    project_urls={"Source": "https://github.com/alexliap/roll_rate_analysis"},
    long_description=long_description,
)
