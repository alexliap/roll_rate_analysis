# Roll Rate Analysis

![deploy on pypi](https://github.com/alexliap/roll_rate_analysis/actions/workflows/publish-package.yaml/badge.svg)
![PyPI Version](https://img.shields.io/pypi/v/roll-rate-analysis?label=pypi%20package)
![PyPI - Downloads](https://img.shields.io/pypi/dm/roll-rate-analysis)

Roll rate analysis is a type of analysis made in the sector of credit risk and is used to define the target variable. More specifically, it is used in the creation of Risk Application, and Behavioral Scorecards. Usually, it is a multiple iteration process which makes the process difficult if someone is working on custom code. The purpose of this package is to make the process easier by parametrizing a lot of factors needed in each case and iteration.

It is currently under development.

### Installation

The package exists on PyPI so tou can install it directly to your environment by running the command

```terminal
pip install roll-rate-analysis
```
### Dependencies

* numpy
* pandas
* polars

Additional packages for development:

* pytest
* pre-commit

### Development

If you want to contribute you fork the repository and clone it on your machine

```terminal
git clone https://github.com/alexliap/roll_rate_analysis.git
```

And after you create you environment (either venv or conda) and activate it then run this command

```terminal
pip install -e '.[dev]'
```

That way not only the required dependencies are installed but also the development ones.

Also this makes it so that when you import the code to test it, you can do it like any other module but containing the changes you made locally.

Before you decide to commit, run the following command to reformat code in order to be in the acceptable style.

```terminal
pre-commit install
pre-commit run --all-files
```
