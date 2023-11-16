from setuptools import find_packages, setup

install_requires = [
    "numpy",
    "polars==0.19.11",
]

setup(
    name="roll_rate_analysis",
    version="0.0.1",
    package_dir={"": "src"},
    author="Alexandros Liapatis",
    author_email="alexanrosliapates@gmail.com",
    packages=find_packages(),
    install_requires=install_requires,
)
