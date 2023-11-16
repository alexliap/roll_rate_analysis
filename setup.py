from setuptools import find_packages, setup

install_requires = ["numpy", "polars==0.19.11", "pandas"]

extras_require = {"dev": ["pytest", "pre-commit", "jupyter"]}

setup(
    name="roll_rate_analysis",
    version="0.1.0",
    package_dir={"": "src"},
    author="Alexandros Liapatis",
    author_email="alexanrosliapates@gmail.com",
    packages=find_packages(),
    install_requires=install_requires,
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
)
