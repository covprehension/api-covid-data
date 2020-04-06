from setuptools import setup

setup(
    name='covid-app',
    packages=['covidapp'],
    include_package_data=True,
    install_requires=[
        'flask',
    ],
)