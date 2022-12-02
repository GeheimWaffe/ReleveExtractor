# ReleveExtractor

This is an **absolutely revolutionary** application which will *transform* the way I'm doing my accounts. 

## How to prepare a build-ready distribution
### Configure the setup information
Create a setup.py file with

    from setuptools import setup
    setup(
        name='pyfin',
        version='1.1.0',
        packages=[<the name of the target package'],
        entry_points = {'console_scripts': ['pyfin_launch=pyfin.__main__:main']},
        install_requires=[<list of the packages to install>],
        url='the URL you want',
        license='GNU',
        author='vincent scherrer',
        author_email='vince1133@yahoo.fr',
        description='This is my extractor program'
)

### the entry point
For the entry point, you have to point to a main module in the package

### How to build
- run the command `sudo python3 setup.py bdist_wheel`
- This produces a build file in the dist folder
I have discovered that only this way works

### How to test
Once you're done you just have to call in the terminal the name of your entry point
