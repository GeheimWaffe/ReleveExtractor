from setuptools import setup

setup(
    name='pyfin',
    version='1.1.0',
    packages=['pyfin'],
    entry_points = {'console_scripts': ['pyfin_launch=pyfin.__main__:main']},
    install_requires=['openpyxl', 'pandas', 'pygsheets', 'odfpy'],
    url='www.pyfin.org',
    license='GNU',
    author='vincent scherrer',
    author_email='vince1133@yahoo.fr',
    description='This is my extractor program'
)
