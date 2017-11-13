import re

from setuptools import setup, find_packages

# Get version without importing, which avoids dependency issues
def get_version():
    with open('optimus/version.py') as version_file:
        return re.search(r"""__version__\s+=\s+(['"])(?P<version>.+?)\1""",
                         version_file.read()).group('version')

import sys
if sys.version_info < (3, 5):
    raise RuntimeError('This version requires Python 3.5+')  # pragma: no cover

def readme():
    with open('Readme.txt') as f:
        return f.read()

install_requires = ['pytest', 'findspark', 'pytest-spark', 'spark-df-profiling-optimus', 'pyspark', 'seaborn',
                    'pixiedust-optimus']
lint_requires = [
    'pep8',
    'pyflakes'
]

tests_require = ['pytest','mock', 'nose']

dependency_links = []
setup_requires=['pytest-runner']
if 'nosetests' in sys.argv[1:]:
    setup_requires.append('nose')

setup(
    name='optimuspyspark',
    version=get_version(),
    author='Favio Vazquez',
    author_email='favio.vazquez@ironmussa.com',
    url='https://github.com/ironmussa/Optimus/',
    download_url='https://github.com/ironmussa/Optimus/archive/1.2.0.tar.gz',
    description=('Optimus is the missing framework for cleaning and preprocessing data in a distributed fashion with '
                 'pyspark.'),
    long_description=readme(),
    license='APACHE',
    packages=find_packages(),
    install_requires=install_requires,
    tests_require=tests_require,
    setup_requires=setup_requires,
    extras_require={
        'test': tests_require,
        'all': install_requires + tests_require,
        'docs': ['sphinx'] + tests_require,
        'lint': lint_requires
    },
    dependency_links=dependency_links,
    test_suite='nose.collector',
    include_package_data=False,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords=['datacleaner', 'apachespark', 'spark', 'pyspark', 'data-wrangling', 'data-cleansing'],
)
