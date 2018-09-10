import re
import sys
from setuptools import setup, find_packages


# Get version without importing, which avoids dependency issues
def get_version():
    with open('optimus/version.py') as version_file:
        return re.search(r"""__version__\s+=\s+(['"])(?P<version>.+?)\1""",
                         version_file.read()).group('version')


# Requirements
with open('requirements.txt') as f:
    required = f.read().splitlines()


if sys.version_info < (3, 6):
    raise RuntimeError('This version requires Python 3.6+')  # pragma: no cover


def readme():
    with open('README.md') as f:
        return f.read()


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
    author='Favio Vazquez and Argenis Leon',
    author_email='favio.vazquez@ironmussa.com',
    url='https://github.com/ironmussa/Optimus/',
    download_url='https://github.com/ironmussa/Optimus/archive/2.0.6.tar.gz',
    description=('Optimus is the missing framework for cleaning and pre-processing data in a distributed fashion with '
                 'pyspark.'),
    long_description=readme(),
    long_description_content_type='text/markdown',
    license='APACHE',
    packages=find_packages(),
    install_requires=required,
    tests_require=tests_require,
    setup_requires=setup_requires,
    extras_require={
        'test': tests_require,
        'all': required + tests_require,
        'docs': ['sphinx'] + tests_require,
        'lint': lint_requires
    },
    dependency_links=dependency_links,
    test_suite='nose.collector',
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords=['datacleaner', 'apachespark', 'spark', 'pyspark', 'data-wrangling', 'data-cleansing'],
)
