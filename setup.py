import os
import re
import sys
from functools import reduce
from typing import Dict, List

from setuptools import setup, find_packages
from setuptools.command.develop import develop


def get_version():
    # Get version without importing, which avoids dependency issues
    with open('optimus/_version.py') as version_file:
        return re.search(r"""__version__\s+=\s+(['"])(?P<version>.+?)\1""",
                         version_file.read()).group('version')


if sys.version_info < (3, 7):
    raise RuntimeError('This version requires Python 3.7+')  # pragma: no cover


def readme():
    with open('README.md', encoding="utf-8") as f:
        return f.read()


# Requirements
try:
    import google.colab
    IN_COLAB = True
except ImportError:
    IN_COLAB = False

requirements_file = 'requirements.txt'

if "DATABRICKS_RUNTIME_VERSION" in os.environ:
    requirements_file = 'requirements/databricks-requirements.txt'
elif IN_COLAB:
    requirements_file = 'requirements/google-colab-requirements.txt'

with open(requirements_file) as f:
    required = f.read().splitlines()
    if "--no-git" in sys.argv:
        required = [r for r in required if "git+" not in r]
        sys.argv.remove("--no-git")

extras_requirements_keys = ['spark', 'pandas',
                            'dask', 'vaex', 'cudf', 'ai', 'db', 'api']
extras_requirements: Dict[str, List[str]] = {}

for extra in extras_requirements_keys:
    with open(f"requirements/{extra}-requirements.txt") as f:
        extras_requirements[extra] = f.read().splitlines()

lint_requirements = ['pep8', 'pyflakes']
test_requirements = ['pytest', 'mock', 'nose']
dependency_links: List[str] = []
setup_requirements = ['pytest-runner']
if 'nosetests' in sys.argv[1:]:
    setup_requirements.append('nose')

empty_list: List[str] = []

setup(
    name='pyoptimus',
    version=get_version(),
    author='Argenis Leon',
    author_email='argenisleon@gmail.com',
    url='https://github.com/hi-primus/optimus/',
    description=(
        'Optimus is the missing framework for cleaning and pre-processing data in a distributed fashion.'),
    long_description=readme(),
    long_description_content_type='text/markdown',
    license='APACHE',
    packages=find_packages(),
    install_requires=required,
    tests_require=test_requirements,
    setup_requires=setup_requirements,
    extras_require={
        'test': test_requirements,
        'all': reduce(lambda x, key: x + extras_requirements[key], extras_requirements, empty_list),
        'docs': ['sphinx'] + test_requirements,
        'lint': lint_requirements,
        **extras_requirements
    },
    dependency_links=dependency_links,
    test_suite='nose.collector',
    include_package_data=True,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering :: Artificial Intelligence',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    keywords=['datacleaner', 'data-wrangling',
              'data-cleansing', 'data-profiling'],
)
