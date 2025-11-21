#!/usr/bin/env python

from setuptools import setup
import re

with open('README.md') as f:
    long_description = f.read()

# Read version from target_redshift/__init__.py
with open('target_redshift/__init__.py') as f:
    version = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", f.read(), re.M).group(1)

setup(name="target-redshift",
      version=version,
      description="Singer.io target for loading data to Amazon Redshift",
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="Community Contributors",
      url='https://github.com/rhodium-data/target-redshift',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=["target_redshift"],
      install_requires=[
          'pipelinewise-singer-python==1.*',
          'boto3>=1.20.0',
          'psycopg2-binary>=2.9.0',
          'inflection>=0.5.0',
          'joblib>=1.0.0'
      ],
      extras_require={
          "test": [
                "pylint>=3.0.0",
                "pytest>=6.2.0",
                "mock>=4.0.0",
                "coverage>=5.0"
            ]
      },
      entry_points="""
          [console_scripts]
          target-redshift=target_redshift:main
      """,
      packages=["target_redshift"],
      package_data = {},
      include_package_data=True,
)
