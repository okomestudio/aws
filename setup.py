#!/usr/bin/env python
import codecs
import os
import re

from setuptools import find_packages
from setuptools import setup


def find_meta(category, fpath="src/tsutils_aws/__init__.py"):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, fpath), "r") as f:
        package_root_file = f.read()
    matched = re.search(
        r"^__{}__\s+=\s+['\"]([^'\"]*)['\"]".format(category), package_root_file, re.M
    )
    if matched:
        return matched.group(1)
    raise Exception("Meta info string for {} undefined".format(category))


setup(
    name="tsutils-aws",
    description="TS's personal utilities for AWS",
    version=find_meta("version"),
    package_dir={"": "src"},
    packages=find_packages("src"),
    entry_points={
        "console_scripts": [
            "s3_copy=tsutils_aws.s3.copy:main",
            "s3_delete=tsutils_aws.s3.delete:main",
            "s3_list=tsutils_aws.s3.list:main",
            "s3_write_success=tsutils_aws.s3.write_success:main",
        ]
    },
    scripts=["bin/lambda_pack_deployment"],
    author="Taro Sato",
    author_email="okomestudio@gmail.com",
    url="https://github.com/okomestudio/tsutils-aws",
)
