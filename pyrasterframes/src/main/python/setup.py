#
# This software is licensed under the Apache 2 license, quoted below.
#
# Copyright 2019 Astraea, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# [http://www.apache.org/licenses/LICENSE-2.0]
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# SPDX-License-Identifier: Apache-2.0
#

# Always prefer setuptools over distutils
from setuptools import setup
from os import path
import sys
from glob import glob
from io import open
import distutils.cmd

try:
    exec(open('pyrasterframes/version.py').read())  # executable python script contains __version__; credit pyspark
except IOError:
    print("Run setup via `sbt 'pySetup arg1 arg2'` to ensure correct access to all source files and binaries.")
    sys.exit(-1)


VERSION = __version__

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    readme = f.read()


def _divided(msg):
    divider = ('-' * 50)
    return divider + '\n' + msg + '\n' + divider


class PweaveDocs(distutils.cmd.Command):
    """A custom command to run documentation scripts through pweave."""
    description = 'Pweave PyRasterFrames documentation scripts'
    user_options = [
        # The format is (long option, short option, description).
        ('files=', 'f', 'Specific files to pweave. Defaults to all in `docs` directory.'),
    ]

    def initialize_options(self):
        """Set default values for options."""
        # Each user option must be listed here with their default value.
        self.files = filter(
            lambda x: not path.basename(x)[:1] == '_',
            glob(path.join(here, 'docs', '*.pymd'))
        )

    def finalize_options(self):
        """Post-process options."""
        import re
        if isinstance(self.files, str):
            self.files = filter(lambda s: len(s) > 0, re.split(',', self.files))

    def doctype(self):
        return "markdown"

    def run(self):
        """Run pweave."""
        import traceback
        import pweave

        for file in self.files:
            name = path.splitext(path.basename(file))[0]
            print(_divided('Running %s' % name))
            try:
                pweave.weave(
                    file=str(file),
                    doctype=self.doctype()
                )
            except Exception:
                print(_divided('%s Failed:' % file))
                print(traceback.format_exc())
                exit(1)

class PweaveNotebooks(PweaveDocs):
    def doctype(self):
        return "notebook"

setup(
    name='pyrasterframes',
    description='Access and process geospatial raster data in PySpark DataFrames',
    long_description=readme,
    long_description_content_type='text/markdown',
    version=VERSION,
    author='Astraea, Inc.',
    author_email='info@astraea.earth',
    license='Apache 2',
    url='https://rasterframes.io',
    project_urls={
        'Bug Reports': 'https://github.com/locationtech/rasterframes/issues',
        'Source': 'https://github.com/locationtech/rasterframes',
    },
    install_requires=[
        'pytz',
        'shapely',
        'pyspark<2.4',
        'numpy>=1.7',
        'pandas',
    ],
    setup_requires=[
        'pytz',
        'shapely',
        'pyspark<2.4',
        'numpy>=1.7',
        'matplotlib<3.0.0',
        'pandas',
        'geopandas',
        'requests',
        'pytest-runner',
        'setuptools>=0.8',
        'ipython==6.2.1',
        "ipykernel==4.8.0",
        'Pweave==0.30.3',
        'pyrsistent==0.15.3',
    ],
    tests_require=[
        'pytest==3.4.2',
        'pypandoc',
        'numpy>=1.7',
        'pandas',
    ],
    packages=[
        'pyrasterframes',
        'geomesa_pyspark',
        'pyrasterframes.jars',
    ],
    package_dir={
        'pyrasterframes.jars': 'deps/jars'
    },
    package_data={
        'pyrasterframes.jars': ['*.jar']
    },
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Other Environment',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries',
        'Topic :: Scientific/Engineering :: GIS',
        'Topic :: Multimedia :: Graphics :: Graphics Conversion',
    ],
    zip_safe=False,
    test_suite="pytest-runner",
    cmdclass={
        'pweave': PweaveDocs,
        'notebooks': PweaveNotebooks
    }
)
