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
from setuptools import setup, find_packages
from os import path, environ
import sys
from glob import glob
from io import open
import distutils.cmd
import importlib

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


def _extract_module(mod):
    module = importlib.import_module(mod)

    if hasattr(module, '__all__'):
        globals().update({n: getattr(module, n) for n in module.__all__})
    else:
        globals().update({k: v for (k, v) in module.__dict__.items() if not k.startswith('_')})


class RunExamples(distutils.cmd.Command):
    """A custom command to run pyrasterframes examples."""

    description = 'run pyrasterframes examples'
    user_options = [
        # The format is (long option, short option, description).
        ('examples=', 'e', 'examples to run'),
    ]

    @staticmethod
    def _check_ex_path(ex):

        file = ex
        suffix = path.splitext(ex)[1]
        if suffix == '':
            file += '.py'
        file = path.join(here, 'examples', file)

        assert path.isfile(file), ('Invalid example %s' % file)
        return file

    def initialize_options(self):
        """Set default values for options."""
        # Each user option must be listed here with their default value.
        self.examples = filter(lambda x: not path.basename(x)[:1] == '_',
                               glob(path.join(here, 'examples', '*')))

    def finalize_options(self):
        """Post-process options."""
        import re
        if isinstance(self.examples, str):
            self.examples = filter(lambda s: len(s) > 0, re.split('\W+', self.examples))
        self.examples = map(lambda x: 'examples.' + path.splitext(path.basename(x))[0],
                            map(self._check_ex_path, self.examples))

    def run(self):
        """Run the examples."""
        import traceback
        for ex in self.examples:
            print(('-' * 50) + '\nRunning %s' % ex + '\n' + ('-' * 50))
            try:
                _extract_module(ex)
            except Exception:
                print(('-' * 50) + '\n%s Failed:' % ex + '\n' + ('-' * 50))
                print(traceback.format_exc())


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
        'pyspark==2.3.3',
        'numpy>=1.7',
        'pandas',
    ],
    setup_requires=[
        'pytz',
        'shapely',
        'pyspark==2.3.3',
        'numpy>=1.7',
        'pandas',
        'pytest-runner',
        'setuptools>=0.8',
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
        'examples': RunExamples
    }
)
