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
        ('files=', 's', 'Specific files to pweave. Defaults to all in `docs` directory.'),
        ('format=', 'f', 'Output format type. Defaults to `markdown`'),
        ('quick=', 'q', 'Check to see if the source file is newer than existing output before building. Defaults to `False`.')
    ]


    def initialize_options(self):
        """Set default values for options."""
        # Each user option must be listed here with their default value.
        self.files = filter(
            lambda x: not path.basename(x)[:1] == '_',
            glob(path.join(here, 'docs', '*.pymd'))
        )
        self.format = 'markdown'
        self.quick = False

    def finalize_options(self):
        """Post-process options."""
        import re
        if isinstance(self.files, str):
            self.files = filter(lambda s: len(s) > 0, re.split(',', self.files))
            # `html` doesn't do quite what one expects... only replaces code blocks, leaving markdown in place
            print("format.....", self.format)
            if self.format == 'html':
                self.format = 'pandoc2html'
        if isinstance(self.quick, str):
            self.quick = self.quick == 'True' or self.quick == 'true'

    def dest_file(self, src_file):
        return path.splitext(src_file)[0] + '.md'

    def run(self):
        """Run pweave."""
        import traceback
        import pweave
        from docs import PegdownMarkdownFormatter

        bad_words = ["Error"]
        pweave.rcParams["chunk"]["defaultoptions"].update({'wrap': False, 'dpi': 175})
        if self.format == 'markdown':
            pweave.PwebFormats.formats['markdown'] = {
                'class': PegdownMarkdownFormatter,
                'description': 'Pegdown compatible markdown'
            }

        for file in sorted(self.files, reverse=True):
            name = path.splitext(path.basename(file))[0]
            dest = self.dest_file(file)

            if (not self.quick) or (not path.exists(dest)) or (path.getmtime(dest) < path.getmtime(file)):
                print(_divided('Running %s' % name))
                try:
                    pweave.weave(file=str(file), doctype=self.format)
                    if self.format == 'markdown':
                        if not path.exists(dest):
                            raise FileNotFoundError("Markdown file '%s' didn't get created as expected" % dest)
                        with open(dest, "r") as result:
                            for (n, line) in enumerate(result):
                                for word in bad_words:
                                    if word in line:
                                        raise ChildProcessError("Error detected on line %s in %s:\n%s" % (n + 1, dest, line))

                except Exception:
                    print(_divided('%s Failed:' % file))
                    print(traceback.format_exc())
                    exit(1)
            else:
                print(_divided('Skipping %s' % name))


class PweaveNotebooks(PweaveDocs):
    def initialize_options(self):
        super().initialize_options()
        self.format = 'notebook'

    def dest_file(self, src_file):
        return path.splitext(src_file)[0] + '.ipynb'

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
        'Shapely>=1.6.0',
        'pyspark<2.4',
        'numpy>=1.7',
        'pandas>=0.25.0',
    ],
    setup_requires=[
        'pytz',
        'Shapely>=1.6.0',
        'pyspark<2.4',
        'numpy>=1.7',
        'matplotlib<3.0.0',
        'pandas>=0.25.0',
        'geopandas',
        'requests',
        'pytest-runner',
        'setuptools>=0.8',
        'ipython==6.2.1',
        'ipykernel==4.8.0',
        'Pweave==0.30.3',
        'fiona==1.8.6',
        'rasterio>=1.0.0',  # for docs
        'folium',
    ],
    tests_require=[
        'pytest==3.4.2',
        'pypandoc',
        'numpy>=1.7',
        'Shapely>=1.6.0',
        'pandas>=0.25.0',
        'rasterio>=1.0.0',
        'boto3',
        'Pweave==0.30.3',
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
