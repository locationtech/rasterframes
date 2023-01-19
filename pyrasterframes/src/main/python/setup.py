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
from os import path, environ, mkdir
import sys
from glob import glob
from io import open
import distutils.cmd

try:
    enver = environ.get('RASTERFRAMES_VERSION')
    if enver is not None:
        open('pyrasterframes/version.py', mode="w").write(f"__version__: str = '{enver}'\n")
    exec(open('pyrasterframes/version.py').read())  # executable python script contains __version__; credit pyspark
except IOError as e:
    print(e)
    print("Try running setup via `sbt 'pySetup arg1 arg2'` to ensure correct access to all source files and binaries.")
    sys.exit(-1)

VERSION = __version__
print(f"setup.py sees the version as {VERSION}")

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
            if self.format.strip() == 'html':
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
        if self.format == 'notebook':
            # Just convert to an unevaluated notebook.
            pweave.rcParams["chunk"]["defaultoptions"].update({'evaluate': False})

        for file in sorted(self.files, reverse=False):
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

# WARNING: Changing this version bounding will result in branca's use of jinja2
# to throw a `NotImplementedError: Can't perform this operation for unregistered loader type`
pytest = 'pytest>=4.0.0,<5.0.0'

pyspark = 'pyspark==3.2.1'
boto3 = 'boto3'
deprecation = 'deprecation'
descartes = 'descartes'
matplotlib = 'matplotlib'
fiona = 'fiona'
folium = 'folium'
gdal = 'gdal'
geopandas = 'geopandas'
ipykernel = 'ipykernel'
ipython = 'ipython'
numpy = 'numpy'
pandas = 'pandas'
pypandoc = 'pypandoc'
pyproj = 'pyproj'
pytest_runner = 'pytest-runner'
pytz = 'pytz'
rasterio = 'rasterio'
requests = 'requests'
setuptools = 'setuptools'
shapely = 'Shapely'
tabulate = 'tabulate'
tqdm = 'tqdm'
utm = 'utm'

# Documentation build stuff. Until we can replace pweave, these pins are necessary
pweave = 'pweave==0.30.3'
jupyter_client = 'jupyter-client<6.0'  # v6 breaks pweave
nbclient = 'nbclient==0.1.0'  # compatible with our pweave => jupyter_client restrictions
nbconvert = 'nbconvert==5.5.0'

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
    python_requires=">=3.7",
    install_requires=[
        gdal,
        pytz,
        shapely,
        pyspark,
        numpy,
        pandas,
        pyproj,
        tabulate,
        deprecation,
    ],
    setup_requires=[
        pytz,
        shapely,
        pyspark,
        numpy,
        matplotlib,
        pandas,
        geopandas,
        requests,
        pytest_runner,
        setuptools,
        ipython,
        pweave,
        jupyter_client,
        nbclient,
        nbconvert,
        fiona,
        rasterio,
        folium,
    ],
    tests_require=[
        pytest,
        pypandoc,
        numpy,
        shapely,
        pandas,
        rasterio,
        boto3,
        pweave
    ],
    packages=[
        'pyrasterframes',
        'geomesa_pyspark',
        'pyrasterframes.jars',
    ],
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
        'Programming Language :: Python :: 3',
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
