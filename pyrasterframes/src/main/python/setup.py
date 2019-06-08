# Always prefer setuptools over distutils
from setuptools import setup, find_packages
import os
from os import path, environ
from io import open
from pathlib import Path
import distutils.cmd
import importlib

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    readme = f.read()

with open(path.join(here, 'requirements.txt')) as f:
    requirements = f.read().splitlines()


def _extract_module(mod):
    module = importlib.import_module(mod)

    if hasattr(module, '__all__'):
        globals().update({n: getattr(module, n) for n in module.__all__})
    else:
        globals().update({k: v for (k, v) in module.__dict__.items() if not k.startswith('_')})


def _divided(msg):
    divider = ('-' * 50)
    return divider + '\n' + msg + '\n' + divider


class RunExamples(distutils.cmd.Command):
    """A custom command to run pyrasterframes examples."""

    description = 'Run PyRasterFrames examples'
    user_options = [
        # The format is (long option, short option, description).
        ('examples=', 'e', 'examples to run'),
    ]

    @staticmethod
    def _check_ex_path(ex):
        file = Path(ex)
        if not file.suffix:
            file = file.with_suffix('.py')
        file = (Path(here) / 'examples' / file).resolve()

        assert file.is_file(), ('Invalid example %s' % file)
        return file

    def initialize_options(self):
        """Set default values for options."""
        # Each user option must be listed here with their default value.
        self.examples = filter(lambda x: not x.name.startswith('_'),
                               list((Path(here) / 'examples').resolve().glob('*.py')))

    def finalize_options(self):
        """Post-process options."""
        import re
        if isinstance(self.examples, str):
            self.examples = filter(
                lambda s: len(s) > 0,
                re.split('\W+', self.examples)
            )
        self.examples = map(
            lambda x: 'examples.' + x.stem,
            map(self._check_ex_path, self.examples)
        )

    def run(self):
        """Run the examples."""
        import traceback

        for ex in self.examples:
            print(_divided('Running %s' % ex))
            try:
                _extract_module(ex)
            except Exception:
                print(_divided('%s Failed:' % ex))
                print(traceback.format_exc())


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
            lambda x: not x.name.startswith('_'),
            list((Path(here) / 'docs').resolve().glob('*.py'))
        )

    def finalize_options(self):
        """Post-process options."""
        import re
        if isinstance(self.files, str):
            self.files = filter(lambda s: len(s) > 0, re.split('\W+', self.files))

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
                    doctype='markdown'
                )
            except Exception:
                print(_divided('%s Failed:' % file))
                print(traceback.format_exc())

setup(
    name='PyRasterFrames',
    description='RasterFrames for PySpark',
    long_description=readme,
    long_description_content_type='text/markdown',
    version=environ.get('RASTERFRAMES_VERSION', 'dev'),
    author='Astraea, Inc.',
    author_email='info@astraea.earth',
    license='Apache 2',
    url='https://rasterframes.io',
    project_urls={
        'Bug Reports': 'https://github.com/locationtech/rasterframes/issues',
        'Source': 'https://github.com/locationtech/rasterframes',
    },
    install_requires=requirements,
    setup_requires=[
        'pytest-runner',
        'setuptools>=0.8',
        'pathlib2',
        'ipython==6.2.1',
        "ipykernel==4.8.0",
        'Pweave==0.30.3'
    ] + requirements,
    tests_require=[
        'pytest==3.4.2',
        'pypandoc',
        'numpy>=1.7',
        'pandas',
    ],
    packages=[
        'pyrasterframes',
        'geomesa_pyspark'
    ],
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Other Environment',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries'
    ],
    zip_safe=False,
    test_suite="pytest-runner",
    cmdclass={
        'examples': RunExamples,
        'pweave': PweaveDocs
    }
    # entry_points={
    #     "console_scripts": ['pyrasterframes=pyrasterframes:console']
    # }
)
