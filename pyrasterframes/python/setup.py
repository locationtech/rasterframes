"""
The operations in this file are designed for development and testing only.
"""


from setuptools import setup, find_packages
import distutils.log
import importlib


def _extract_module(mod):
    module = importlib.import_module(mod)

    if hasattr(module, '__all__'):
        globals().update({n: getattr(module, n) for n in module.__all__})
    else:
        globals().update({k: v for (k, v) in module.__dict__.items() if not k.startswith('_')})



class ExampleCommand(distutils.cmd.Command):
    """A custom command to run pyrasterframes examples."""

    description = 'run pyrasterframes examples'
    user_options = [
        # The format is (long option, short option, description).
        ('examples=', 'e', 'examples to run'),
    ]

    def initialize_options(self):
        from pathlib import Path
        """Set default values for options."""
        # Each user option must be listed here with their default value.
        self.examples = filter(lambda x: not x.name.startswith('_'),
                               list(Path('./examples').resolve().glob('*.py')))

    def _check_ex_path(self, ex):
        from pathlib import Path
        file = Path(ex)
        if not file.suffix:
            file = file.with_suffix('.py')
        file = (Path('./examples') / file).resolve()

        assert file.is_file(), ('Invalid example %s' % file)
        return file

    def finalize_options(self):
        """Post-process options."""
        import re
        if isinstance(self.examples, str):
            self.examples = re.split('\W+', self.examples)
        self.examples = map(lambda x: 'examples.' + x.stem,
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


class ZipCommand(distutils.cmd.Command):
    """A custom command to create a minimal zip distribution."""

    description = 'create a minimal pyrasterframes source zip file'
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        """Create the zip."""
        import zipfile
        from pathlib import Path
        import os
        zfile = 'pyrasterframes.zip'

        if os.path.isfile(zfile):
            os.remove(zfile)
        with zipfile.ZipFile(zfile, 'w') as przip:
            przip.write('pyrasterframes')
            # Bring in source files and readme
            patterns = ['*.py', '*.rst', '*.jar']
            root = Path('.').resolve()
            for pattern in patterns:
                for file in list(root.glob('pyrasterframes/' + pattern)):
                    przip.write(str(file.relative_to(root)))
            # Put a copy of the license in the zip
            przip.write('LICENSE.md', 'pyrasterframes/LICENSE.md')





with open('README.rst') as f:
    readme = f.read()

#pyspark_ver = 'pyspark>=2.2.0,<2.3'
pyspark_ver = 'pyspark==2.3.2'

setup_args = dict(
    name='pyrasterframes',
    description='Python bindings for RasterFrames',
    long_description=readme,
    version='0.0.1',
    url='http://rasterframes.io',
    author='D. Benjamin Guseman',
    author_email='guseman@astraea.io',
    license='Apache 2',
    setup_requires=['pytest-runner', pyspark_ver, 'pathlib'],
    install_requires=[
        # pyspark_ver,
        # 'pathlib'
    ],
    tests_require=[
        pyspark_ver,
        'pytest==3.4.2',
        'pypandoc',
        'numpy>=1.7'
    ],
    test_suite="pytest-runner",
    packages=find_packages(exclude=['tests', 'examples']),
    include_package_data=True,
    package_data={'.':['LICENSE.md'], 'pyrasterframes':['*.jar']},
    exclude_package_data={'.':['setup.*', 'README.*']},
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Other Environment',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries'
    ],
    zip_safe=False,
    cmdclass={
        'examples': ExampleCommand,
        'minzip': ZipCommand
    }
    # entry_points={
    #     "console_scripts": ['pyrasterframes=pyrasterframes:console']
    # }
)

if __name__ == "__main__":
    setup(**setup_args)
