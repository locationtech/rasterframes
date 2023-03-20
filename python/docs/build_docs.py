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


import traceback
from enum import Enum
from glob import glob
from os import path
from typing import List

import pweave
import typer
from pweave import PwebPandocFormatter


# Setuptools/easy_install doesn't properly set the execute bit on the Spark scripts,
# So this preemptively attempts to do it.
def _chmodit():
    try:
        import os
        from importlib.util import find_spec

        module_home = find_spec("pyspark").origin
        print(module_home)
        bin_dir = os.path.join(os.path.dirname(module_home), "bin")
        for filename in os.listdir(bin_dir):
            try:
                os.chmod(os.path.join(bin_dir, filename), mode=0o555, follow_symlinks=True)
            except OSError:
                pass
    except ImportError:
        pass


_chmodit()


class PegdownMarkdownFormatter(PwebPandocFormatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # Pegdown doesn't support the width and label options.
    def make_figure_string(self, figname, width, label, caption=""):
        return "![%s](%s)" % (caption, figname)


app = typer.Typer()


def _dest_file(src_file, ext):
    return path.splitext(src_file)[0] + ext


def _divided(msg):
    divider = "-" * 50
    return divider + "\n" + msg + "\n" + divider


def _get_files():
    here = path.abspath(path.dirname(__file__))
    return list(filter(lambda x: not path.basename(x)[:1] == "_", glob(path.join(here, "*.pymd"))))


class Format(str, Enum):
    html = "html"
    markdown = "markdown"
    notebook = "notebook"
    pandoc2html = "pandoc2html"


@app.command()
def pweave_docs(
    files: List[str] = typer.Option(
        _get_files(), help="Specific files to pweave. Defaults to all in `docs` directory."
    ),
    format: Format = typer.Option(
        Format.markdown, help="Output format type. Defaults to `markdown`"
    ),
    quick: bool = typer.Option(
        False,
        help="Check to see if the source file is newer than existing output before building. Defaults to `False`.",
    ),
):

    """Pweave PyRasterFrames documentation scripts"""

    ext = ".md"
    bad_words = ["Error"]
    pweave.rcParams["chunk"]["defaultoptions"].update({"wrap": False, "dpi": 175})

    if format == Format.markdown:
        pweave.PwebFormats.formats["markdown"] = {
            "class": PegdownMarkdownFormatter,
            "description": "Pegdown compatible markdown",
        }
    elif format == Format.notebook:
        # Just convert to an unevaluated notebook.
        pweave.rcParams["chunk"]["defaultoptions"].update({"evaluate": False})
        ext = ".ipynb"
    elif format == Format.html:
        # `html` doesn't do quite what one expects... only replaces code blocks, leaving markdown in place
        format = Format.pandoc2html

    for file in sorted(files, reverse=False):
        name = path.splitext(path.basename(file))[0]
        dest = _dest_file(file, ext)

        if (not quick) or (not path.exists(dest)) or (path.getmtime(dest) < path.getmtime(file)):
            print(_divided("Running %s" % name))
            try:
                pweave.weave(file=str(file), doctype=format)
                if format == Format.markdown:
                    if not path.exists(dest):
                        raise FileNotFoundError(
                            "Markdown file '%s' didn't get created as expected" % dest
                        )
                    with open(dest, "r") as result:
                        for (n, line) in enumerate(result):
                            for word in bad_words:
                                if word in line:
                                    raise ChildProcessError(
                                        "Error detected on line %s in %s:\n%s" % (n + 1, dest, line)
                                    )

            except Exception:
                print(_divided("%s Failed:" % file))
                print(traceback.format_exc())
                # raise typer.Exit(code=1)
        else:
            print(_divided("Skipping %s" % name))


if __name__ == "__main__":
    app()
