from os import path
import traceback
import pweave
from ..docs import PegdownMarkdownFormatter
import typer
from typing import List
from glob import glob
from enum import Enum

app = typer.Typer()

def _dest_file(src_file, ext):
    return path.splitext(src_file)[0] + ext

def _divided(msg):
    divider = ('-' * 50)
    return divider + '\n' + msg + '\n' + divider

def _get_files():
    # TODO move one up
    here = path.abspath(path.dirname(__file__))

    return filter(
            lambda x: not path.basename(x)[:1] == '_',
            glob(path.join(here, 'docs', '*.pymd'))
        )

class Format(str, Enum):
    html = "html"
    markdown = "makedown"
    notebook = "notebook"

@app.command()
def pweave_docs(
        files: List[str] = typer.Option(_get_files(), help="Specific files to pweave. Defaults to all in `docs` directory."),
        format: Format = typer.Option(Format.markdown, help="Output format type. Defaults to `markdown`"),
        quick: bool = typer.Option(False, help="Check to see if the source file is newer than existing output before building. Defaults to `False`.")                                                     ):
    
        """Pweave PyRasterFrames documentation scripts"""
        # `html` doesn't do quite what one expects... only replaces code blocks, leaving markdown in place
        if format == "html":
            format == "pandoc2html"

        if format == "notebook":
            ext = ".ipynb"
        else:
            ext = ".md"
       
        bad_words = ["Error"]
        pweave.rcParams["chunk"]["defaultoptions"].update({'wrap': False, 'dpi': 175})
        if format == 'markdown':
            pweave.PwebFormats.formats['markdown'] = {
                'class': PegdownMarkdownFormatter,
                'description': 'Pegdown compatible markdown'
            }
        if format == 'notebook':
            # Just convert to an unevaluated notebook.
            pweave.rcParams["chunk"]["defaultoptions"].update({'evaluate': False})

        for file in sorted(files, reverse=False):
            name = path.splitext(path.basename(file))[0]
            dest = _dest_file(file, ext)

            if (not quick) or (not path.exists(dest)) or (path.getmtime(dest) < path.getmtime(file)):
                print(_divided('Running %s' % name))
                try:
                    pweave.weave(file=str(file), doctype=format)
                    if format == 'markdown':
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
                    raise typer.Exit(code=1)
            else:
                print(_divided('Skipping %s' % name))


if __name__ == "__main__":
    app()