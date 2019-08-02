# RasterFrames Documentation

The conceptual principles to consider when writing RasterFrames users' documentation are covered in [Documentation Principles](documentation-principles.md). This document covers the mechanics of writing, evaluating, and building the documentation during the writing process.

## Organization

The documentation build is a two step process, whereby two sources (three if API docs are included) are merged together and converted into a static HTML website. They are:

* Technical content and Python examples: `<src-root>/pyrasterframes/src/main/python/docs`
* Global documentation assets and Scala specific content: `<src-root>/docs/src/main/paradox`

The build constructs in `<src-root>/docs` are (due to legacy reasons) the top-level mechanisms of bringing it all together, but the meat of the content is in  `<src-root>/pyrasterframes/...`, and will be the focus of most of this document.

## Prerequisites

* [`sbt`](https://www.scala-sbt.org/)
* Python 3
* Markdown editor. [Visual Studio Code](https://code.visualstudio.com/) with [`language-weave` extension](https://marketplace.visualstudio.com/items?itemName=jameselderfield.language-weave) is one option. [Atom](https://atom.io/) is another which might actually have better support for evaluating code in Markdown, but I've not tried it.

> Note: If you're using Visual Studio Code, you can associate the `.pymd` with the `language-weave` plugin by adding this to your `settings.json` file.
      
```json
"files.associations": {
 "*.pymd": "pweave_md"
}
```

## Content Development Process

Start with one of the existing files in `<src-root>/pyrasterframes/src/main/python/docs` as a template. [`local-algebra.pymd`](../pyrasterframes/src/main/python/docs/local-algebra.pymd) is a good example. If the content will have code blocks you want evaluated an results injected into the output, use the file extension `.pymd`. If the content doesn't use evaluatable code blocks, use `.md`.

All `.pymd` files are processed with a tool called [Pweave](http://mpastell.com/pweave), which produces a regular Markdown file where identified code blocks are evaluated and their results (optionally) included in the text. Matplot lib is supported! It is much like `knitr` in the R community. If we run into issues with Pweave, we can also consider [`knitpy`](https://github.com/jankatins/knitpy) or [`codebraid`](https://github.com/gpoore/codebraid). Codebraid looks particularly powerful, so we may think to transition to it.

Pweave has a number of [code chunk options](http://mpastell.com/pweave/chunks.html) for controlling the output. Refer to those documents on details, and experiment a little to see what conveys your intent best.

To set up an environment whereby you can easily test/evaluate your code blocks during writing:

1. Run `sbt` from the `<src-root>` directory. You should get output that looks something like:  
    ```
    $ sbt
    ...
    [info] Loading settings for project pyrasterframes from build.sbt ...
    [info] Loading settings for project rf-notebook from build.sbt ...
    [info] Set current project to RasterFrames (in build file:<src-root>/)
    sbt:RasterFrames>    
    ```  
2. The first time you check out the code, or whenever RasterFrames code is updated, you need to build the project artifacts so they are available for Pweave.
    ```
    sbt:RasterFrames> pyrasterframes/package
    [info] Compiling 4 Scala sources to <src-root>/core/target/scala-2.11/classes ...
   ... lots of noise ...
   [info] PyRasterFrames assembly written to '<src-root>/pyrasterframes/target/python/deps/jars/pyrasterframes-assembly-0.8.0-SNAPSHOT.jar'
   [info] Synchronizing 44 files to '<src-root>/pyrasterframes/target/python'
   [info] Running 'python setup.py build bdist_wheel' in '<src-root>/pyrasterframes/target/python'
   ... more noise ...
   [info] Python .whl file written to '<src-root>/pyrasterframes/target/python/dist/pyrasterframes-0.8.0.dev0-py2.py3-none-any.whl'
   [info] Maven Python .zip artifact written to '<src-root>/pyrasterframes/target/scala-2.11/pyrasterframes-python-0.8.0-SNAPSHOT.zip' 
    [success] Total time: 83 s, completed Jul 5, 2019 12:25:48 PM
    sbt:RasterFrames>
    ```
3. To evaluate all the `.pymd` files, run:
    ```
    sbt:RasterFrames> pyrasterframes/pySetup pweave
    ```
    To build the artifact (step 1) and evaluate all the `.pymd` files, you can run:
    ```
    sbt:RasterFrames> pyrasterframes/doc
    ```
    There's a command alias for this last step: `pyDocs`.
4. To evaluate a single `.pymd` file, you pass the `-s` option and the filename relative to the `python` directory:
    ```
    sbt:RasterFrames> pyrasterframes/pySetup pweave -s docs/getting-started.pymd
    [info] Synchronizing 44 files to '<src-root>/pyrasterframes/target/python'
    [info] Running 'python setup.py pweave -s docs/getting-started.pymd' in '<src-root>/pyrasterframes/target/python'
    running pweave
    --------------------------------------------------
    Running getting-started
    --------------------------------------------------
    status
    status
    Processing chunk 1 named None from line 14
    ...
    Weaved docs/getting-started.pymd to docs/getting-started.md
    ```
5. The _output_ Markdown files are written to `<src-root>/pyrasterframes/target/python/docs`. _Note_: don't edit any files in the `pyrasterframes/target` directory... they will get overwritten each time `sbt` runs a command.
6. During content development it's sometimes helpful to see the output rendered as basic HTML. To do this, add the `-d html` option to the pweave command:
    ```
    sbt:RasterFrames> pyrasterframes/pySetup pweave -f html -s docs/getting-started.pymd
    [info] Synchronizing 54 files to '<src-roog>/pyrasterframes/target/python'
    [info] Running 'python setup.py pweave -f html -s docs/getting-started.pymd' in '<src-root>/pyrasterframes/target/python'
    running pweave
    --------------------------------------------------
    Running getting-started
    --------------------------------------------------
    ...
    Weaved docs/getting-started.pymd to docs/getting-started.html
    ```
    Note: This feature requires `pandoc` to be installed.
7. To build all the documentation and convert to a static html site, run:
    ```bash
    sbt makeSite
    ``` 
    Results will be found in `<src-root>/docs/target/site`.

## Notebooks

The `rf-notebooks` sub-project creates a Docker image with Jupyter Notebooks pre-configured with RasterFrames. Any `.pymd` file under `.../python/docs/` is converted to an evaluated Jupyter Notebook and included as a part of the build (an additional bonus of the Pweave tool). 

## Submission Process

Submit new and updated documentation as a PR against locationtech/rasterframes. Make sure you've signed the Eclipse Foundation ECA and you ["Signed-off-by:"](https://stackoverflow.com/questions/1962094/what-is-the-sign-off-feature-in-git-for) each commit in the PR. The "Signed-off-by" address needs to be the exact same one as registered with the [Eclipse Foundation](https://wiki.eclipse.org/Development_Resources/Contributing_via_Git).
