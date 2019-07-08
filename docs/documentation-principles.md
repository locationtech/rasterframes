# Documentation Principles

This document outlines some concrete considerations for the planned rewrite of the _RasterFrames Users' Manual_. 
See [`docs/README.md`](https://github.com/locationtech/rasterframes/blob/develop/docs/README.md) for technical details on the mechanics of building the documentation.

## Title

The project is "RasterFrames". Documentation shall use the name "RasterFrames". The RasterFrames runtime is deployed in two forms: `rasterframes` (for Scala/Java/SQL-only), and `pyrasterframes` (Python). But the user should know and think of the project as one thing: RasterFrames.

## Format

The documentation shall be rendered in Markdown (`.md`) and Python Markdown (`.pymd`). The source of this documentation is committed in the same project as the code, in `pyrasterframes/src/main/python/docs`. Additional details on processing the docs can be found [here](https://github.com/locationtech/rasterframes/tree/develop/pyrasterframes#running-python-markdown-sources).

Filenames shall be in skewer case; lower case with dashes ('-') separating words. For example, `foo-in-bar-with-baz.md`.

## Target Audience

The target audience for the _RasterFrames User's Manual_ is the intermediate data scientist or developer, already adept at either Python or Scala. Eventually this should be expanded to include SQL adepts. This user may or may not be an expert at EO data usage, so attention to jargon, undefined terms, new concepts etc. should be kept in mind, making use of authoritative external resources to fill in knowledge. 

> Enumerate concepts they are aware of, including:
> * Scene discretization
> * Temporal revisit rate
> * Spatial resolution
> * Spatial extent

The user may or may not be familiar with Apache Spark, so they should also be guided in filling in minimum requisite knowledge. At a minimum we have to explain what a `SparkSession` is, and that we have to configure it; note the difference between an "action" and "transformation"; what a "collect" action is, and the consequences if the data is large; awareness of partitioning.

There's also an opportunity to emphasize the scalability benefits over, say, a rasterio/Pandas-only solution (but that we interop with them too).

The users' goals with EO data are could be from a number of different perspectives:

* Creating map layers
* Statistical analysis
* Machine learning
* Change detection
* Chip preparation

While the emphasis of the documentation should be on the core capabilities (and flexibility therein) of RasterFrames, a nod toward these various needs in the examples shown can go a long way in helping the user understand appropriateness of the library to their problem domain.

## Pedagogical Technique

The documentation shall emphasize the use of executable code examples, with interspersed prose to explain them. The RasterFrames tooling supports the `.pymd` (Python Markdown) format whereby delimited code blocks are evaluated at build time in order to include output/results. The project currently uses ['Pweave'](http://mpastell.com/pweave/chunks.html) to do this (this may change, but `.pymd` will remain the source format). `Pweave` also has the ability to convert `.pymd` to Jupyter Notebooks, which may serve useful. Through this process we can be assured that any examples shown are code the user can copy into their own projects.

Visuals are always helpful, but even more so when there's a lot of code involved that needs continual contextualization and explanation. `Pweave` supports rendering of `matplotlib` charts/images, a capability we should make use of. Furthermore, where beneficial, we should create diagrams or other visuals to help express concepts and relationships.

This "code-first" focus is admittedly in tension with the competing need to explain some of the more abstract aspects of distributed computing necessary for advanced monitoring, profiling, optimization, and deployment. We should evolve the documentation over the long term to address some of these needs, but in the near term the focus should be on the core conceptual model necessary for understanding tile processing. Diagrams can be helpful here.
 
## Polyglot Considerations

In terms of implementation, RasterFrames is a Scala project first. All algorithmic, data modeling, and heavy lifting, etc. are implemented in Scala.

However, due to user base preferences, RasterFrames is primarily _deployed_ through Python. As such, documentation, examples, etc. should first be implemented in Python. 

Secondarily to that, SQL should be used to highlight the analyst-friendly expression of the functionality in SQL. At least a handful of examples in a dedicated SQL page would go far in showing the cross-language support.

Thirdly, Scala developers should be encouraged to use the platform, clearly stating that the APIs are on equal footing, using consistent naming conventions, etc. and that most examples should translate almost one-to-one.

In the long term I'd love to see Python, Scala, and SQL all treated in equal footing, with examples expressed in all languages, but that's a tall order this early in the project development.

## User Journey

As noted in the _Target Audience_ section, the documentation needs to guide the user through process from curiosity around EO data to scalable processing of it. Within the first section or so the user should see an example that reads an image and does something somewhat compelling with it, noting that the same code will work on a laptop with a small amount of imagery, as well as on 100s of computers with TB (or more) of imagery. Problems "solved in the small" can be grown to "global scale".

With a "journey" focus, concepts and capabilities are introduced incrementally, building upon previous examples and concepts, adding more complexity as it develops. Once the fundamentals are covered, we then move into the examples that are closer to use-cases or cookbook entries.

The preliminary outline is a follows, but is open for refinement, rethinking, etc.

1. Description
2. Architecture
3. Getting Started
    * `pyspark` shell
    * Jupyter Notebook
    * Standalone Python Script
4. Raster Data I/O
    * Reading Raster Data
    * Writing Raster Data
5. Spatial Relations
6. Raster Processing
    * Local Algebra
    * “NoData” Handling
    * Aggregation
    * Time Series
    * Spark ML Support
    * Pandas and NumPy Interoperability
7. Cookbook Examples
8. Extended Examples / Case Studies
9. Function Reference

## Hiccups

During the documentation process we are likely to run into problems whereby the goal of the writer is inhibited by a bug or capability gap in the code. We should use this opportunity to improve the library to provide the optimal user experience before resorting to workarounds or hacks or addition of what might seem to be arbitrary complexity.

## Testing

To "be all that we can be", testing the documentation against a new user is a boon. It may be hard to capture volunteers to do this, but we should consider enlisting interns and friends of the company to go through the documentation and give feedback on where gaps exist.



