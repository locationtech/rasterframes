# RasterFrames Jupyter Notebook Docker Image

RasterFrames provides a Docker image with a Jupyter Notebook pre-configured with RasterFrames in the PySpark kernel.

## Launching RasterFrames Notebook from Source

First, have Docker installed.

Then, from the source root directory, run

```bash
sbt rf-notebook/startRFNotebook
```

This will build the project, create a Docker image, and start an instance.

To stop it:

```bash
sbt rf-notebook/stopRFNotebook
```

The above commands are just conveniences. The real work is done by `docker-compose` via the [`docker-compose.yml`](src/main/docker/docker-compose.yml) file in `rf-notebook/src/main/docker/docker-compose.yml`. By default it exposes and binds to ports `8888` and `4040-4044`.


## Docker Image Build

If you just want to build the image:

```bash
sbt rf-notebook/publishLocal
``` 
