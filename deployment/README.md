# RasterFrames Jupyter Notebook Docker Container

RasterFrames provides a Docker image with a Jupyter Notebook pre-configured with RasterFrames support for Python 3 and Scala Spylon kernels.

## Quick start

This will use the [latest image](https://hub.docker.com/r/s22s/rasterframes-notebooks/) published to Docker Hub. 

```bash
# Optionally pull the latest image.
$ docker pull s22s/rasterframes-notebooks

# from root of the git repo
$ cd deployment/docker/jupyter
$ docker-compose up
```

## Custom run

The `docker-compose` incantation automatically exposes port 8888 for the Jupyter Notebook and ports ports 4040-4044 for the Spark UI.

The image can equivalently be run with: 

    $ docker run -it --rm -p 8888:8888 -p 4040-4044:4040-4044 s22s/rasterframes-notebooks

The `docker run` command can be changed to quickly customize the container.

To mount a directory on the host machine (to load or save local files directly from Jupyter) add
 
    -v /some/host/folder/for/work:/home/jovyan/work
    
to the command.    

Attach the notebook server to a different host port with 
 
    -p 8630:8888
    
if you already have a notebook server running on port 8888.

If you want to use a known password, use

```bash
docker run -it --rm -p 8888:8888 -p 4040-4044:4040-4044 \
    s22s/rasterframes-notebooks \
    start-notebook.sh --NotebookApp.password='sha1:1c360e8dd3e1:946d17ef9e6b8cbb28c7bb0152329786918cc424'
```
    
Where the password sha is generated with [`notebook.auth.passwd`](https://jupyter-notebook.readthedocs.io/en/stable/public_server.html#preparing-a-hashed-password).

Please see the `Dockerfile` and the `docker-compose.yml` file on GitHub ([here](https://github.com/locationtech/rasterframes/tree/develop/deployment/docker/jupyter)) as a starting point to customize your image and container. 


## For Development

To build the Docker image based on local development changes:

```bash
# from the root of the repo
sbt deployment/rfNotebookContainer
```

## Base images

This image is based on [jupyter/pyspark-notebook](https://hub.docker.com/r/jupyter/pyspark-notebook), with some 
portions from [jupyter/all-spark-notebook](https://hub.docker.com/r/jupyter/all-spark-notebook). 
Much more extensive instructions can be found at those locations.