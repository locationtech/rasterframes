# RasterFrames Jupyter Notebook Docker Container

Please see the Dockerfile and the docker-compose.yml file on GitHub 
([here](https://github.com/locationtech/rasterframes/tree/develop/deployment/docker/jupyter)) 
for ease of use with `docker-compose` (e.g. `docker-compose up`). The key detail is to map 
port 8888, and ports 4040-4044 if you would like to use the Spark-UI.

Alternately, the image can be run with: 

    docker run -it --rm -p 8888:8888 -p 4040-4044:4040-4044 s22s/rasterframes-notebooks

To mount a directory on the host machine (to load or save local files directly from Jupyter) add
 
    -v /some/host/folder/for/work:/home/jovyan/work
    
to the command.    

This image is based on [jupyter/pyspark-notebook](https://hub.docker.com/r/jupyter/pyspark-notebook), with some 
portions from [jupyter/all-spark-notebook](https://hub.docker.com/r/jupyter/all-spark-notebook). 
Much more extensive instructions can be found at those locations.