FROM gcr.io/pacific-torus-347809/mle-fp/base:latest

# expose port 
EXPOSE 8888

# copy files 
COPY /pipelines ./pipelines/

COPY Makefile .

RUN pip3 install pytest


# set working dir
WORKDIR pipelines/

# run command 
ENTRYPOINT ["/bin/bash"]

