FROM gcr.io/pacific-torus-347809/mle-fp/base:latest

# expose port 
EXPOSE 8888

# copy files 
COPY /pipelines ./pipelines/

# set working dir
WORKDIR pipelines/

# run command 
CMD ["python3", "train_pipeline.py"]

