# ---------------------
# Base image Dockerfile 
# ---------------------

FROM python:3.8

# --------------
# Image settings 
# --------------

# port
EXPOSE 8888

# working directory 
WORKDIR /project

# copy files 
COPY requirements.txt .

# install requirements file
RUN pip install --upgrade pip &&\ 
    pip install --no-cache-dir -r requirements.txt 
