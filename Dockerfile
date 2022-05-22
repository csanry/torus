FROM python:3.9-bullseye

# expose port 9000
EXPOSE 8888

# set the working directory 
WORKDIR /project

# copy files 
COPY . /project/

# install requirements file
RUN pip install --upgrade pip &&\ 
    pip install --no-cache-dir -r base_requirements.txt &&\
    useradd -ms /bin/bash lab &&\
    cd /project &&\
    make pkg

USER lab

# # set shell to bash
# SHELL ["bin/bash", "-c"]

# to update
CMD ["jupyter-lab","--ip=0.0.0.0","--no-browser","--allow-root"]

