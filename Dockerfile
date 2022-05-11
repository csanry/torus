FROM python:3.9.12-bullseye

# expose port 9000
EXPOSE 9000

# set the working directory 
WORKDIR /usr/

# copy files 
COPY . .


# install requirements file
RUN pip install --no-cache-dir -r requirements.txt


# set shell to bash
# SHELL ["bin/bash", "-c"]

# to update
CMD ["python3", "src/app/testing_container.py"]

