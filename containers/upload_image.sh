#!/bin/bash

docker build -t gcr.io/pacific-torus-347809/mle-fp/base/latest .

docker push gcr.io/pacific-torus-347809/mle-fp/base:latest


# to pull 
# docker pull gcr.io/pacific-torus-347809/mle-fp/base:latest