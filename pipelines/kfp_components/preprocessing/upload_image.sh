#!/bin/bash

docker build -t gcr.io/pacific-torus-347809/mle-fp/preprocessing:v1 .

docker push gcr.io/pacific-torus-347809/mle-fp/preprocessing:v1
