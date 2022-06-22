#!/bin/bash

docker build -t gcr.io/pacific-torus-347809/mle-fp/ingest:latest .

docker push gcr.io/pacific-torus-347809/mle-fp/ingest:latest
