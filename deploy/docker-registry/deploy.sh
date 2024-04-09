#!/bin/bash

dir=$(dirname $(realpath $0))
mkdir "${dir}/data"

docker run -d -p 5000:5000 --restart=always --name registry \
             -v ${dir}/config.yml:/etc/docker/registry/config.yml \
             -v ${dir}/data:/var/lib/registry \
             registry
