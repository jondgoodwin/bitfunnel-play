#!/bin/bash

# Build MG4J

cd git/mg4j-workbench
git pull
mvn package
cd ../..