#!/bin/bash

# Install the Ubuntu packages required to build the C++ and Java-based search engines.
# (The -y option auto-answers "yes" to the confirmation prompt)
# - cmake is the build tool used by C++ programs
# - build-essential ensures we have a CMake-usable g++ compiler
# - default-jre ensures we have OpenJava 8
# - maven is the Java build automation tool

sudo apt-get update
sudo apt-get -y install cmake build-essential libboost-all-dev default-jre default-jdk maven

# Set up standard folder structure for BitFunnel

cd /bf
mkdir scripts
export PATH=$PATH:/bf/scripts
mkdir data
mkdir cmake
mkdir git

# Retrieve the necessary code/data repositories

cd git
git clone https://github.com/BitFunnel/BitFunnel.git BitFunnel
git clone https://github.com/BitFunnel/mg4j-workbench.git mg4j-workbench
git clone --recursive https://github.com/BitFunnel/partitioned_elias_fano.git pef
git clone https://github.com/BitFunnel/sigir2017-bitfunnel.git sigir2017-bitfunnel
cd ..

# Build BitFunnel and PEF

# ./bfbuild.sh
# ./pefbuild.sh
# ./mg4jbuild.sh