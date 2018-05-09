#!/bin/bash

# Build BitFunnel
# CMAKE_EXPORT_COMPILE_COMMANDS generates a compile_commands.json file containing the exact compiler calls

cd git/BitFunnel
git pull
cd ../../cmake
rm -rf BitFunnel
mkdir BitFunnel
cd BitFunnel
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=1 -G "Unix Makefiles" ../../git/BitFunnel
make
sudo ln -fs /bf/cmake/BitFunnel/tools/BitFunnel/src/BitFunnel /usr/bin/bitfunnel
cd ../..