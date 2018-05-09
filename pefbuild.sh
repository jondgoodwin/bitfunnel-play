#!/bin/bash

# Build Paritioned Elias Fano
# CMAKE_EXPORT_COMPILE_COMMANDS generates a compile_commands.json file containing the exact compiler calls

cd cmake
rm -rf pef
mkdir pef
cd pef
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=1 -G "Unix Makefiles" ../../git/pef
make

# bin/create_freq_index opt /mnt/d/temp/PartitionedEliasFano/export ~/data/ten.index.opt
# bin/Runner opt \
#           ~/data/ten.index.opt \
#            /mnt/d/temp/PartitionedEliasFano/export-filtered-ints.txt \
#            8 \
#            ~/data/out.csv

sudo ln -fs /bf/cmake/pef/bin/create_freq_index /usr/bin/pefindex
sudo ln -fs /bf/cmake/pef/bin/Runner /usr/bin/pefrunner
cd ../..