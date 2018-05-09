# Corpus is a central coordinator for search engine setup as well as query & analysis activity
#
# The data path folders are organized by convention:
# "Data Folder"          data_folder (e.g., "wikipedia" or "gov2")
# |-- docs               docs_folder (location of all document corpus & index data)
#     |-- manifest.txt     manifest (File listing all chunk files)
#     |-- chunks           chunks_folder (Bitfunnel chunk files)
#     |-- config           Bitfunnel folder (for statistics and termtables)
#     |-- mg4jindex        MG4J index folder
#     |-- pefindex         PEF index folder
# |-- "experiment"       test_folder (location to place query results & logs)
#
# Things you can do with corpus after establishing one:
# .set_chunks_folder(folder, manifest)       # change default "chunks" and "manifest.txt"
# .set_test_name(name)                       # Start a new experiment / test folder

import os
import re
import subprocess
from datetime import datetime

class Corpus:
    def __init__(self,
                 data_folder,         # Location of corpus folder & where to put results
                 mg4j_workbench):     # Location of mg4j-workbench repo holding built jar

        self.data_folder = data_folder
        self.mg4j_jar = os.path.join(mg4j_workbench, "target", "mg4j-1.0-SNAPSHOT.jar")

        self.docs_folder = os.path.join(self.data_folder, "docs")
        self.set_chunks_folder("chunks", "manifest.txt")
        self.set_test_name("experiment")

        self.engines = {}

    # Set the folder (in docs) & manifest file for the document chunks
    # Create manifest file listing all chunk files, if it does not exist
    def set_chunks_folder(self, folder, manifest=None):
        self.chunks_folder = os.path.join(self.docs_folder, folder)

        if manifest is None:
            manifest = folder + "_manifest.txt"
        self.manifest = os.path.join(self.docs_folder, manifest)
        if not os.path.exists(self.manifest):
            args = ("find {0} -type f > {1}").format(self.chunks_folder,
                                                 self.manifest)
            self.execute(args, None)

        return self

    # Build manifest.txt from chunks in folder based on filename match to regular expression
    def create_manifest_from_pattern(self, name, chunk_pattern):
        self.manifest = os.path.join(self.docs_folder, name)
        if os.path.exists(self.manifest):
            return self

        regex = re.compile(chunk_pattern)
        chunks = [os.path.join(self.chunks_folder, f)
                  for root, dirs, files in os.walk(self.chunk_dir)
                  for f in files
                  if regex.search(f) is not None]

        for chunk in chunks:
            print(chunk)

        print("Writing manifest {0}".format(self.manifest))
        with open(self.manifest, 'w') as file:
            for chunk in chunks:
                file.write(chunk + '\n')
        return self

    # Set the name of the current test experiment
    def set_test_name(self, name):
        self.test_folder = os.path.join(self.data_folder, name)
        if not os.path.exists(self.test_folder):
             os.makedirs(self.test_folder)
        return self

    # Run query log across all registered engines, putting results in test_folder
    # If maxthreads is specified, the query is run multiple times varying threads
    #   Results for each thread attempts are captured in a different test folder
    def run_queries(self, querylog, minthreads=1, maxthreads=None):
    
        for engtype, engine in self.engines.items():
            # Bitfunnel runs all thread tests at same time
            if engtype == 'bf':
                engine.run_queries(querylog, minthreads, maxthreads)
            elif maxthreads is None:
                engine.run_queries(querylog, minthreads)
            else:
                save_test_folder = self.test_folder
                for index, threads in enumerate(range(minthreads, maxthreads+1)):
                    self.test_folder = save_test_folder + "_" + str(threads)
                    if not os.path.exists(self.test_folder):
                        print("mkdir " + self.test_folder)
                        os.makedirs(self.test_folder)
                    engine.run_queries(querylog, threads)
                self.test_folder = save_test_folder

        return self

# --------- "Internal" methods, used by engines

    # Register a ready-to-use search engine
    def add_engine(self, type, engine):
        self.engines[type] = engine
        return self

    # Perform command "args" in a subprocess
    def run(self, args, working_directory, logfile = None):
        proc = subprocess.Popen(args, cwd=working_directory, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True);

        if logfile is not None:
            with open(os.path.join(self.test_folder, logfile), 'w') as log:
                print("Running {0} at {1}\n".format(args, str(datetime.now())), end='', file = log)
                for line in proc.stdout:
                    print(line.decode(), end='', file = log)
                    print(line.decode(), end='')
                for line in proc.stderr:
                    print(line.decode(), end='', file = log)
                    print(line.decode(), end='')
        else:
            for line in proc.stdout:
                print(line.decode(), end='')
            for line in proc.stderr:
                print(line.decode(), end='')

        proc.stdout.close()
        returncode = proc.wait()

        # Ensure we don't get zombie processes. This had been a problem with the 7z decompression.
        del proc
        return returncode

    # Execute command and log its results in log file
    def execute(self, command, logfile = None):
        print(command)
        rc = self.run(command, os.getcwd(), logfile)
        print("Finished: {0} return code\n".format(rc))
        return rc

    def mg4j_execute(self, command, logfile = None):
        return self.execute("java -cp {0} -Dfile.encoding=UTF-8 -Xmx16g {1}".format(self.mg4j_jar, command),
                            logfile)