# Things you can do with BitFunnel after establishing it
# .copy_chunks(newfolder, filter, annotate)  # create an altered copy of current chunks
# .build_index(density, treatment)           # Run statistics and termtable, if not existing
# .run_queries(querylog, threads)            # Run queries and store results

import os

class BitFunnel:
    def __init__(self,
                 corpus,                          # Corpus object handling docs & queries
                 bf_executable,                   # Full path to BitFunnel program
                 memory = None):                  # How much memory to use when running BitFunnel

        self.corpus = corpus
        corpus.add_engine('bf', self)

        self.bf_executable = bf_executable
        if memory == None:
            self.memory = ""
        else:
            self.memory = "-memory {0}".format(memory)

    # Create a copy of the corpus chunks using BitFunnel filter
    # Filter examples:
    #    -size 256 4095     (posting count range for each doc.)
    #    -count 1000        (# of documents to copy)
    #    -random 4301 0.25  (copy a random quarter of the docs)
    # Specify 'annotate' for annotate to inject shard terms into corpus
    def copy_chunks(self, newfolder, filter=None, annotate=None):

        self.corpus.docs_folder = os.path.join(self.corpus.data_folder, newfolder)
        if not os.path.exists(self.corpus.docs_folder):
            os.makedirs(self.corpus.docs_folder)
            new_chunks_folder = os.path.join(self.corpus.docs_folder, "chunks")
            os.makedirs(new_chunks_folder)
        
            if annotate is None:
                annotate = ""
            else:
                annotate = "-writer " + annotate

            args = ("{0} filter {1} {2} {3} {4}".format(self.bf_executable,
                                                        self.corpus.manifest,
                                                        new_chunks_folder,
                                                        filter,
                                                        annotate))
            self.corpus.execute(args, "bf_copy_chunks.log")

        self.corpus.set_chunks_folder("chunks", "chunks/Manifest.txt")
        
        return self
        
    # Run statistics and termtable to create BitFunnel index (config)
    # This only performs work if needed data is missing
    def build_index(self, density=None, treatment=None):
    
        self.config_folder = os.path.join(self.corpus.docs_folder, "config")
        if not os.path.exists(os.path.join(self.config_folder, "DocFreqTable-0.csv")):
            self.run_statistics()

        if not os.path.exists(os.path.join(self.config_folder, "TermTable-0.bin")):
            self.build_termtables(density, treatment)

        return self

    # Capture corpus statistics needed to calculate optimal index
    # This is performed even if it was performed previously
    def run_statistics(self):

        # Create config folder, if missing
        if not os.path.exists(self.config_folder):
            os.makedirs(self.config_folder)

        args = ("{0} statistics {1} {2} -text").format(self.bf_executable,
                                                       self.corpus.manifest,
                                                       self.config_folder)
        self.corpus.execute(args, "bf_run_statistics.log")

        return self

    # Build the termtables based on captured statistics
    # This is performed even if it was performed previously
    def build_termtables(self, density=None, treatment=None):

        if density==None:
            density = 0.15
        if treatment==None:
            treatment = "Optimal"

        # Build termtables (index) for all shards
        args = ("{0} termtable {1} {2} {3}").format(self.bf_executable,
                                                    self.config_folder,
                                                    density,
                                                    treatment)
        self.corpus.execute(args, "bf_build_termtables.log")

        return self

    # Run query log using the specified number/range of threads
    def run_queries(self, querylog, minthreads=1, maxthreads=None):

        self.config_folder = os.path.join(self.corpus.docs_folder, "config")
        max = maxthreads  
        if max == None:
            max = minthreads
        save_test_folder = self.corpus.test_folder

        # Create script file
        self.repl_script = os.path.join(self.corpus.test_folder, "repl.script")
        with open(self.repl_script, "w") as file:
            file.write("threads {0}\n".format(max))
            file.write("load manifest {0}\n".format(self.corpus.manifest));
            file.write("status\n");
            file.write("compiler\n");
            for index, thread in enumerate(range(minthreads, max+1)):
                file.write("threads {0}\n".format(thread))
                if not maxthreads is None:
                    self.corpus.test_folder = save_test_folder + "_" + str(thread)
                    if not os.path.exists(self.corpus.test_folder):
                        print("mkdir " + self.corpus.test_folder)
                        os.makedirs(self.corpus.test_folder)
                file.write("cd {0}\n".format(self.corpus.test_folder))
                file.write("query log {0}\n".format(os.path.join(self.corpus.data_folder, querylog)))
            file.write("quit\n")

        # Start BitFunnel repl
        args = ("{0} repl {1} -script {2} {3}").format(self.bf_executable,
                                                       self.config_folder,
                                                       self.repl_script,
                                                       self.memory)
        self.corpus.execute(args, "bf_run_queries.log")

        self.corpus.test_folder = save_test_folder
        return self

    def analyze_index(self):
        results = IndexCharacteristics("BitFunnel", self.ingestion_thread_count, self.thread_counts)

        with open(self.bf_run_queries_log, 'r') as myfile:
            run_queries_log = myfile.read()
            results.set_float_field("bits_per_posting", "Bits per posting:", run_queries_log)
            results.set_float_field("total_ingestion_time", "Total ingestion time:", run_queries_log)

        for i, threads in enumerate(self.thread_counts):
            query_summary_statistics = os.path.join(self.bf_index_path,
                                                    "results-{0}".format(threads),
                                                    "QuerySummaryStatistics.txt")
            with open(query_summary_statistics, 'r') as myfile:
                data = myfile.read()
                results.append_float_field("planning_overhead", r"Planning overhead:", data)
                results.append_float_field("qps", "QPS:", data)
                results.append_float_field("mps", "MPS:", data)
                results.append_float_field("mpq", "MPQ:", data)
                results.append_float_field("mean_query_latency", "Mean query latency:", data)

        self.compute_false_positive_rate(results);

        return results


    def analyze_corpus(self, gov2_directories):
        results = CorpusCharacteristics(gov2_directories, self.min_term_count, self.max_term_count)

        with open(self.bf_build_statistics_log, 'r') as input:
            build_statistics_log = input.read()
            results.set_int_field("documents", "Document count:", build_statistics_log)
            results.set_int_field("terms", "Raw DocumentFrequencyTable count:", build_statistics_log)
            results.set_int_field("postings", "Posting count:", build_statistics_log)
            results.set_int_field("bytes", "Total bytes read:", build_statistics_log)

        run_queries_log_file = os.path.join(self.bf_index_path, "results-1", "QuerySummaryStatistics.txt")
        with open(run_queries_log_file) as input:
            run_log = input.read()
            results.set_float_field("matches_per_query", "MPQ:", run_log)

        return results
