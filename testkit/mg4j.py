import os
import platform

class Mg4j:
    def __init__(self,
                 corpus):                          # Corpus object handling docs & queries

        self.corpus = corpus                 
        corpus.add_engine('mg4j', self)

        # Establish full path names for files and folders
        self.index_folder = os.path.join(self.corpus.docs_folder, "mg4jindex")
        self.mg4j_basename = os.path.join(self.index_folder, "index")

    # Build MG4J index from manifest.txt listed files
    # This only performs work if index is missing
    def build_index(self):

        if os.path.exists(self.index_folder):
            return self

        # Create index folder
        os.makedirs(self.index_folder)

        args = ("it.unimi.di.big.mg4j.tool.IndexBuilder "
                "-o \"org.bitfunnel.reproducibility.ChunkManifestDocumentSequence({0})\" "
                "{1}").format(self.corpus.manifest, self.mg4j_basename)
        self.corpus.mg4j_execute(args, "mg4j_build_index.log")
        
        return self

    # Run query log using the specified number/range of threads
    def run_queries(self, querylog, threads=1):

        self.mg4j_results_file = os.path.join(self.corpus.test_folder, "mgj4results.csv")
        args = ("org.bitfunnel.reproducibility.QueryLogRunner "
                "mg4j {0} {1} {2} {3}").format(self.mg4j_basename,
                                               os.path.join(self.corpus.data_folder, querylog),
                                               self.mg4j_results_file,
                                               threads)
        self.corpus.mg4j_execute(args, "mg4j_run_queries.log")

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
