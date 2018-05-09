import os

class Pef:
    def __init__(self,
                 corpus,                          # Corpus object handling docs & queries
                 pef_path):                       # Full path to PEF program

        self.corpus = corpus                 
        corpus.add_engine('pef', self)

        self.pef_creator = os.path.join(pef_path, "create_freq_index")
        self.pef_runner = os.path.join(pef_path, "Runner")
 
        # Establish full path names for files and folders
        self.mg4jindex_folder = os.path.join(self.corpus.docs_folder, "mg4jindex")
        self.pefindex_folder = os.path.join(self.corpus.docs_folder, "pefindex")
        self.pef_index_type = "opt"
        self.pef_index_file = os.path.join(self.pefindex_folder, "index." + self.pef_index_type)
        
    # Build PEF index from MG4J index
    # This only performs work if needed data is missing
    def build_index(self):
    
        if os.path.exists(self.pefindex_folder):
            return self
        os.makedirs(self.pefindex_folder)

        # Export info needed to build pef index from mg4j index
        args = ("org.bitfunnel.reproducibility.IndexExporter "
                "{0} {1} --index").format(os.path.join(self.mg4jindex_folder, "index"), 
                                          os.path.join(self.pefindex_folder, "index"))
        self.corpus.mg4j_execute(args, "pef_build_collection.log")

        # Create PEF index
        args = ("{0} {1} {2} {3}").format(self.pef_creator,
                                          self.pef_index_type,
                                          os.path.join(self.pefindex_folder, "index"),
                                          self.pef_index_file)
        self.corpus.execute(args, "pef_build_index.log")

        return self

    # Run query log using the specified number/range of threads
    def run_queries(self, querylog, threads=1):

        self.pef_results_file = os.path.join(self.corpus.test_folder, "pefresults.csv")
        args = ("{0} {1} {2} {3} {4} {5}").format(self.pef_runner,
                                                  self.pef_index_type,
                                                  self.pef_index_file,
                                                  os.path.join(self.corpus.data_folder, querylog),
                                                  threads,
                                                  self.pef_results_file)
        self.corpus.execute(args, "pef_run_queries.log")

        return self