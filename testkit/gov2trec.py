#!/usr/bin/env python3

from corpus import Corpus
from bitfunnel import BitFunnel
from mg4j import Mg4j
from pef import Pef

corpus = Corpus( r"/bf/data/gov2",
                 r"/bf/git/mg4j-workbench")
corpus.set_test_name("TRECquery")

bf = ( BitFunnel( corpus,
                  r"/bf/cmake/BitFunnel/tools/BitFunnel/src/BitFunnel",
                  15000000)
       .build_index()
)

mg4j = Mg4j( corpus ).build_index()

pef = Pef(corpus, 
          r"/bf/cmake/pef/bin").build_index()

corpus.run_queries(r"06.efficiency_topics.all", 1)
