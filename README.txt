hadoop jar HadoopPageRank.jar weimar.hadoop.pagerank.HadoopPageRank -conf config.xml 5

Usge: hadoop jar HadoopPageRank.jar [number of iterations]

input Microsoft academic graph data is put on hdfs.
String data_paperRef = "/corpora/corpus-microsoft-academic-graph/data/PaperReferences.tsv.bz2";
String data_paperAuthor = "/corpora/corpus-microsoft-academic-graph/data/PaperAuthorAffiliations.tsv.bz2";
String data_Author = "/corpora/corpus-microsoft-academic-graph/data/Authors.tsv.bz2";

the final output file path is
/user/gofo3028/final2

and intermediate file is all kept (if iterations is 5)
/user/gofo3028/0 (filtered author data)
/user/gofo3028/1 (author number data)
/user/gofo3028/2 (filtered paper data)
/user/gofo3028/3 (reference data which linked paperid to authorid)
/user/gofo3028/4 (reference data which linked reference paperid to authorid)
/user/gofo3028/5 (graph data)
/user/gofo3028/6 (pagerank process data)
/user/gofo3028/7 (pagerank process data)
/user/gofo3028/8 (pagerank process data)
/user/gofo3028/9 (pagerank process data)
/user/gofo3028/10 (pagerank process data)
/user/gofo3028/30 (cited number data)
/user/gofo3028/31 (paper number data)
/user/gofo3028/pagerank
/user/gofo3028/top
/user/gofo3028/final