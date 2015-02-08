# emcore

This is a Java implementation of different approaches to extracting k-core subgraphs from large graphs.

A k-core class contains all the nodes of the graph which are connected to at least k nodes, such that each of these nodes in turn belongs to the same k-core class.

Two basic approaches are investigated:
a bottom-up approach (where decomposition begins from k=1 and iteratively removes all nodes of the lower class from the graph), and top-down approach (where decomposition begins from the largest class, trying to compute the nodes with top k edges first, iteratively removing top nodes from the graph).

The implementation is aimed for the k-core decomposition of the very large graphs, and thus it tries to break the input graph into small files and keep in memory only the nodes of the current k-class, updating them by sequentially uploading necessary information from the disk-based files.

In order to run each program, you need to supply two folders in a current directory: temp and results.
Intermediate partitioned input is written into temp directory, and the final k-core classes are written into results directory.

If your input graph is directed, you need to convert it into an undirected graph by complementing each edge into both directions. For this run:

<code>java -Xmx512M -jar converttoundirected.jar inputgraph.txt</code>

This will create an undirected graph and write it to file: inputgraph.txt_COMPLEMENTED.

And after this, remove duplicate edges: 
<code>
sort -k 1n,2n inputgraph.txt_COMPLEMENTED > test1.txt
uniq test1.txt test2.txt
</code>

File test2.txt represents the undirected graph.

Two basic implementations serve as a reference for the correctness and as a performance baseline for investigating efficiency of the algoritms for the k-core decomposition of large graphs. To run these algorithms:

<code>java -Xmx1024M -jar bottomupbasic.jar test2.txt</code>

<code>java -Xmx1024M -jar topdownbasic.jar test2.txt</code>

The algorithm "EM core" is implemented next, according to the description given in the following paper:
Cheng, J.; Yiping Ke; Shumo Chu; Ozsu, M.T., "Efficient core decomposition in massive networks," Data Engineering (ICDE), 2011 IEEE 27th International Conference on , vol., no., pp.51,62, 11-16 April 2011. 
URL: http://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=5767911&isnumber=5767827

To run this EMCore implementation:

<code>java -Xmx1024M -jar emcore.jar test2.txt</code>

The sample input graph is provided in file test2.txt. This file contains an undirected graph, where each edge is encoded in a line with a pair of node IDs delimited by a tab.


