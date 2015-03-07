# emcore

This is a Java implementation of the "EM core" algorithm, according to the description given in the following paper:
Cheng, J.; Yiping Ke; Shumo Chu; Ozsu, M.T., "Efficient core decomposition in massive networks," Data Engineering (ICDE), 2011 IEEE 27th International Conference on , vol., no., pp.51,62, 11-16 April 2011. 
URL: http://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=5767911&isnumber=5767827

The implementation is aimed for the k-core decomposition of very large graphs, by breaking the input graph into small files and keeping in memory only the nodes of the current k-core class, updating the remaining disk-based partitions.

Before running the program, make two folders in a current directory: <em>temp</em> and <em>results</em>.
Intermediate partitioned input is written into the <em>temp</em> directory, and the final k-core classes are written into the <em>results</em> directory.

Compile all classes into the <em>bin</em> directory

<code>javac -d bin -classpath bin src/utils/*.java
javac -d bin -classpath bin src/emcore/*.java
javac -d bin -classpath bin src/baseline/*.java</code>

If your input graph is directed, you need to convert it into an undirected graph by complementing each edge into both directions. For this run the <em>utils.CompleteEdges</em> class.

<code>java -Xmx512M -classpath bin utils.CompleteEdges inputGraph.txt</code>

This will create an undirected graph and write it to file: <em>inputgraph.txt_COMPLEMENTED</em>.

After this, remove duplicate edges: 
<code>
sort -k 1n,2n inputgraph.txt_COMPLEMENTED > test1.txt
uniq test1.txt test2.txt
</code>

File <em>test2.txt</em> represents the undirected version of the original graph.

To run this EMCore implementation:

<code>java -Xmx4G -classpath bin emcore.EMCoreIntervals test2.txt emcore.properties</code>

The <em>emcore.properties</em> file contains the default parameters of the algorithm, which were adjusted for the better performance, but can be changed. 
The sample input graph is provided in file <em>test2.txt</em>. This file contains an undirected graph, where each line represents an edge as a tab-delimited pair of node IDs.


