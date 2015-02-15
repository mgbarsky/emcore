package baseline;
import java.io.*;

import java.util.*;

import utils.*;

/**
 * This is the basic algorithm which starts from the lowest k
 * removes all vertices with degree k
 * subtracts 1 from all adjVertices of these removed vertices
 * iterates to collect additional vertices with degree k
 * until no more can be found.
 * This concludes one iteration of this basic algorithm.
 * 
 * To accomodate processing of large files,
 * the entire file is not loaded into ram, but it is broken into smaller files, each of them is scanned and processed at once.
 * 
 * The scaling bottleneck is when the original subgraph for current value of k exceeds the available main memory.
 * 
 * Program arguments:
 * 1 - name of the input graph file
 * 2 (optional) - max number of elements in each small file - that depends on the available main memory. Default: 100,000 elements. 
 * Each element represents a vertex, including parent nodes and the node ids in the adjacency lists.
 * 3 (optional) - input delimiter. Default: tab
 **/

public class BottomUpAlgorithm {

	public static void main (String [] args)
	{
		int MAX_ELEMENTS = 100000;
		if (args.length < 1)
		{
			System.out.println ("provide the name of the input graph file and  "
					+ "optionally the max number of elements in memory and a delimiter for each line in this file ");
			System.exit(0);
		}
		
		BufferedReader reader = null;
		String inputFileName = args[0];
		
		if (args.length >1)
			MAX_ELEMENTS = Integer.parseInt(args[1]);
		
		String delimiter = String.valueOf('\t');
		if (args.length >2)
			delimiter = args[2];
		
		//first, rewrite the entire input as a set of vertices with adjacency lists
		//and break the input into small files with at most MAX_ELEMENTS total vertices IDs
		try
		{
			File file = new File(inputFileName);
		    reader = new BufferedReader(new FileReader(file));
		}
		catch (Exception ge)
		{
			System.out.println("Unexpected error when reading from File "+inputFileName +": "+ge.getMessage());
			System.exit(1);
		}
		
		String line;
		Vertex currentVertex =null;
		int currentvertexID = -1;
		
		List <Vertex> buffer = new ArrayList <Vertex>();  //buffer stores all graph nodes with adjacency lists, to be written to disk
				
		int elementsCount =0;
		int outputFileID = 0;
		int totalOutputFiles=0;
		int totalNodes = 0;
		int lineID =0;
		int maxDegree =0;
		
		try
		{
			while ((line = reader.readLine()) != null) 
			{
				String [] pair = line.split(delimiter);
				int parentVertexID =0;
				int childVertexID=0 ;
				boolean validLine = true;
				
				try
				{
					parentVertexID = Integer.parseInt(pair[0]);
					childVertexID = Integer.parseInt(pair[1]);
				}
				catch (Exception e)
				{
					validLine = false;
				}
				
				if (validLine)
				{
					if (parentVertexID != currentvertexID) //change of vertices
					{
						if (currentVertex != null) //prev vertex is done, try to add it to the best bucket
						{
							buffer.add(currentVertex);
							totalNodes++;
							if (currentVertex.getDegree() > maxDegree)
								maxDegree = currentVertex.getDegree();
							elementsCount += (1+currentVertex.getDegree());
							
							if (elementsCount >MAX_ELEMENTS)
							{
								flushResetBuffer (buffer, outputFileID++);
								elementsCount=0;
								totalOutputFiles++;
								buffer = new ArrayList<Vertex>();
							}													
						}
						currentVertex = new Vertex(parentVertexID);
						currentvertexID = parentVertexID;
					}
					currentVertex.addAdjVertex(childVertexID);
					
					if (totalNodes % 10000 == 0)
						System.out.println("Added vertex "+totalNodes);
					lineID++;
					if (lineID % 100000 == 0)
						System.out.println("Processed line "+lineID);
				}
			}
			buffer.add(currentVertex);
			totalNodes++;
			flushResetBuffer (buffer, outputFileID++);				
			totalOutputFiles++;			
		}
		catch (Exception ge)
		{
			System.out.println("Unexpected error when processing lines of input file: "+ge.getMessage());
			System.exit(1);
		}	
		
		System.out.println("Total output files = "+totalOutputFiles +"; total nodes in the graph="+totalNodes+"; max node degree is "+maxDegree);
		boolean done = false;
		int k=1;
		
		//now start going over files and collecting first nodes - with degree 1, and then all the rest - bottom up
		while (!done)
		{
			done = iterate (totalOutputFiles, k++);			
		}		
	}
	
	private static boolean iterate (int totalFiles,int k)
	{
		boolean allProcessed = true;
		String inputFilePrefix = Utils.TEMP_FOLDER + System.getProperty("file.separator") + Utils.BLOCK_FILE_PREFIX;
		
		BufferedReader reader = null;
		BufferedWriter writer=null;
				
		Map <Integer, Object> degreeKNodes = new HashMap <Integer, Object>();  //stores nodes identified as k-core so far
		
		//1. Collect all nodes with degree k first
		
		for (int i=0;i<totalFiles; i++)
		{
			boolean fileExists = true;
			try
			{
				File file = new File(inputFilePrefix+i);
			    reader = new BufferedReader(new FileReader(file));
			}
			catch (Exception ge)
			{
				//file does not exist, do nothing, continue
				fileExists =false;
			}
			
			if (fileExists)  //read all k-degree nodes from it
			{
				String line=null;	
				int lineID = 0;
				try
				{				
					boolean done = false;
					while ((line = reader.readLine()) != null &&  !done && !line.equals("")) 
					{
						String [] fields = line.split(Utils.FIELD_SEPARATOR);
						int degree =0;
						
						if (fields.length == 3 && !fields [2].equals(""))
						{
							String [] adjNodes = fields [2].split(Utils.VALUE_SEPARATOR);						
							degree = adjNodes.length;
						}
						if (degree == k)
						{
							Vertex v = Vertex.constructFromString (line);
							degreeKNodes.put(v.getID(), null);
						}
						else  //the vertices are sorted ascending, so the next is bigger than the current k
							done = true;	
						lineID++;
					}
					
					reader.close();
				}
				catch (Exception ge)
				{
					System.out.println("Unexpected error when reading from File "+(inputFilePrefix+i) 
							+" line number "+lineID +": "+ ge.getMessage());
					ge.printStackTrace();
				    System.exit(1);
				}
			}
		}
		
		
		//now we need to iterate over input files again, and - remove all nodes with degree k, and remove all adjacent vertices  that correspond to these nodes
		//the remaining nodes are written into new files
		boolean allRemoved = false;
		
		while (!allRemoved)
		{
			allRemoved = true;
			allProcessed = true;			
			
			for (int i=0;i<totalFiles; i++)
			{
				List <Vertex> remainingNodes = new ArrayList <Vertex>();
				boolean fileExists = true;
				try
				{
					File file = new File(inputFilePrefix+i);
				    reader = new BufferedReader(new FileReader(file));
				}
				catch (Exception ge)
				{
					//file does not exist, do nothing, continue
					fileExists =false;
				}
			
				if (fileExists)  //read all k-degree nodes from it
				{
					String line=null;	
					int lineID = 0;
					try
					{				
						while ((line = reader.readLine()) != null &&   !line.equals("")) 
						{
							Vertex v = Vertex.constructFromString (line);
							if (!degreeKNodes.containsKey(v.getID()))
							{
								List <Integer>newAdjNodes = new ArrayList <Integer>();
								for (int a=0; a<v.getDegree(); a++)
								{
									Integer aID = v.getAdjvertexID(a);
									if (!degreeKNodes.containsKey(aID))
									{
										newAdjNodes.add(aID);										
									}
									else
										allRemoved = false;
								}
								v.replaceAdjVertices( newAdjNodes);
								if (v.getDegree() <= k)
									degreeKNodes.put(v.getID(), null);
								else
									remainingNodes.add(v);
							}
							else
								allRemoved = false;
							lineID++;
						}						
										
						reader.close();
					}
					catch (Exception ge)
					{
						System.out.println("Unexpected error when reading from File "+(inputFilePrefix+i) 
								+" line number "+lineID +": "+ ge.getMessage());
						ge.printStackTrace();
					    System.exit(1);
					}
					
					//write remaining nodes back to the file
					try
					{
						File outputfile = new File(inputFilePrefix+i);
						FileWriter fileWriter = new FileWriter(outputfile);
						writer = new BufferedWriter(fileWriter);				
					}
					catch (Exception ge)
					{
						System.out.println("Unexpected error when opening file '"+(inputFilePrefix+i) +"' for writing: "+ge.getMessage());
					    System.exit(1);
					}
					
					if (remainingNodes.size()>0)
					{
						allProcessed = false;
						//write them back to the same file
						//now sort them ascending for the next bottom-up processing
						Collections.sort(remainingNodes, new DegreeComparator(true)); //sorts in ascending order according to a new degree, after removing vertices
						//System.out.println("--------------------");
						//System.out.println (remainingNodes);
						//System.out.println();
						
						//write to file
						try
						{
							for (int e=0; e<remainingNodes.size(); e++)
							{
								writer.write(remainingNodes.get(e).toString());
								writer.newLine();
							}
							writer.flush();
							writer.close();
						}
						catch (Exception ge)
						{
							System.out.println("Unexpected error when writing to file '"+(inputFilePrefix+i) +"': "+ge.getMessage());
							System.exit(1);
						}
					}
				}
			}
		}
		
		//finally write current core class to the results folder
		if (degreeKNodes.size()>0)
		{
			String fname = Utils.RESULT_FOLDER +System.getProperty("file.separator")+"core_class_"+k;
			try
			{
				File outputfile = new File(fname);
				FileWriter fileWriter = new FileWriter(outputfile);
				writer = new BufferedWriter(fileWriter);
				System.out.println("Total core-"+k +" nodes:"+degreeKNodes.keySet().size());
				List <Integer>list = new LinkedList <Integer>(degreeKNodes.keySet());
				Collections.sort(list);
				writer.write(list.toString());
				writer.flush();
				writer.close();
			}
			catch (Exception ge)
			{
				System.out.println("Unexpected error when writing to file '"+fname +"' core class "+k+": "+ge.getMessage());
			    System.exit(1);
			}			
		}
		return allProcessed;
	}
		
	private static void flushResetBuffer (List <Vertex>buffer, int currentFileID)
	{		
		String fileName = Utils.TEMP_FOLDER +System.getProperty("file.separator") + Utils.BLOCK_FILE_PREFIX + currentFileID;
		Collections.sort(buffer, new DegreeComparator(true)); //sorts in ascending order of degrees - bottom-up computation
		//System.out.println(buffer);
		BufferedWriter writer = null;
		try 
		{		   
		    writer = new BufferedWriter(new FileWriter(fileName));		
		
			for (int i=0; i<buffer.size(); i++)
			{
				writer.write(buffer.get(i).toString());
				writer.newLine();
			}
		
			writer.flush();
			writer.close();
		} 
		catch (FileNotFoundException e) 
		{
			System.out.println("Cannot write to File "+fileName +": "+e.getMessage());
		    e.printStackTrace();
		    System.exit(1);
		} 
		catch (Exception ge) 
		{
			System.out.println("Unexpected error when writing to File "+fileName +": "+ge.getMessage());
		    System.exit(1);
		} 
	}
}

/*
 * Test printout for amazon0302.txt -> test2.txt (complemented, sorted, duplicates removed)
Total output files = 21; total nodes in the graph=262111; max node degree is 420
Total core-1 nodes:6503
Total core-2 nodes:7451
Total core-3 nodes:11078
Total core-4 nodes:78421
Total core-5 nodes:158372
Total core-6 nodes:286

for input small.txt
Added vertex 0
Total output files = 5; total nodes in the graph=14; max node degree is 6
Total core-1 nodes:4
Total core-2 nodes:5
Total core-3 nodes:5
 */
