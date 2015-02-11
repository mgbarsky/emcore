package baseline;

import java.io.*;
import java.util.*;

import utils.*;

public class TopDownAlgorithm {
	static boolean debug = false;
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
		int k=maxDegree;
		
		//now start going over files and collecting first nodes - with degree 1, and then all the rest - bottom up
		while (!done && k>1)
		{
			done = iterate (totalOutputFiles, k--);			
		}		
	}
	
	private static boolean iterate (int totalFiles,int k)
	{
		boolean allProcessed = true;
		String inputFilePrefix = Utils.TEMP_FOLDER + System.getProperty("file.separator") + Utils.BLOCK_FILE_PREFIX;
		
		BufferedReader reader = null;
		BufferedWriter writer=null;
				
		Map <Integer, Object> candidateKeys = new HashMap <Integer, Object>();  //stores nodes identified as candidates for k-core
		List <Vertex> candidates = new ArrayList <Vertex> (); //put everything here with degree + deposit >= k
		//1. Collect all nodes with degree+deposit >= k first
		if (k==4)
			k=4;
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
			
			if (fileExists)  //read all top k-degree nodes from it
			{
				String line=null;	
				int lineID = 0;
				try
				{				
					boolean done = false;
					while ((line = reader.readLine()) != null &&  !done && !line.equals("")) 
					{
						Vertex v = Vertex.constructFromString (line);
						if (v.getDegree()+v.getDepositCount() >= k)
						{
							candidates.add(v);
							candidateKeys.put(v.getID(), null);
						}
						else  //the vertices are sorted descending, so the next does not qualify to be in k-core class
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
		
		
		//now we need to refine candidates for the core class k, working with the sub-graph built in main memory
		boolean doneRefining = false;
		while (!doneRefining)
		{
			doneRefining = true; //assuming that we will be done in this iteration, we are done when no more changes to candidates
			Map <Integer,Object> idsToRemove = new HashMap <Integer,Object>();
			
			for (int i=0; i<candidates.size(); i++)
			{
				Vertex v = candidates.get(i);
				
				int totalConnections = v.getDepositCount();
				
				for (int a=0; a<v.getDegree(); a++)
				{
					int childID = v.getAdjvertexID(a);
					if (candidateKeys.containsKey(childID))
						totalConnections++;
				}
				
				if (totalConnections < k)
				{					
					idsToRemove.put(v.getID(),null);
				}
			}
			
			if (idsToRemove.size()>0)
			{
				doneRefining = false;
				List <Vertex> newCandidates = new ArrayList <Vertex> ();
				for (int i=0; i< candidates.size(); i++)
				{
					Vertex v = candidates.get(i);
					if (idsToRemove.containsKey(v.getID()))
					{
						candidateKeys.remove(v.getID());
					}
					else //keep this vertex in candidates
					{
						newCandidates.add(v);
					}
				}
				candidates = newCandidates;
			}	
		}
		
		//done refining, this is the final group of core-k class
		System.out.println("Total core-"+k +" nodes:"+candidateKeys.keySet().size());
		
		//now we need to iterate over input files again, and - remove all nodes with degree k, 
		//and remove all adjacent vertices  that correspond to these nodes, depositing one token per removed node - top down - to know
		//that it is connected to a higher degree node
		//the remaining changed nodes are written into new files
		
		allProcessed = true; //assume that no more nodes remain to process - all have been assigned to some core class			
			
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
						if (!candidateKeys.containsKey(v.getID())) //this node does not belong to the current k-core, and is written back to file
						{
							List <Integer>newAdjNodes = new ArrayList <Integer>(); //check if need to remove some of adjacent nodes
							for (int a=0; a<v.getDegree(); a++)
							{
								Integer aID = v.getAdjvertexID(a);
								if (!candidateKeys.containsKey(aID))
								{
									newAdjNodes.add(aID);										
								}
								else
								{
									v.addDepositToken();
								}
							}
							v.replaceAdjVertices( newAdjNodes);
							remainingNodes.add(v);
						}
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
					//now sort them descending for the next top-down processing
					Collections.sort(remainingNodes, new DegreeComparator(false)); //sorts in descending order according to a new core upper bound
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
		
		//finally write current core class to the results folder
		if (candidateKeys.size()>0)
		{
			String fname = Utils.RESULT_FOLDER +System.getProperty("file.separator")+"core_class_"+k;
			try
			{
				File outputfile = new File(fname);
				FileWriter fileWriter = new FileWriter(outputfile);
				writer = new BufferedWriter(fileWriter);
				
				List <Integer> list = new LinkedList <Integer>(candidateKeys.keySet());
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
		Collections.sort(buffer, new DegreeComparator(false)); //sorts in DESCENDING  order of degrees -top-down computation
		if (debug)
			System.out.println(buffer);
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

/* Test results for amazon0302.txt -> test2.txt (complemented, sorted, duplicates removed)
 * Total core-6 nodes:286
Total core-5 nodes:158372
Total core-4 nodes:78421
Total core-3 nodes:11078
Total core-2 nodes:7451

Added vertex 0
Total output files = 5; total nodes in the graph=14; max node degree is 6
Total core-6 nodes:0
Total core-5 nodes:0
Total core-4 nodes:0
Total core-3 nodes:5
Total core-2 nodes:5
 */