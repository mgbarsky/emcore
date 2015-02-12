package emcore;
import utils.*;

import java.util.*;
import java.io.*;


public class IterationIntervalProcessor {
		
	int totalInputBlockslastFolder;
	int totalOutputBlocks=0;
	int totalFolders = 0;
	
	int minK;
	int maxK;
	private BufferedReader reader;
	private BufferedWriter writer;
	private BufferedWriter resultwriter;
	private PartialGraph graph = new PartialGraph();
		
	private Map <Integer, Map<Integer,Object>> coreClasses = new HashMap <Integer, Map<Integer,Object>>();
	private Map <Integer,Object> classIntervalKeys;
	
	private long totalReads=0;
	private long totalWrites =0;
	public IterationIntervalProcessor ( int totalFolders, int totalBlockslastFolder, int minK, int maxK)
	{
		this.minK = minK;
		this.maxK = maxK;
		this.totalInputBlockslastFolder = totalBlockslastFolder;
		this.totalFolders = totalFolders;
	}
	
	public long getTotalReads()
	{
		return totalReads;
	}
	public long getTotalWrites()
	{
		return totalWrites;
	}
	public int getCountKCoreClass (int k)
	{
		if (coreClasses.containsKey(k))
			return coreClasses.get(k).keySet().size();
		return 0;
	}
	
	public int getTotalNodesInInterval ()
	{
		return classIntervalKeys.keySet().size();
	}
	
	public boolean process ()
	{
		//first go over all input buckets and collect all the vertices with UBcore >= minK into main memory
		for (int d=0; d<=this.totalFolders ; d++)
		{
			int totalFilesInFolder = EMCore.maxFilesPerFolder;
			if (d == this.totalFolders - 1 ) //last folder or the only folder
				totalFilesInFolder = this.totalInputBlockslastFolder;
			for (int f=0; f< totalFilesInFolder; f++)
			{
				String fileName = Utils.TEMP_FOLDER + System.getProperty("file.separator") + d +System.getProperty("file.separator")+ Utils.BLOCK_FILE_PREFIX +f;
				boolean fileExists = true;
				try
				{
					File file = new File(fileName);
				    reader = new BufferedReader(new FileReader(file));
				}
				catch (Exception ge)
				{
					fileExists = false;
				}
			
				if (fileExists)
				{
					String line=null;
					int lineID=0;
					try
					{				
						boolean done = false;
						while ((line = reader.readLine()) != null &&  !done && !line.equals("")) 
						{
							if (EMCoreIntervals.printAnalysisMessages) totalReads++;
							Vertex v = Vertex.constructFromString (line);
							//System.out.println(line);
							
							int estimatedUBcore = v.getUBCore();
							if (estimatedUBcore >= this.minK)
							{
								this.graph.addVertex(v);
							}
							else  //the vertices are sorted descending (from bigger to smaller)
								done = true;	
							lineID++;
						}
						
						reader.close();
					}
					catch (Exception ge)
					{
						System.out.println("Unexpected error when reading from File "+fileName 
								+" line number "+lineID +": "+ ge.getMessage());
						ge.printStackTrace();
					    return false;
					}
				}
			}
		}		
		if (EMCoreIntervals.printDebugMessages)System.out.println("Total nodes in the interval graph = "+this.graph.getTotalVertices());
		
		coreClasses = this.graph.getAllCoreClasses (minK, maxK);
		classIntervalKeys = new HashMap <Integer,Object>();	
		
		if (coreClasses.keySet().size() >0) //transfer all keys to a new dictionary
		{
			//open final result file to write k-core classes into
			String fname = Utils.RESULT_FOLDER +System.getProperty("file.separator")+"RESULT_CORE_CLASSES";
			try
			{
				File outputfile = new File(fname);
				FileWriter fileWriter = new FileWriter(outputfile, true);
				resultwriter = new BufferedWriter(fileWriter);
				
				//transfer all good nodes into a new dictionary
				for (int k=maxK; k>= minK; k--)
				{
					if (coreClasses.containsKey(k))
					{
						Map <Integer,Object> classKmap = coreClasses.get(k);
						
						classIntervalKeys.putAll(classKmap);
						resultwriter.write("Core class "+k+":");
						resultwriter.newLine();
					
						resultwriter.write(classKmap.keySet().toString());
						resultwriter.newLine();
						resultwriter.write("============================");
						resultwriter.newLine();		
						if (EMCoreIntervals.printAnalysisMessages) totalWrites++;
					}
				}
				resultwriter.flush();
				resultwriter.close();
			}
			catch (Exception ge)
			{
				System.out.println("Unexpected error when opening file '"+fname +"' and writing results for classes from "+this.minK+" to "+this.maxK+": "+ge.getMessage());
			    
				System.out.println ("PLEASE CREATE FOLDER <results> IN CURRENT DIRECTORY TO WRITE FINAL LISTS OF K-CORE CLASSES.");
			    System.exit(1);
			}
		}
		
		//now go over input files again, add deposit tokens, remove resolved vertices also from adjlists, and write the results into output blocks		
		if (classIntervalKeys.keySet().size()>0)
		{
			for (int d=0; d<this.totalFolders ; d++)
			{
				int totalFilesInFolder = EMCore.maxFilesPerFolder;
				if (d == this.totalFolders - 1) //last folder
					totalFilesInFolder = this.totalInputBlockslastFolder;
				for (int f=0; f< totalFilesInFolder; f++)
				{
					String fileName = Utils.TEMP_FOLDER + System.getProperty("file.separator") + d +System.getProperty("file.separator")+ Utils.BLOCK_FILE_PREFIX +f;
				
					boolean fileExists = true;
					try
					{
						File file = new File(fileName);
					    reader = new BufferedReader(new FileReader(file));
					}
					catch (Exception ge)
					{
						//the file could be already fully processed, so no error
						fileExists = false;
					}
				
					List <Vertex> remainingVertices = new ArrayList <Vertex>();
					if (fileExists)
					{
						String line=null;
						int lineID=0;
						
						try
						{				
							while ((line = reader.readLine()) != null ) 
							{
								if (!line.equals(""))
								{
									Vertex v = Vertex.constructFromString (line);
									if (EMCoreIntervals.printAnalysisMessages) totalReads++;
									Integer id = v.getID();
									if (!classIntervalKeys.containsKey(id))
									{																	
										List <Integer>newAdjNodes = new ArrayList <Integer>();
										for (int a=0; a<v.getDegree(); a++)
										{
											Integer aID = v.getAdjvertexID(a);
											if (!classIntervalKeys.containsKey(aID))
											{
												newAdjNodes.add(aID);										
											}
											else
												v.addDepositToken();																	
										}
										v.replaceAdjVertices(newAdjNodes);
										
										//recalculate UBCore
										v.setUBCore(Math.min(v.getDegree()+v.getDepositCount(),v.getUBCore()));								
										
										//add the remaining vertex with lower class to the list, to be processed in the next iteration
										remainingVertices.add(v);											
									}					
									lineID++;
								}
							}
							
							reader.close();						
						}
						catch (Exception ge)
						{
							System.out.println("Unexpected error when reading from File "+fileName 
									+" line number "+lineID +": "+ ge.getMessage());
							ge.printStackTrace();
						    return false;
						}
					}
					
					//open output file and write remaining nodes to a new bucket
					try
					{
						File outputfile = new File(fileName);
						FileWriter fileWriter = new FileWriter(outputfile);
						writer = new BufferedWriter(fileWriter);				
					}
					catch (Exception ge)
					{
						System.out.println("Unexpected error when opening file '"+fileName +"' for writing: "+ge.getMessage());
					    return false;
					}
					if (remainingVertices.size() > 0)
					{	
						//now sort them descending for the next top-down processing
						Collections.sort(remainingVertices, new UBCoreComparator(false)); //sorts in descending order according to a new core upper bound
							
						//write to file
						try
						{
							for (int e=0; e<remainingVertices.size(); e++)
							{
								if (EMCoreIntervals.printAnalysisMessages) totalWrites++;
								writer.write(remainingVertices.get(e).toString());
								writer.newLine();
							}
							writer.flush();
							writer.close();
						}
						catch (Exception ge)
						{
							System.out.println("Unexpected error when writing to file '"+fileName +"': "+ge.getMessage());
						    return false;
						}					
					}
				}
			}
		}
		
		
		return true;		
	}	

}