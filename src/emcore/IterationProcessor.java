package emcore;
import utils.*;

import java.util.*;
import java.io.*;


public class IterationProcessor {
		
	int totalInputBlockslastFolder;
	int totalOutputBlocks=0;
	int totalFolders = 0;
	
	int k;
	private BufferedReader reader;
	private BufferedWriter writer;
	private PartialGraph graph = new PartialGraph();
	private Map <Integer,Object> classKnodes ;
	
	public IterationProcessor ( int totalFolders, int totalBlockslastFolder, int k)
	{
		this.k = k;
		
		this.totalInputBlockslastFolder = totalBlockslastFolder;
		this.totalFolders = totalFolders;
	}
	
	public int getCountKCoreClass ()
	{
		return classKnodes.keySet().size();
	}
	
	public boolean process ()
	{
		//first go over all input buckets and collect all the vertices with UBcore >= k into main memory
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
							Vertex v = Vertex.constructFromString (line);
							//System.out.println(line);
							
							int estimatedUBcore = v.getUBCore();
							if (estimatedUBcore >= this.k)
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
		
		classKnodes = this.graph.getCoreClassDictionary(this.k);		
		
		//now go over input files again, add deposit tokens, remove resolved vertices also from adjlists, and write the results into output blocks		
		if (classKnodes.keySet().size()>0)
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
									
									Integer id = v.getID();
									if (!classKnodes.containsKey(id))
									{																	
										List <Integer>newAdjNodes = new ArrayList <Integer>();
										for (int a=0; a<v.getDegree(); a++)
										{
											Integer aID = v.getAdjvertexID(a);
											if (!classKnodes.containsKey(aID))
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
		
		//finally, record vertices with class k to a final output file
		if (classKnodes.keySet().size()>0)
		{
			String fname = Utils.RESULT_FOLDER +System.getProperty("file.separator")+"RESULT_CORE_CLASSES";
			try
			{
				File outputfile = new File(fname);
				FileWriter fileWriter = new FileWriter(outputfile, true);
				writer = new BufferedWriter(fileWriter);
				writer.write("Core class "+this.k+":");
				writer.newLine();
				
				writer.write(classKnodes.keySet().toString());
				writer.newLine();
				writer.write("============================");
				writer.newLine();
				writer.flush();
				writer.close();
			}
			catch (Exception ge)
			{
				System.out.println("Unexpected error when opening file '"+fname +"' for writing result for class "+this.k+": "+ge.getMessage());
			    
				System.out.println ("PLEASE CREATE FOLDER <results> IN CURRENT DIRECTORY TO WRITE FINAL LISTS OF K-CORE CLASSES.");
			    System.exit(1);
			}			
		}
		return true;		
	}	

}