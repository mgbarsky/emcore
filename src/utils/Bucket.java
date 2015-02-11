package utils;


import java.util.*;
import java.io.*;


public class Bucket {
	Map <Integer, Integer> nodeKeys;
	
	List <Vertex> nodes;
	int totalNodesCounter;
	int bucketID;
	int bucketPlaceInArray;
	
	private int tightenedUBCoreCount;
	//private int maxUBcore = 0;
	
	
	public Bucket (int placeInArray, int bucketID)
	{
		this.bucketID = bucketID;
		this.bucketPlaceInArray = placeInArray; //this does not change
		nodeKeys = new HashMap <Integer,Integer>();		
		nodes = new ArrayList <Vertex>();
	}
		
	public int getBucketPlaceInArray()
	{
		return this.bucketPlaceInArray;
	}
	public int getTightenedCount ()
	{
		return this.tightenedUBCoreCount;
	}
	
	public void setNewBucketID (int bucketID)
	{
		this.bucketID =bucketID;
	}
	
	public void addVertex (Vertex v)
	{
		nodes.add(v);
		int totalInThisV = 1 + v.getDegree();
		Integer position = new Integer (this.nodes.size()-1);
		
		this.nodeKeys.put(v.getID(), position);
		this.totalNodesCounter += totalInThisV;				
	}
	
	//public int getMaxUBCore ()
	//{
	//	return this.maxUBcore;
	//}
	
	public int getTotalNodesCount() //gets total number of nodes stored in the bucket, including adjacency lists
	{
		return this.totalNodesCounter;
	}
	
	public int getTotalVertices ()
	{
		return this.nodes.size();
	}
	
	public Vertex getVertexByID (int ID)
	{
		Integer pos = this.nodeKeys.get(ID);
		
		if (pos == null)
			return null;
		
		return this.nodes.get(pos);
	}
	
	public Vertex getVertexByPosition (int pos)
	{
		if (pos >= this.nodes.size())
			return null;
		
		return this.nodes.get(pos);
	}
	
	public boolean contains (int vertexID)
	{
		return this.nodeKeys.containsKey(vertexID);
	}
	
	public void tightenUBCore (boolean timeBounded, long totalMaxTimeMS)
	{
		//define the dictionary to hold mapping from node id to position in the 
		//list of nodes for all nodes with lesser degree that are processed
		//in order to find them quickly
		Map <Integer, Integer> lesserK_Processed = new HashMap  <Integer, Integer>();	
		//1. sort all nodes in bucket in ascending order of degree
		Collections.sort(this.nodes, new DegreeComparator());
		
		//Compute upper bound of K for each node in the bucket, and record its position
		//to the dictionary
		long start = System.currentTimeMillis();
		boolean done = false;
		
		for (int i=0; i< this.nodes.size() && !done; i++)
		{	
			Vertex current = this.nodes.get (i);
			int originalDegree = current.getDegree();
			int bestMin = originalDegree; //the best upper bound for k we know so far
			this.nodes.get (i).setUBCore(bestMin);
			
			List <Vertex> candidates = new ArrayList  <Vertex> ();
			
			for (int k=0; k< current.getDegree(); k++)
			{
				int childID = current.getAdjvertexID(k);
				if (lesserK_Processed.containsKey(childID))
				{
					int posInNodesArray = lesserK_Processed.get(childID);
					Vertex candidate = this.nodes.get(posInNodesArray);
					if (candidate.getUBCore() < bestMin)
						candidates.add (candidate);
				}
			}
				
			//now go through all the candidates and try to tighten original upper bound which was set to degree
								
			List <Vertex> z_set = new ArrayList <Vertex> ();
			int z_setMaxUpBound = 0;
			for (int k=0; k < candidates.size(); 	k++)
			{
				z_set.add(candidates.get(k));
				if (candidates.get(k).getUBCore() > z_setMaxUpBound)
					z_setMaxUpBound = candidates.get(k).getUBCore();
				
				int z_value = Math.max (originalDegree - z_set.size(), z_setMaxUpBound);
				
				if (z_value < bestMin)
					bestMin = z_value;
			}
			
			if (bestMin < originalDegree)
				this.tightenedUBCoreCount++;
			this.nodes.get (i).setUBCore(bestMin);
			
			lesserK_Processed.put(current.getID(),i);
			if (timeBounded && ((System.currentTimeMillis() - start) >= totalMaxTimeMS))
				done =true;
		}
		
		//take one from the top
		//this.maxUBcore = this.nodes.get(0).getUBCore();
	}
	
	
	
	public boolean flushReset (int folderID, int fileID)
	{	
		if (fileID == 0) //create a new folder
		{
			new File(Utils.TEMP_FOLDER +System.getProperty("file.separator")+folderID).mkdir();
		}
		String fileName = Utils.TEMP_FOLDER +System.getProperty("file.separator") +folderID+System.getProperty("file.separator")+ Utils.BLOCK_FILE_PREFIX + fileID;
		Collections.sort(this.nodes, new UBCoreComparator(false)); //sorts in descending order according to a tighter core bound
		//System.out.println(this);
		BufferedWriter writer = null;
		try 
		{		   
		    writer = new BufferedWriter(new FileWriter(fileName));		
		
			for (int i=0; i<this.nodes.size(); i++)
			{
				writer.write(nodes.get(i).toString());
				writer.newLine();
			}
		
			writer.flush();
			writer.close();
		} 
		catch (FileNotFoundException e) 
		{			
			System.out.println("Cannot write to File "+fileName +": "+e.getMessage());
			System.out.println ("PLEASE CREATE FOLDER <temp> IN CURRENT DIRECTORY TO WRITE INTERMEDIATE RESULTS.");
		    System.exit(1);
		} 
		catch (Exception ge) 
		{
			System.out.println("Unexpected error when writing to File "+fileName +": "+ge.getMessage());
		    return false;
		} 
		nodeKeys = new HashMap <Integer,Integer>();		
		nodes = new ArrayList <Vertex>();
		this.totalNodesCounter = 0;
		
		this.tightenedUBCoreCount=0;
		return true;
	}
	
	public String toString ()
	{
		return this.nodes.toString();
	}
}
