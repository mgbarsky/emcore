/**
 * This piece partitions input into manageable chunks of about 100,000 nodes 
 * (defined as maxTotalNodes parameter when tighenBounds is set to false)
 * 
 * @author marina
 *
 */

package emcore;
import java.util.*;
import java.io.*;

import utils.*;

public class SimplePartitioner implements PartitionerInterface {
	
	private long totalLineWrites = 0;
	private int maxDegree =0;
	private BufferedReader reader;
	private String delimiter;
	
	private int maxTotalNodes;
	private int maxBucketSize;
	
	private int minBucketSize;
	private int totalNodes;
	
	private Bucket bucket;
	
	private int currentBucketFileID;
	
	private int currentFolderID = 0;	
		
		
	//this stores how many nodes are estimated for each core class
	private Map <Integer,Integer> coreClassCounts = new HashMap <Integer,Integer> () ;
	
	private Map <Integer,Integer> degreeCounts ; 	
	
	public boolean init (String inputFileName, char delimiterChar, 
			int maxTotalNodes, int maxBucketSize, int minBucketSize)
	{
		this.delimiter = String.valueOf(delimiterChar);
		this.maxTotalNodes = maxTotalNodes;
		
		try
		{
			File file = new File(inputFileName);
		    reader = new BufferedReader(new FileReader(file));
		}
		catch (Exception ge)
		{
			System.out.println("Unexpected error when reading from File "+inputFileName +": "+ge.getMessage());
		    return false;
		}
		
		bucket = new Bucket (0, 0);
		
		if (EMCoreIntervals.printAnalysisMessages)
			degreeCounts = new HashMap <Integer,Integer> () ;
		return true;
	}
	
	public Map <Integer,Integer> getCoreClassCounts() 
	{
		return coreClassCounts;
	}
	
	public Map <Integer,Integer> getDegreeCounts() 
	{
		return degreeCounts;
	}
	
	public long getTotalReads()
	{
		return totalLineWrites;
	}
	
	public long getTotalWrites()
	{
		return totalLineWrites;
	}
	
	public int getMaxDegree ()
	{
		return this.maxDegree;
	}
	
	public int getTotalFilesLastFolder ()
	{
		return currentBucketFileID;
	}
	
	public int getTotalFolders ()
	{
		return this.currentFolderID +1;
	}
	
	public int getMaxUBCore ()
	{
		return this.maxDegree;
	}
	
	//this will just read an input and collect information about the degree of vertices, and add them to a bucket
	//when there are maxTotalNodes in the bucket, flushes it to disk
	public boolean perform (boolean tightenUB, boolean limitUBTightening, long msTimeBoundPerBucket)
	{		
		String line=null;
		Vertex currentVertex = null;
		int currentvertexID = -1;
		int lineID = 0;
		int totalAdded = 0;
		try
		{
			while ((line = reader.readLine()) != null) 
			{
				String [] pair = line.split(this.delimiter);
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
							currentVertex.setUBCore(currentVertex.getDegree());
							bucket.addVertex(currentVertex);
							
							if(currentVertex.getDegree() > maxDegree)
								maxDegree = currentVertex.getDegree();
							
							totalAdded++;
							if (EMCoreIntervals.printDebugMessages && totalAdded % EMCoreIntervals.msgEachNode == 0)
								System.out.println("Added vertex "+totalAdded);
							this.totalNodes += (1+currentVertex.getDegree());	
							
							//now check if need to empty bucket
							if (bucket.getTotalNodesCount() > maxTotalNodes)
							{
								collectUBCounts (bucket);
						    	if (this.currentBucketFileID >= EMCoreIntervals.maxFilesPerFolder)
								{
									this.currentFolderID++;
									this.currentBucketFileID = 0;
								}
						    	if (!bucket.flushReset(this.currentFolderID,this.currentBucketFileID++))
						    		System.exit(1);
							}
						}
						currentVertex = new Vertex(parentVertexID);
						currentvertexID = parentVertexID;
					}
					currentVertex.addAdjVertex(childVertexID);
					lineID++;
					if (EMCoreIntervals.printDebugMessages && lineID % 1000000 == 0)
						System.out.println("Processed line "+lineID);
				}
			}
		}
		catch (Exception ge)
		{
			System.out.println("Unexpected error when performing input partitioning: "+ge.getMessage());
		    return false;
		}
		
		//add the last vertex
		currentVertex.setUBCore(currentVertex.getDegree());
		bucket.addVertex(currentVertex);
		if(currentVertex.getDegree() > maxDegree)
			maxDegree = currentVertex.getDegree();
		
		totalAdded++;
		
		//empty last bucket
		collectUBCounts (bucket);
    	if (this.currentBucketFileID >= EMCoreIntervals.maxFilesPerFolder)
		{
			this.currentFolderID++;
			this.currentBucketFileID = 0;
		}
    	if (!bucket.flushReset(this.currentFolderID,this.currentBucketFileID++))
    		System.exit(1);
		
		if(EMCoreIntervals.printAnalysisMessages)
		{
			System.out.println("Total lines processed = "+lineID);
			System.out.println("Total nodes in the graph = "+totalAdded);
			System.out.println("Max vertex degree = "+this.maxDegree);
			System.out.println("Total edges = "+this.totalNodes);
		}
		return true;
	}
	
	private void collectUBCounts (Bucket b)
	{
		if (EMCoreIntervals.printAnalysisMessages)
			totalLineWrites+=(b.getTotalVertices());
		for (int i=0; i< b.getTotalVertices(); i++)
		{
			int ubCoreClass = b.getVertexByPosition(i).getUBCore();			
	    	
			int totalCountForThisClass = 1;
			if (this.coreClassCounts.containsKey(ubCoreClass))
				totalCountForThisClass = this.coreClassCounts.get(ubCoreClass) +1;
			this.coreClassCounts.put(ubCoreClass, totalCountForThisClass);
			
			if (EMCoreIntervals.printAnalysisMessages)
			{

				int degree =  b.getVertexByPosition(i).getDegree();
				int totalCountForThisDegree = 1;
				if(	degreeCounts.containsKey(degree))
					totalCountForThisDegree += degreeCounts.get(degree);
				degreeCounts.put(degree, totalCountForThisDegree);
			}
		}
	}
  
}