package emcore;
import java.util.*;

import java.io.*;
import utils.*;

public class Partitioner {
	
	private int maxDegree =0;
	private BufferedReader reader;
	private String delimiter;
	
	private int maxTotalNodes;
	private int maxBucketSize;
	
	private int minBucketSize;
	private int totalNodes;
	
	private List <Bucket> buckets;
	
	private int runningBucketID;
	private int currentBucketFileID;
	
	private int totalNumberOfFolders = 0;
	
	private long msTimeBound;
	private boolean tightenUB = false;
	private boolean limitUBTightening = false;
	private int totalTightenedCount;
	
	private int maxCore=0;
	
		
	//stores where we can find the buckets which contain a vertex with a nodeid 
	Map <Integer,Integer> vertexToBucket = new HashMap <Integer,Integer> () ;
	
	//this stores how many nodes are estimated for each core class
	public Map <Integer,Integer> coreClassCounts = new HashMap <Integer,Integer> () ;
	
	public Map <Integer,Integer> degreeCounts ; 
	
	public boolean init (String inputFileName, char delimiterChar, 
			int maxTotalNodes, int maxBucketSize, int minBucketSize)
	{
		this.delimiter = String.valueOf(delimiterChar);
		this.maxTotalNodes = maxTotalNodes;
		this.maxBucketSize = maxBucketSize;
		this.minBucketSize = minBucketSize;
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
		
		buckets = new ArrayList <Bucket>();
		
		if (EMCore.printAnalysisMessages)
			degreeCounts = new HashMap <Integer,Integer> () ;
		return true;
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
		return this.totalNumberOfFolders;
	}
	public int getMaxUBCore ()
	{
		return this.maxCore;
	}
	
	
	public boolean perform (boolean tightenUB, boolean limitUBTightening, long msTimeBoundPerBucket)
	{
		this.tightenUB = tightenUB;
		this.limitUBTightening = limitUBTightening;
		this.msTimeBound = msTimeBoundPerBucket;
		
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
							int bestBucketID = getBestBucketID (currentVertex);
							
							Bucket bucket = this.buckets.get(bestBucketID);
							
							currentVertex.setUBCore(currentVertex.getDegree());
							bucket.addVertex(currentVertex);
							
							if(currentVertex.getDegree() > maxDegree)
								maxDegree = currentVertex.getDegree();
							
							vertexToBucket.put(currentVertex.getID(), bestBucketID);
							totalAdded++;
							if ( totalAdded % EMCore.msgEachNode == 0)
								System.out.println("Added vertex "+totalAdded);
							this.totalNodes += (1+currentVertex.getDegree());						
						}
						currentVertex = new Vertex(parentVertexID);
						currentvertexID = parentVertexID;
					}
					currentVertex.addAdjVertex(childVertexID);
					lineID++;
					if (EMCore.printDebugMessages && lineID % 1000000 == 0)
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
		int bestBucketID = getBestBucketID (currentVertex);
		
		Bucket bucket = this.buckets.get(bestBucketID);
		currentVertex.setUBCore(currentVertex.getDegree());
		bucket.addVertex(currentVertex);
		if(currentVertex.getDegree() > maxDegree)
			maxDegree = currentVertex.getDegree();
		vertexToBucket.put(currentVertex.getID(), bestBucketID);
		totalAdded++;
		
		//System.out.println(buckets);
		//now flush all remaining buckets
		Bucket combinedBucket = null;
		for (int i=0; i< this.buckets.size(); i++)
		{
			Bucket currBucket = this.buckets.get(i);
			if (this.tightenUB) currBucket.tightenUBCore(this.limitUBTightening, this.msTimeBound);
			this.totalTightenedCount += currBucket.getTightenedCount();
			if (currBucket.getTotalNodesCount() < this.minBucketSize)
			{
				if (combinedBucket == null)
					combinedBucket = new Bucket (-1,this.runningBucketID++);
				
				if (combinedBucket.getTotalNodesCount() + currBucket.getTotalNodesCount() < this.maxBucketSize )
				{
					for (int k=0; k< currBucket.getTotalVertices(); k++)
					{						
						combinedBucket.addVertex (currBucket.getVertexByPosition(k));
					}
				}
				else 
				{					
					collectUBCounts (combinedBucket);
					if (this.currentBucketFileID > EMCore.maxFilesPerFolder)
					{
						this.totalNumberOfFolders++;
						this.currentBucketFileID =0;
					}
					combinedBucket.flushReset(this.totalNumberOfFolders,this.currentBucketFileID++);
					
					
					combinedBucket.setNewBucketID(this.runningBucketID++);
					for (int k=0; k< currBucket.getTotalVertices(); k++)
					{						
						combinedBucket.addVertex (currBucket.getVertexByPosition(k));
					}
				}
			}
			else
			{
				collectUBCounts (this.buckets.get(i));
		    	if (this.currentBucketFileID > EMCore.maxFilesPerFolder)
				{
					this.totalNumberOfFolders++;
					this.currentBucketFileID =0;
				}
				this.buckets.get(i).flushReset(totalNumberOfFolders,this.currentBucketFileID++);					
			}
		}
		
		if (combinedBucket != null && combinedBucket.getTotalNodesCount()>0)
		{			
	    	collectUBCounts (combinedBucket);
	    	if (this.currentBucketFileID > EMCore.maxFilesPerFolder)
			{
				this.totalNumberOfFolders++;
				this.currentBucketFileID = 0;
			}
			combinedBucket.flushReset(this.totalNumberOfFolders,this.currentBucketFileID++);			
		}
		if (EMCore.printDebugMessages) System.out.println("Total lines processed = "+lineID);
		if (EMCore.printDebugMessages) System.out.println("Total nodes in the graph = "+totalAdded);
		
		System.out.println("Max vertex degree = "+this.maxDegree);
		System.out.println("Total tightened UBCore = "+this.totalTightenedCount);
		System.out.println("max UBCore = "+this.maxCore);	
		return true;
	}
	
	private void collectUBCounts (Bucket b)
	{
		for (int i=0; i< b.getTotalVertices(); i++)
		{
			int ubCoreClass = b.getVertexByPosition(i).getUBCore();			
	    	if (this.maxCore < ubCoreClass)
	    		this.maxCore = ubCoreClass;
			int totalCountForThisClass = 1;
			if (this.coreClassCounts.containsKey(ubCoreClass))
				totalCountForThisClass = this.coreClassCounts.get(ubCoreClass) +1;
			this.coreClassCounts.put(ubCoreClass, totalCountForThisClass);
			
			if (EMCore.printAnalysisMessages)
			{
				int degree =  b.getVertexByPosition(i).getDegree();
				int totalCountForThisDegree = 1;
				if(	degreeCounts.containsKey(degree))
					totalCountForThisDegree += degreeCounts.get(degree);
				degreeCounts.put(degree, totalCountForThisDegree);
			}
		}
	}
    private int getBestBucketID (Vertex v)
    {    	
    	int totalAdjNodes = v.getDegree();
    	
    	Map <Integer,Integer> dictCounts = new HashMap <Integer,Integer> (); 
    	
    	for (int j=0; j<totalAdjNodes; j++)
		{
			int currAdjVertexID = v.getAdjvertexID(j);
			//see if we have a bucket that contains this node
			if (this.vertexToBucket.containsKey(currAdjVertexID))
			{
				int bucketPos = this.vertexToBucket.get(currAdjVertexID);
				
				int count = 1;
				if (dictCounts.containsKey(bucketPos))
					count += dictCounts.get(bucketPos);
				
				dictCounts.put(bucketPos, count);
			}			
		}
    	
    	
    	//select the best from dictCounts (with the best count, and next which allows to add more elements)
    	//for this we need to sort it in descending order of values (counts)
    	IntValueComparator bvc =  new IntValueComparator(dictCounts);
        TreeMap<Integer,Integer> sorted_map = new TreeMap<Integer,Integer>(bvc);
    	
        //make the map sorted - by counts, descending
        sorted_map.putAll(dictCounts);
        
        
        Set set = sorted_map.entrySet();
     //   System.out.println(set);
        //iterate over it
        Iterator it = set.iterator();
        
        while (it.hasNext())
        {
        	Map.Entry me = (Map.Entry)it.next();
        	int bucketID = (int)me.getKey();
        	Bucket currBucket = buckets.get(bucketID);
        	if ((currBucket.getTotalNodesCount()+totalAdjNodes+1) < this.maxBucketSize)
			{
        		return bucketID;  //found best bucket - return it
			}
        }  
    	
        //there are no good buckets to put this node in
    	//if there is no non-empty buckets to place our node into:
    	//either all are almost full, or do not contain its adjacent nodes
    	//put it into an empty bucket - either at the end of the buckets array
    	//or after emptying the most full bucket
    	if (this.totalNodes + totalAdjNodes +1  < this.maxTotalNodes)
    	{
    		this.buckets.add(new Bucket(this.buckets.size() ,runningBucketID++));
    		return this.buckets.size() - 1;
    	}  
    	
    	//cant place into existing buckets, and number of allowed buckets is exhausted
    	//we need to flush one of the existing buckets - find which by finding the most full
    	Collections.sort(this.buckets, new BucketsCountsComparator());
    	//int mostNodes =this.buckets.get(0).getTotalNodesCount();
    	int mostFullBucketID = this.buckets.get(0).getBucketPlaceInArray();
    	
    	//restore original order
    	Collections.sort(this.buckets, new BucketIDComparator());
    	    	
    	//now flush the most full bucket to free space
    	this.totalNodes -= this.buckets.get(mostFullBucketID).getTotalNodesCount();
    	if (this.tightenUB) this.buckets.get(mostFullBucketID).tightenUBCore(this.limitUBTightening,this.msTimeBound);
    	
    	
    	this.totalTightenedCount+=this.buckets.get(mostFullBucketID).getTightenedCount();
    	
    	//now we need to remove all references to this bucket from the mapping nodeid to bucketid position
    	for (int j=0; j<this.buckets.get(mostFullBucketID).getTotalVertices(); j++)
		{
			int currVertexID = this.buckets.get(mostFullBucketID).getVertexByPosition(j).getID();
			
			this.vertexToBucket.remove(currVertexID);						
		}
    	
    	collectUBCounts (this.buckets.get(mostFullBucketID));
    	if (this.currentBucketFileID > EMCore.maxFilesPerFolder)
		{
			this.totalNumberOfFolders++;
			this.currentBucketFileID = 0;
		}
    	if (!this.buckets.get(mostFullBucketID).flushReset(this.totalNumberOfFolders,this.currentBucketFileID++))
		
    	this.buckets.get(mostFullBucketID).setNewBucketID( this.runningBucketID++);
    	return mostFullBucketID;
    }
}