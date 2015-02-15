package emcore;

import utils.*;
import java.util.*;

public class EMCoreIntervals {
	public static final boolean printDebugMessages = false;
	public static int msgEachNode = 10000;
	public static int maxFilesPerFolder=512;
	public static final boolean printAnalysisMessages=true;
	
	public static void main (String [] args)
	{
		int maxTotalNodes = 15000000;
		int maxBucketSize = 1024;
		int minBucketSize = 512;
		long msTimeBoundPerBucket = 1000;
		char delimiterChar = '\t';
		boolean tighenBounds = true;
		boolean limitTighteningTime = false;
		
		long totalDiskReads =0;
		long totalDiskWrites = 0;
				
		int maxNodesPerIteration = 1000000000;
		 
		if (args.length < 1)
		{
			String msg = "Provide the name of the input graph file and  "+System.getProperty("line.separator");
			msg+="[optional] the name of the properties file";
						
			System.out.println (msg);
			System.exit(0);
		}
		
		String inputFileName = args[0];
		
		if (args.length >1) //read properties from file
		{
			String propertiesFileName = args[1];
		
			//read program parameters defined in the properties file
			PropertiesReader pReader = new PropertiesReader (propertiesFileName);
			if (!pReader.load()) return;
		
			Properties params = pReader.getDictionary();
			try
			{
				delimiterChar = params.getProperty("inputDelimiter").charAt(0);
				
				msTimeBoundPerBucket = Long.parseLong(params.getProperty("msTimeBoundPerBucket"));
				tighenBounds = Boolean.parseBoolean(params.getProperty("tighenBounds"));
				limitTighteningTime = Boolean.parseBoolean(params.getProperty("limitTighteningTime"));
				//printDebugMessages = Boolean.parseBoolean(params.getProperty("printDebugMessages"));;
				msgEachNode = Integer.parseInt(params.getProperty("msgEachNode"));
				
				maxTotalNodes = Integer.parseInt(params.getProperty("maxTotalNodes"));
				maxBucketSize = Integer.parseInt(params.getProperty("maxBucketSize"));
				minBucketSize = Integer.parseInt(params.getProperty("minBucketSize"));
				
				maxFilesPerFolder = Integer.parseInt(params.getProperty("maxFilesPerFolder"));
				maxNodesPerIteration = Integer.parseInt(params.getProperty("maxNodesPerIteration"));
				
				//printAnalysisMessages = Boolean.parseBoolean(params.getProperty("printAnalysisMessages"));;
			}
			catch (Exception e){
				e.printStackTrace();
				return;
			}
		}		
		
		
		PartitionerInterface p = null;
		if (tighenBounds)
			p= new Partitioner();
		else
			p=new SimplePartitioner();		
		
		if (!p.init(inputFileName, delimiterChar, maxTotalNodes, maxBucketSize, minBucketSize))
			System.exit(1);
		
		long startTime = System.currentTimeMillis();
		if (!p.perform(tighenBounds, limitTighteningTime, msTimeBoundPerBucket))
			System.exit(1);
		long endTime = System.currentTimeMillis();
		if (printDebugMessages) System.out.println ("Total time for partitioning "+(endTime - startTime) +" ms.");
		if (printAnalysisMessages)
		{
			System.out.println ("During partitioning Total line reads="+p.getTotalReads()+" total node writes="+p.getTotalWrites());
			totalDiskReads+=p.getTotalReads();
			totalDiskWrites+=p.getTotalWrites();
		}
		
		
		startTime = System.currentTimeMillis();
		int maxK = p.getMaxUBCore(); //5
		int maxDegree = p.getMaxDegree();
		
		Map <Integer,Integer> coreClassCounts = p.getCoreClassCounts();
		//if (printAnalysisMessages)
		//{
		//	System.out.println (getSortedmapString(p.getDegreeCounts(), maxDegree) );
		//	System.out.println (getSortedmapString(coreClassCounts, maxK) );			
		//}
		
		//if (printDebugMessages)
		//{
		//	Iterator<Integer>it = coreClassCounts.values().iterator();
		//	int totalCounts =0;
		//	while(it.hasNext())
		//	{
		//		totalCounts += it.next();
		//	}
		//	System.out.println ("Total candidates count = "+totalCounts);
		//}
		int totalBlocksInlastFolder =  p.getTotalFilesLastFolder();
		int totalFolders = p.getTotalFolders();
		
		if (printDebugMessages) System.out.println ("Total folders: "+totalFolders +"; total files in each folder: "
				+EMCore.maxFilesPerFolder+"("+totalBlocksInlastFolder+" in the last folder).");
		int k=maxK;
		
		while (k>0)
		{
			int totalNodesInInterval =0;
			int maxIterationK = -1;
			while (k > 0 && totalNodesInInterval < maxNodesPerIteration)
			{
				if (coreClassCounts.containsKey(k) )
				{
					int totalCandidateNodes = coreClassCounts.get(k);
					int nextlevelCounts =0;
					if (totalCandidateNodes >= k || totalNodesInInterval >= k)  //promising
					{
						if (maxIterationK == -1)
							maxIterationK = k;
						totalNodesInInterval+= totalCandidateNodes;
					}
					else //none of these nodes will be of k-core, not enough total nodes in the graph
					{
						nextlevelCounts = totalCandidateNodes;
						if (coreClassCounts.containsKey(k-1))
							nextlevelCounts += coreClassCounts.get(k-1);
						coreClassCounts.put(k-1, nextlevelCounts);
					}	
				}
				k--;
			}
			
			int minIterationK = k+1;
				
			int nextlevelCounts = 0;
			
			if (printAnalysisMessages) System.out.println("Processing interval of core classes from "+minIterationK 
					+" to "+maxIterationK+" : total "+totalNodesInInterval+" candidate nodes.");
			
			IterationIntervalProcessor it = new IterationIntervalProcessor(totalFolders,  totalBlocksInlastFolder,minIterationK,maxIterationK);
			if (!it.process())
				return;
			if (printAnalysisMessages)
			{
				totalDiskWrites +=it.getTotalWrites();
				totalDiskReads+=it.getTotalReads();
			}
			
			int goodCandidates = it.getTotalNodesInInterval();
			if (printAnalysisMessages) 
			{
				System.out.println ("Found total "+goodCandidates +" nodes for this core classes interval.");
				for (int d=minIterationK; d <= maxIterationK; d++)
				{
					if (it.getCountKCoreClass(d)>0)
						System.out.println ("Total," + it.getCountKCoreClass(d) + ",class,"+d);
				}
			}
			//remaining nodes are going to be processed at the next, lower k
			nextlevelCounts = totalNodesInInterval - goodCandidates;			
				
			if (nextlevelCounts >0)
			{
				if (coreClassCounts.containsKey(k))
					nextlevelCounts += coreClassCounts.get(k);
				coreClassCounts.put(k, nextlevelCounts);
			}	
		}
		
		endTime = System.currentTimeMillis();
		System.out.println ("Total time for EMcore algorithm "+(endTime - startTime) +" ms.");
		if (printAnalysisMessages)
			System.out.println("Total disk reads="+totalDiskReads+" total disk writes="+totalDiskWrites);
	}
	
	private static String getSortedmapString (Map <Integer, Integer> map, int maxKey)
	{
		StringBuffer sb = new StringBuffer("");
		for (int i = maxKey; i >0; i--)
		{
			if (map.containsKey(i))
				sb.append(i).append(",").append(map.get(i)).append(";");
		}
		
		return sb.toString();		
	}
}