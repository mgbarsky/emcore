package emcore;

import utils.*;
import java.util.*;

public class EMCore {
	public static boolean printDebugMessages = true;
	public static int msgEachNode = 10000;
	public static int maxFilesPerFolder=512;
	public static boolean printAnalysisMessages=true;
	
	public static void main (String [] args)
	{
		int maxTotalNodes = 15000000;
		int maxBucketSize = 1024;
		int minBucketSize = 512;
		long msTimeBoundPerBucket = 1000;
		char delimiterChar = '\t';
		boolean tighenBounds = true;
		boolean limitTighteningTime = false;
		
		
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
				printDebugMessages = Boolean.parseBoolean(params.getProperty("printDebugMessages"));;
				msgEachNode = Integer.parseInt(params.getProperty("msgEachNode"));
				
				maxTotalNodes = Integer.parseInt(params.getProperty("maxTotalNodes"));
				maxBucketSize = Integer.parseInt(params.getProperty("maxBucketSize"));
				minBucketSize = Integer.parseInt(params.getProperty("minBucketSize"));
				
				maxFilesPerFolder = Integer.parseInt(params.getProperty("maxFilesPerFolder"));
				maxNodesPerIteration = Integer.parseInt(params.getProperty("maxNodesPerIteration"));
				
				printAnalysisMessages = Boolean.parseBoolean(params.getProperty("printAnalysisMessages"));;
			}
			catch (Exception e){
				e.printStackTrace();
				return;
			}
		}		
		
		
		Partitioner p = new Partitioner();
		
		if (!p.init(inputFileName, delimiterChar, maxTotalNodes, maxBucketSize, minBucketSize))
			System.exit(1);
		
		long startTime = System.currentTimeMillis();
		if (!p.perform(tighenBounds, limitTighteningTime, msTimeBoundPerBucket))
			System.exit(1);
		long endTime = System.currentTimeMillis();
		if (printDebugMessages) System.out.println ("Total time for partitioning "+(endTime - startTime) +" ms.");
		
		
		startTime = System.currentTimeMillis();
		int maxK = p.getMaxUBCore(); //5
		int maxDegree = p.getMaxDegree();
		if (printAnalysisMessages)
		{
			System.out.println (getSortedmapString(p.degreeCounts, maxDegree) );
			System.out.println (getSortedmapString(p.coreClassCounts, maxK) );			
		}
		int totalBlocksInlastFolder =  p.getTotalFilesLastFolder();
		int totalFolders = p.getTotalFolders();
		
		if (printDebugMessages) System.out.println ("Total folders: "+totalFolders +"; total files in each folder: "
				+EMCore.maxFilesPerFolder+"("+totalBlocksInlastFolder+" in the last folder).");
		int k=maxK;
		while (k>0)
		{				
			if (p.coreClassCounts.containsKey(k) )
			{
				long sTime = System.currentTimeMillis();
				int totalCandidateNodes = p.coreClassCounts.get(k);
				
				int nextlevelCounts = 0;
				if (totalCandidateNodes >= k)
				{
					
					IterationProcessor it = new IterationProcessor (totalFolders,  totalBlocksInlastFolder,  k);
					if (!it.process())
						return;
					
					int goodCandidates = it.getCountKCoreClass();
					nextlevelCounts = totalCandidateNodes - goodCandidates;
					//long eTime = System.currentTimeMillis();
					if (printDebugMessages) System.out.println ("Total nodes potentially of class "+k+": "+totalCandidateNodes);
					if (goodCandidates > 0)
					{
											
						if (printDebugMessages && it.getCountKCoreClass() >0)  System.out.println("Total nodes with core class "+k + " is "+it.getCountKCoreClass());						
					}
					//if (printDebugMessages) System.out.println ("Total time for processing class "+(k)+" is "+(eTime - sTime) +" ms.");	
				}
				else
				{
					nextlevelCounts = totalCandidateNodes;					
				}
				
				if (nextlevelCounts >0)
				{
					if (p.coreClassCounts.containsKey(k-1))
						nextlevelCounts += p.coreClassCounts.get(k-1);
					p.coreClassCounts.put(k-1, nextlevelCounts);
				}
			}
			k--;			
		}
		
		endTime = System.currentTimeMillis();
		if (printDebugMessages) System.out.println ("Total time for EMcore algorithm "+(endTime - startTime) +" ms.");
	}
	
	private static String getSortedmapString (Map <Integer, Integer> map, int maxKey)
	{
		StringBuffer sb = new StringBuffer("");
		for (int i = maxKey; i >0; i--)
		{
			if (map.containsKey(i))
				sb.append(i).append("=").append(map.get(i)).append(", ");
		}
		
		return sb.toString();		
	}
}