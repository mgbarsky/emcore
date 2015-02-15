package emcore;
import java.util.*;

public interface PartitionerInterface {
	public boolean init (String inputFileName, char delimiterChar, 
			int maxTotalNodes, int maxBucketSize, int minBucketSize);
	public long getTotalReads();
	
	public long getTotalWrites();
	
	public int getMaxDegree ();
	
	public int getMaxUBCore ();
	
	public int getTotalFolders ();
	
	public int getTotalFilesLastFolder ();
	
	public Map <Integer,Integer> getCoreClassCounts();
	
	public Map <Integer,Integer> getDegreeCounts ();
	public boolean perform (boolean tightenUB, boolean limitUBTightening, long msTimeBoundPerBucket);
	
	
}
