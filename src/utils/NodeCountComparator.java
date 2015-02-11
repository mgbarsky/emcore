package utils;

public class NodeCountComparator implements java.util.Comparator<BucketID_NodeCount>{
	//always sorts ascending - with max count on top
	public int compare (BucketID_NodeCount bnc1, BucketID_NodeCount bnc2)
	{		
		return (bnc1.getCount() - bnc2.getCount());
	}

}