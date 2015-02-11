package utils;

/**
 * Comparator to sort list of buckets: default -  in descending order: from bigger to smaller
 * Used to get the fullest bucket
 * @author marina
 *
 */
public class BucketsCountsComparator implements java.util.Comparator<Bucket> {
	private boolean asc = false;
	
	public BucketsCountsComparator()
	{
		
	}
	
	public BucketsCountsComparator (boolean fromsmalltobig)
	{
		this.asc = fromsmalltobig;
	}
	
	public int compare (Bucket b1, Bucket b2)
	{
		if (this.asc) //sort ascending
			return (b1.getTotalNodesCount()- b2.getTotalNodesCount());
		return (b2.getTotalNodesCount()- b1.getTotalNodesCount()); //else - sort descending
	}
}