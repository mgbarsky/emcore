package utils;

public class BucketID_NodeCount {
	private int bucketID;
	private int count;
	
	public BucketID_NodeCount (int bucketID)
	{
		this.bucketID = bucketID;
	}
	
	public int getBucketID ()
	{
		return this.bucketID;
	}
	
	public void incrementCount ()
	{
		this.count++;
	}
	
	public int getCount ()
	{
		return this.count;
	}

}
