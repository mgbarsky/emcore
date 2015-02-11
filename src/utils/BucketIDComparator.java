package utils;
/**
 * Resorts buckets to the original order
 * @author marina
 *
 */
public class BucketIDComparator implements java.util.Comparator<Bucket> {
	public int compare (Bucket b1, Bucket b2)
	{		
		return (b1.getBucketPlaceInArray()- b2.bucketPlaceInArray);		
	}
}