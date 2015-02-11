package utils;


public class UBCoreComparator implements java.util.Comparator<Vertex> {
	private boolean up = true;
	
	public UBCoreComparator()
	{
		
	}
	
	public UBCoreComparator (boolean up)
	{
		this.up = up;
	}
	
	public int compare (Vertex v1, Vertex v2)
	{
		if (this.up) //sort ascending
			return (v1.getUBCore() - v2.getUBCore());
		return (v2.getUBCore() - v1.getUBCore()); //else - sort descending
	}
}