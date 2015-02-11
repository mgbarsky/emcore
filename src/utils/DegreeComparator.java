package utils;


public class DegreeComparator implements java.util.Comparator<Vertex> {
	private boolean up = true;
	
	public DegreeComparator()
	{
		
	}
	
	public DegreeComparator (boolean up)
	{
		this.up = up;
	}
	
	public int compare (Vertex v1, Vertex v2)
	{
		if (this.up) //sort ascending
			return ((v1.getDegree()+v1.getDepositCount()) - (v2.getDegree()-v2.getDepositCount()));
		return ((v2.getDegree()+v2.getDepositCount()) - (v1.getDegree()-v1.getDepositCount())); //else - sort descending
	}
}