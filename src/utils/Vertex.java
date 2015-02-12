package utils;
import java.util.*;

public class Vertex {
	private int ID;
	private List <Integer> adjVertices;
	
	private int depositCount;
	private int UBCore;  //estimated core - for the paper algorithm
	
	
	
	public Vertex (int id)
	{
		this.ID = id;
		adjVertices = new ArrayList <Integer> ();		
		depositCount =0;
		UBCore = 0;
	}
	
	public void setUBCore (int val)
	{
		UBCore = val;
	}
	
	public int getUBCore ()
	{
		return UBCore;
	}
	
	public void addDepositToken ()
	{
		this.depositCount++;
	}
	
	public int getDepositCount ()
	{
		return this.depositCount;
	}
	
	
	
	public void addAdjVertex (int adjID)
	{
		adjVertices.add(new Integer(adjID));
		this.UBCore ++;
	}
	
	public void removeAdjVertex (int adjID)
	{
		for (int i=0; i< adjVertices.size(); i++)
		{
			if (adjVertices.get(i).intValue() == adjID)
			{
				adjVertices.remove(i);
				return;
			}
		}
	}
	
	public Integer getAdjvertexID (int index)
	{
		return adjVertices.get(index);
	}
	
	public void replaceAdjVertices (List <Integer>  newList)
	{
		adjVertices.clear();
		adjVertices.addAll(newList);
	}
	
	public int getDegree ()
	{
		return this.adjVertices.size();
	}	
	
	public int getID()
	{
		return this.ID;
	}
	
	public static Vertex constructFromString (String line)  //parses the vertex object from its string representation
	{		
		String [] fields = line.split(Utils.FIELD_SEPARATOR);
		//4 fields at most, unless no more adj vertices, in this case 3 fileds: ID, deposit count, Upper bound estimation of a core class
		
		//first is vertex id
		int id = Integer.parseInt(fields[0]);		
		Vertex v = new Vertex (id);		
		
		v.depositCount = Integer.parseInt(fields[1]);
		v.UBCore = Integer.parseInt(fields[2]);
		if (fields.length > 3 && !fields[3].equals(""))
		{
			String [] adjVertices = fields[3].split(Utils.VALUE_SEPARATOR);
			for (int j=0; j<adjVertices.length; j++ )
				v.adjVertices.add(Integer.parseInt(adjVertices[j]));
		}
		return v;
	}
	
	//converts each vertex to a string, to save it to a file
	//string representation of each vertex:
	// Field separator- separated: vertex id, deposit count, Upper bound for core, list of adj vertices IDs
	//List of adjvertices IDs is separated by VALUE_SEPARATOR
	public String toString ()
	{
		return this.ID +Utils.FIELD_SEPARATOR +this.depositCount +Utils.FIELD_SEPARATOR+this.UBCore+Utils.FIELD_SEPARATOR+getAdjVerticesString();
	}
	
	private String getAdjVerticesString()
	{
		StringBuffer sb = new StringBuffer("");
		
		for (int i=0; i< this.adjVertices.size(); i++)
		{
			sb.append(adjVertices.get(i));
			if (i<this.adjVertices.size()-1)
				sb.append(Utils.VALUE_SEPARATOR);
		}
		return sb.toString();
	}
}

