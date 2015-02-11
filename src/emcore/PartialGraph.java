package emcore;
import utils.*;
import java.util.*;

public class PartialGraph {
	int totalVertices;
	int totalCount;
	List <Vertex> list;
	Map <Integer,Vertex> verticesDictionary;
	
	public PartialGraph ()
	{
		list = new ArrayList <Vertex>();
		verticesDictionary = new HashMap <Integer,Vertex> ();
	}

	public void addVertex (Vertex v)
	{
		this.list.add(v);
		verticesDictionary.put(v.getID(), v);
	}
	
	public int getTotalVertices()
	{
		return this.list.size();
	}
	
	public Map <Integer, Object> getCoreClassDictionary (int k)
	{
		boolean done = false;
		
		//remove every node from this subgraph that is not connected to at least k remaining nodes, and repeat until no more removals
		Map <Integer,Object> coreClassDict = new HashMap <Integer,Object>();
		while (!done)
		{	
			done =true;
			List <Integer> idsToRemove = new ArrayList <Integer>();			
		
			for (int i=0; i<this.list.size(); i++)
			{
				Vertex v = this.list.get(i);
				int validCount =v.getDepositCount();
				for (int a = 0; a < v.getDegree(); a++)
				{
					int adjID = v.getAdjvertexID(a);
					if (verticesDictionary.containsKey(adjID))
					{
						validCount++;
					}		
				}
				if (validCount < k)
					idsToRemove.add(v.getID());
			}
			
			if (idsToRemove.size()>0)
			{
				done =false;
				for (int j=0; j < idsToRemove.size(); j++)
				{
					Integer id = idsToRemove.get(j);					
					verticesDictionary.remove(id);
				}
				
				List <Vertex> newList = new ArrayList <Vertex>();
				for (int j=0; j< this.list.size(); j++ )
				{
					Vertex v = this.list.get(j);
					if (verticesDictionary.containsKey(v.getID()))
						newList.add(v);
				}
				this.list = newList;
			}
		}		
		
		Iterator <Integer> it = this.verticesDictionary.keySet().iterator();
		while (it.hasNext())
		{
			coreClassDict.put(it.next(), null);
		}
		return coreClassDict;
	}	
	
}