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
	
	public Map <Integer, Map<Integer,Object>> getAllCoreClasses (int minK, int maxK)
	{
		Map <Integer, Map<Integer,Object>> coreClasses = new HashMap <Integer, Map<Integer,Object>>();
		
		boolean done = false;
		
		for (int k=minK; k<=maxK && !done; k++)
		{
			Map <Integer,Object> coreClassDict = getCoreClassDictionaryBottomUp ( k);
			
			if (coreClassDict.keySet().size() >0)
			{
				coreClasses.put(k, coreClassDict );
			//	System.out.println(coreClassDict);
				
				if(list.size() == 0)
					done = true;
			}
		}
		return coreClasses;
	}
	
	private Map <Integer, Object> getCoreClassDictionaryBottomUp (int k)
	{		
		//remove every node from this subgraph that is not connected to at least k remaining nodes (+deposit count), and repeat until no more removals
		Map <Integer,Object> coreClassDict = new HashMap <Integer,Object>();
		
		//the first step is to remove all vertices with number of edges less than k
		//================
		//1. removing invalid nodes
		//=========================
		Map <Integer,Object> invalidIdsMap = new HashMap <Integer,Object> ();//we will remove the vertices which do not have enough children in this graph	
		
		
		//initializing invalid ids, and removing all vertices that are not in this partial graph from adjacency lists
		for (int i=0; i<this.list.size(); i++)
		{
			Vertex v = this.list.get(i);
			int validCount =v.getDepositCount();
			List <Integer>newAdjVertices = new ArrayList<Integer>();
			for (int a = 0; a < v.getDegree(); a++) //counting how many edges from v in this subgraph
			{
				int adjID = v.getAdjvertexID(a);
				if (verticesDictionary.containsKey(adjID))
				{
					newAdjVertices.add(adjID);
					validCount++;
				}		
			}
			v.replaceAdjVertices(newAdjVertices);
			if (validCount  < k)
			{
				invalidIdsMap.put(v.getID(), null);
			}
		}
		
		//remove less than k class nodes from the verticesDictionary and from the list, this will decrease some vertices degrees and make them invalid too
		//so we continue until all invalid (<k) vertices are removed
		
		if (invalidIdsMap.keySet().size()>0)
		{
			boolean allInvalidRemoved = false;
			
			while (!allInvalidRemoved)
			{
				allInvalidRemoved = true;
				int prevValidCount = this.list.size();
				int prevInvalidCount = invalidIdsMap.keySet().size();
				List <Vertex> newList = new ArrayList <Vertex> ();
				for (int i=0; i< this.list.size(); i++)
				{
					Vertex v = this.list.get(i);
					
					if (invalidIdsMap.containsKey(v.getID()))  //this vertex contains less than k children
					{
						allInvalidRemoved = false;
						verticesDictionary.remove(v.getID());	//and dont add it to a new updated list of nodes						
					}
					else //this is valid vertex so far
					{
						//check if v did not become invalid itself, after previous iteration
						if (v.getDegree()+v.getDepositCount() < k)
						{
							invalidIdsMap.put(v.getID(), null);
							verticesDictionary.remove(v.getID());	
							allInvalidRemoved = false;
						}
						else
						{
							//remove invalid children
							List <Integer> newAdjVertices = new ArrayList <Integer>();
							for (int a =0; a< v.getDegree(); a++)
							{
								int aID = v.getAdjvertexID(a);
								if (!invalidIdsMap.containsKey(aID))
									newAdjVertices.add(aID);
								else
									allInvalidRemoved = false;
							}
							v.replaceAdjVertices(newAdjVertices);
							newList.add(v);
						}						
					}					
				}
				this.list = newList;
				
				int validCount = this.list.size();
				int invalidCount = invalidIdsMap.keySet().size();
				
				if (validCount != prevValidCount)
					allInvalidRemoved = false;
			}
		}
		//=================================
		//2. Collect nodes with the lowest degree k, remove them from the verticesDictionary and from the list
		//======================================
		//now, all the remaining nodes are of at least k degree
		//we are going in a loop now and collect degree k nodes
		//then remove them from the graph, and collect all with degree <=k until no more changes
		boolean allKcoreCollected = false;
		while (!allKcoreCollected)
		{
			allKcoreCollected = true;
			//now we are going through all remaining vertices and remove all with core class = k, adding them to coreClassDict
			List <Vertex> newList = new ArrayList <Vertex> ();
			for (int i=0; i< this.list.size(); i++)
			{
				Vertex v = this.list.get(i);
				
				//count how many valid children
				int validCount =v.getDepositCount();
				for (int a = 0; a < v.getDegree(); a++) //counting how many edges from v in this subgraph
				{
					int adjID = v.getAdjvertexID(a);
					if (verticesDictionary.containsKey(adjID))
					{
						validCount++;
					}		
				}
				
				//collect k core nodes
				if (validCount  <= k)
				{
					allKcoreCollected = false;
					coreClassDict.put(v.getID(), null);												
				}
			}
			
			
			//now remove k-core nodes from partial graph, from list and from and adjacency lists of remaining nodes
			newList = new ArrayList <Vertex> ();
			for (int i=0; i< this.list.size(); i++)
			{
				Vertex v = this.list.get(i);
				int vID = v.getID();
				if (coreClassDict.containsKey(vID))
				{
					allKcoreCollected = false;
					verticesDictionary.remove(v.getID());	
				}
				//remove children of class k
				else
				{
					List <Integer> newAdjVertices = new ArrayList <Integer>();
					for (int a =0; a< v.getDegree(); a++)
					{
						int aID = v.getAdjvertexID(a);
						if (!coreClassDict.containsKey(aID))
							newAdjVertices.add(aID);
						else
							allKcoreCollected = false;
					}
					v.replaceAdjVertices(newAdjVertices);
					newList.add(v);	
				}
			}
			this.list = newList;
		}		
		
		return coreClassDict;
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