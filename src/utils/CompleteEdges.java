package utils;

import java.util.*;
import java.io.*;

/**
 * @author mbarsky
 * Takes as an input a graph file
 * each line contains an edge: parentNode ID - delimiter - child node id
 * If a delimiter is different from a tab, specify it as the second program parameter
 * 
 *Then it will add, for each edge, the opposite edge (child node ID - to - parentNode ID)
 *This will complement cases where the input graph is directed and an edge exists only in one direction
 *
 *The resulting file has to be sorted (by parentID, then by child ID)
 *and the duplicate entries removed, before applying the k-core decomposition
 *sort -k 1n,2n amazon0302.txt_COMPLEMENTED > test1.txt
 *uniq test1.txt test2.txt
 *
 *use test2.txt
 */

//this will add bidirectional edges for each existing edge, converting a directed graph to undirected graph
public class CompleteEdges {
	public static void main (String [] args)
	{
		if (args.length < 1)
		{
			System.out.println ("provide the name of the input graph file and  "
					+ "optionally a delimiter for each line in this file ");
			System.exit(0);
		}
		
		BufferedReader reader = null;
		String inputFileName = args[0];
	
		String delimiter = String.valueOf('\t');
		if (args.length >1)
			delimiter = args[1];
				
		//open reader
		try
		{
			File file = new File(inputFileName);
		    reader = new BufferedReader(new FileReader(file));
		}
		catch (Exception ge)
		{
			System.out.println("Unexpected error when reading from File "+inputFileName +": "+ge.getMessage());
			System.exit(1);
		}
		
		//open writer
		BufferedWriter writer = null;
		try
		{
			File outputfile = new File(inputFileName+"_COMPLEMENTED");
			FileWriter fileWriter = new FileWriter(outputfile);
			writer = new BufferedWriter(fileWriter);				
		}
		catch (Exception ge)
		{
			System.out.println("Unexpected error when opening file '"+(inputFileName+"_COMPLEMENTED") +"' for writing: "+ge.getMessage());
		    System.exit(1);
		}
		
		try
		{
			String line;
			while ((line = reader.readLine()) != null) 
			{
				String [] pair = line.split(delimiter);
				int parentVertexID = 0;	
				int childVertexID=0 ;
				boolean validLine = true;
				
				try
				{
					parentVertexID = Integer.parseInt(pair[0]);
					childVertexID = Integer.parseInt(pair[1]);
				}
				catch (Exception e)
				{
					validLine = false;
				}
				
				if (validLine)
				{
					//write this line back to file
					writer.write(parentVertexID+delimiter+childVertexID);
					writer.newLine();
					
					//write the opposite connection (duplicates to be removed later)
					writer.write(childVertexID +delimiter + parentVertexID);
					writer.newLine();					
				}
			}	
			reader.close();
			writer.flush();
			writer.close();
		}
		catch (Exception ge)
		{
			System.out.println("Unexpected error when processing lines of input file to complement edges: "+ge.getMessage());
			System.exit(1);
		}	
	}
}