package utils;
import java.io.*;
import java.util.*;

public class DefaultPropertiesWriter {
	String fileName;
	
	public DefaultPropertiesWriter (String propertiesFileName)
	{
		this.fileName = propertiesFileName;
	}
	
	public boolean fillDefaults ()
	{		
		try {
			Properties properties = new Properties();
			
			properties.setProperty("maxTotalNodes", ""+15000000); //max number of all nodes (including adjacency ids ) in memory during partitioning
			properties.setProperty("maxBucketSize", ""+1024); //max number of total nodes (inc. adj) in 1 bucket
			properties.setProperty("minBucketSize", ""+512); //min number of nodes (inc. adj) in 1 bucket
			properties.setProperty ("inputDelimiter",String.valueOf('\t')); //delimiter of an input pair of nodes, each pair represents an edge of the graph
			
			properties.setProperty("tighenBounds", ""+true); //whether to tighten bound for each bucket nodes by the end of partitioning phase
			properties.setProperty("limitTighteningTime", ""+false); //whether to limit time of tightening per bucket
			properties.setProperty("msTimeBoundPerBucket", ""+1000); //how many milliseconds to allocate for tightening core class bound per bucket

			properties.setProperty("maxNodesPerIteration", ""+1000000); //how many max nodes to load into a partial graph at each iteration
			
			properties.setProperty("printDebugMessages", ""+true); //whether to print debug messages
			properties.setProperty("msgEachNode", ""+10000); //print message about processing each node every msgEachNode-th time
			
			
			File file = new File(this.fileName);
			FileOutputStream fileOut = new FileOutputStream(file);
			properties.store(fileOut, "Default EM core parameters");
			fileOut.close();
		} catch (Exception e) {
			System.out.println("error writing default properties to file "+fileName+": "+e.getMessage());
			return false;
		}
		return true;
	}
	
	public static void main (String [] args)
	{
		DefaultPropertiesWriter w = new DefaultPropertiesWriter ("emcore.properties");
		w.fillDefaults();
	}

}
