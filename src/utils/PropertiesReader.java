package utils;

import java.io.*;
import java.util.*;

public class PropertiesReader {
	
	String fileName;
	Properties dictionary;
	public PropertiesReader (String propertiesFileName)
	{
		this.fileName = propertiesFileName;		
	}

	public boolean load ()
	{
		try
		{
			File f = new File (fileName);
			FileInputStream finput = new FileInputStream (f);
			this.dictionary = new Properties();
			this.dictionary.load(finput);
			finput.close();
		}
		catch (Exception e)
		{
			System.out.println("Error reading properties file "+fileName+": "+e.getMessage());
			return false;
		}
		return true;
	}
	
	public Properties getDictionary ()
	{
		return this.dictionary;
	}
}
