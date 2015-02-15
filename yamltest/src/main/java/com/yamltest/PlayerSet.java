package com.yamltest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

/**
 * Hello world!
 *
 */
public class PlayerSet 
{
    public static void main( String[] args )
    {
    	try {
			InputStream input = new FileInputStream(new File(
			        "src/main/java/335985.yaml"));
			Yaml yaml = new Yaml();
	    	//String document = "\n- Hesperiidae\n- Papilionidae\n- Apatelodidae\n- Epiplemidae";
	    	Map list = (HashMap)yaml.load(input);
	    	Map matchInfo = (HashMap)list.get("info");
	    	matchInfo.get("outcome");
	    	System.out.println(list);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
}
