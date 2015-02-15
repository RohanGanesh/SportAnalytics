package com.yamltest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


/*
 * Author- Rohan G
 * This is the mainclass used to call the yaml processor class
 * 
 * hre we give the local path fot he yaml file
 * 
 * 
 */
public class testyaml {
public static void main(String[] args) {
	YamlProcesser fe = new YamlProcesser();
	InputStream input;
	try {
		
		/*File file = new File("C:\\Users\\Rohan\\Downloads\\ipl\\files\\");
		List<String> fielLIst =listFilesForFolder(file);
		for (int i = 0; i < fielLIst.size(); i++) {
			String fileName = "C:\\Users\\Rohan\\Downloads\\ipl\\files\\"+fielLIst.get(i);
			input = new FileInputStream(new File(fileName));
			 fe.processFile(input,fileName);
			System.out.println(fileName);
		}*/
		input = new FileInputStream(new File("src/main/java/392184.yaml"));
		 fe.processFile(input,"");
		//fe.processFile(input);
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

}




public static List<String> listFilesForFolder(final File folder) {
List<String> fileList =  new ArrayList<String>();
    for (final File fileEntry : folder.listFiles()) {
        if (fileEntry.isDirectory()) {
            listFilesForFolder(fileEntry);
        } else {
           // System.out.println(fileEntry.getName());
            fileList.add(fileEntry.getName());
        }
    }
    return fileList;
}
}
