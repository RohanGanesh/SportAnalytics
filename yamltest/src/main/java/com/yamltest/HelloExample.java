package com.yamltest;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.log4j.Logger;

public class HelloExample{
	
	final static Logger logger = Logger.getLogger(HelloExample.class);
	
	public static void main(String[] args) {
	
		String commitDate= "2015/01/21";
		Date date = null;
		DateFormat format = new SimpleDateFormat("yyyy/mm/dd",
				Locale.ENGLISH);
		try {
			date= format.parse(commitDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println(date);
		
	}
	
	private void runMe(String parameter){
		
		if(logger.isDebugEnabled()){
			logger.debug("This is debug : " + parameter);
		}
		
		if(logger.isInfoEnabled()){
			logger.info("This is info : " + parameter);
		}
		
		logger.warn("This is warn : " + parameter);
		logger.error("This is error : " + parameter);
		logger.fatal("This is fatal : " + parameter);
		
	}
	
}