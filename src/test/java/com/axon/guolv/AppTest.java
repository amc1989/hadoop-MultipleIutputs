package com.axon.guolv;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest {
    public static void main(String [] args){
    	System.out.println(getTime("2016-10-1"));
    }
    private static long getTime(String openDate) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date date =null;
		try {
			 date = sdf.parse(openDate);
		} catch (ParseException e) {
			//
		}
		return date.getTime();
	}
}


