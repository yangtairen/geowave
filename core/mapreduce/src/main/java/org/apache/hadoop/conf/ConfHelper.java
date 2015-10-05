package org.apache.hadoop.conf;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfHelper
{
	 /**
	   * get keys matching the the regex 
	   * @param regex
	   * @return Map<String,String> with matching keys
	   */
	  public static Map<String,String> getValByRegex(Configuration conf, String regex) {
	    Pattern p = Pattern.compile(regex);

	    Map<String,String> result = new HashMap<String,String>();
	    Matcher m;

	    for(Map.Entry<Object,Object> item: conf.getProps().entrySet()) {
	      if (item.getKey() instanceof String && 
	          item.getValue() instanceof String) {
	        m = p.matcher((String)item.getKey());
	        if(m.find()) { // match
	          result.put((String) item.getKey(),conf.getProps().getProperty((String) item.getKey()));
	        }
	      }
	    }
	    return result;
	  }
}
