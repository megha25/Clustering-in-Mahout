import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class YelpReducer extends
    Reducer<Text, Text, Text, Text> {
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  String s="";
	  int count = 0;
	  String str="";
	  
	  String business="";
	  
	  String review="";
	  
	  
	  
	  
	  
	  
	  
	  
    //int sum = 0;
    //Summing up the counts for each word
 for (Text value : values) 
   {

s=s + " " +value.toString();
	 
     count++;
   }
   
   if(count>=50)
   {
	  
	  // System.out.println(s);
	    business = key.toString();
	    review=s;	    
	   
   }
   
   System.out.println(business + " " + review);
   context.write(new Text(business), new Text(review));
	  
  }
}
