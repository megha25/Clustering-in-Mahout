
import java.io.*;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.*;

import com.sun.jersey.api.json.JSONConfiguration.Builder;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class YelpMapper extends Mapper<LongWritable, Text, Text, Text> {
    
	//The path to the stopWords file.         
	//final static String STOP_WORD_FILE = "/Downloads/stopWords"; 
	
            
			
         //   String stopWords[]=null;
            public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
			{
				JSONObject jsn;
				String text="";
				String ln="";
				String t="";
				String stop[]=new String[600];
				String business_id="";
				// String[] words=null;
				 
				 ArrayList<String> list = new ArrayList<>();
				
				int j=0;
				
				int k=0;
				
				try
				{
					//String data=value.toString();
					//Extracting business_id and text fields from the json object
					jsn = new JSONObject(value.toString());
					
										
					
				Pattern p =Pattern.compile("\"text\"[a-zA-z\\s]*");
				Matcher m = p.matcher(value.toString());
				
				FileReader stp_file = new FileReader("stopWords.txt");
				BufferedReader b = new BufferedReader(stp_file);
				
				j=jsn.names().length();
				
					
				if(m.find())
				{
				     
					text = (String)jsn.get("text");
				    
					 while((ln=b.readLine())!=null)
				     {
				    	
				    	 
				    	 
				    	  stop[k] = ln;
				    	
				    	  k++;
				    	 
				    	
				     }
				   
				
				 
				for(k=0;k<stop.length;k++)  {
					//i(text.contains(stop[i])){
						 text=text.replaceAll("\\b"+stop[k]+"\\s+"+"\\b"," ");
						 
					 }
				 }
				//System.out.println(text);
				b.close();
		
				Pattern p1 =Pattern.compile("\"business_id\"[a-zA-z\\s0-9]*");
				Matcher m1 = p1.matcher(value.toString());
				if(m1.find())
				{
					business_id = (String)jsn.get("business_id");
					//System.out.println(business_id);
				}
				context.write(new Text(business_id), new Text(text));
				}

				 catch(JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
}

