package weimar.hadoop.pagerank.helper;
		

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
		
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
		
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
		
import java.lang.StringBuffer;

public class CreateGraphMap extends Mapper<Object, Text, Text, Text> {
	
	
	// creating the am matrix 
	public void map(Object key, Text value, Context context)
	throws IOException, InterruptedException {

//		int numUrls = context.getConfiguration().getInt("numUrls", 1);
//		double val = 1.0/(double)numUrls;
		
		String[] strArray = value.toString().split("\t");
		//StringBuffer sb = new StringBuffer();
		
		String source, target;
		source = strArray[1];
		target = strArray[0];
		//sb.append(String.valueOf(val));
//		if(!source.equals(target))
		context.write(new Text(strArray[1]), new Text("#"+strArray[0]));
//		String[] srclist = value.toString().split("%");
//		for (int i=0 ;i<srclist.length; i++){
////			targetUrl = Integer.parseInt(strArray[i]); 
////			sb.append("#"+targetUrl);
//			
//		} 
		
	}//map
}