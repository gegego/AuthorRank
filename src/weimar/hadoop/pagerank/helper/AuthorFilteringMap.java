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

//authorid paperid
public class AuthorFilteringMap extends Mapper<Object, Text, Text, Text> {
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
			
			String[] strArray = value.toString().split("\t");
			
		    context.write(new Text(strArray[1]), new Text("%"+strArray[0]));
		}
}
