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
		
public class FilterAuthorMap extends Mapper<Object, Text, Text, LongWritable> {

    private final static LongWritable one = new LongWritable(1);

	public void map(Object key, Text value, Context context)
	throws IOException, InterruptedException {
		
		String[] strArray = value.toString().split("\t");
		
		String authorid;
		authorid = strArray[1];
		
	    context.write(new Text(authorid), one);
		
	}
}
