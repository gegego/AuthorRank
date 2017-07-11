package weimar.hadoop.pagerank.helper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//paperid referencepaperid
public class PaperReferenceMap extends Mapper<Object, Text, Text, Text>{
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
				
				String[] strArray = value.toString().split("\t");
				
			    context.write(new Text(strArray[0]), new Text("%"+strArray[1]));				
			}
}
