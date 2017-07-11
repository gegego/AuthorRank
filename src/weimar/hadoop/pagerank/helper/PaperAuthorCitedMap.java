package weimar.hadoop.pagerank.helper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//import 
public class PaperAuthorCitedMap extends Mapper<Object, Text, Text, Text>{

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] strArray = value.toString().split("\t");
		
//		String[] paperlist = strArray[1].split("%");
//		for (String paper : paperlist)
//		{
//			context.write(new Text(paper), new Text("%"+strArray[0]));
//		}
		context.write(new Text(strArray[1]), new Text("%"+strArray[0]));
	}
}
