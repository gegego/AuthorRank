package weimar.hadoop.pagerank.helper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import weimar.hadoop.pagerank.HadoopPageRank;

 

public class CreateGraphReduce extends Reducer<Text, Text, Text, Text>{
	
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
	
		int numUrls = context.getConfiguration().getInt("numUrls", 1);
		double rank = 1.0/(double)numUrls;
		int weightsum = 0;
//		Set<String> authors = new HashSet<String>();
		
		Map<String, Integer> authors = new HashMap<String, Integer>();
		
		StringBuffer sb = new StringBuffer();
		sb.append(String.valueOf(rank));
		
		for (Text val : values) {
			
			if(val.toString().startsWith("#")) {
				if(authors.containsKey(val.toString()))
				{
					authors.put(val.toString(), authors.get(val.toString())+1);
				}
				else
				{
					authors.put(val.toString(), 1);
				}
			}			
	    }
		
		for (Map.Entry<String, Integer> entry : authors.entrySet()) {
			weightsum += entry.getValue();
			String val = entry.getKey() + "@" + entry.getValue().toString();
		    sb.append(val);
		}
		sb.insert(0, weightsum+"@");

		context.write(key, new Text(sb.toString()));
	}
}