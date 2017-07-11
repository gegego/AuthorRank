package weimar.hadoop.pagerank.helper;

import java.io.IOException;
 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanupResultsReduce extends Reducer<Text, Text, Text, Text>{
public void reduce(Text key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {
	context.write(key, values.iterator().next());
}
}