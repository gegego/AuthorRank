package weimar.hadoop.pagerank.helper;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import weimar.hadoop.pagerank.HadoopPageRank;

public class CitedCountingReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	public void reduce(Text key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		
		long sum = 0;
		for (LongWritable val : values) {
			sum += val.get();
	    }
		
		context.write(key, new LongWritable(sum));
	}
}
