package weimar.hadoop.pagerank.helper;

/*
 * collect the page rank results from previous computation.
 */

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import weimar.hadoop.pagerank.RankRecord;

public class CleanupResultsMap extends Mapper<Object, Text, Text, Text> {
	
	public void map(Object key, Text value, Context context)
	throws IOException, InterruptedException {
		
		String strLine = value.toString();
		RankRecord rrd = new RankRecord(strLine);
		DecimalFormat df = new DecimalFormat("0");
		df.setMaximumFractionDigits(10);
		context.write(new Text(rrd.sourceID), new Text(df.format(rrd.rankValue)));
		}
	
}
