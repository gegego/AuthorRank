package weimar.hadoop.pagerank;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator; // new import 
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
 
public class PageRankMap extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {

		String line = value.toString();
		RankRecord rrd = new RankRecord(line);
			
		StringBuilder mapOutput = new StringBuilder(); 			
		if (rrd.targetList.size()>0)
		{			
			for(Map.Entry<String, Integer> entry : rrd.targetList.entrySet())
			{
				double weightedRankPerTarget = ((double)entry.getValue()/(double)rrd.weight) * (double)rrd.rankValue;
				context.write(new Text(entry.getKey()), new Text(String.valueOf(weightedRankPerTarget)));
				mapOutput.append("#" + entry.getKey() + "@" + entry.getValue());
			}
			
			context.write(new Text(rrd.sourceID), new Text(mapOutput.toString()));
			context.write(new Text(rrd.sourceID), new Text("@"+rrd.weight));
		} //for
	} // end map
}
