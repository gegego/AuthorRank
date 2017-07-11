package weimar.hadoop.pagerank;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.lang.StringBuffer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
 
public class PageRankReduce extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		double sumOfRankValues = 0.0;
		int weight = 0;
		String targetIDsList = "";
		StringBuffer sb = new StringBuffer();
		
		String sourceID = key.toString();
		int numUrls = context.getConfiguration().getInt("numUrls",1);
		
		for (Text value: values){
			String[] strArray = value.toString().split("#");
			if (strArray.length == 1){
				
				if(strArray[0].startsWith("@"))
				{
					weight += Integer.parseInt(strArray[0].substring(1));
				}
				else
				{
					sumOfRankValues += Double.parseDouble(strArray[0]);
				}				
			}
			else{
				for (int i = 1; i < strArray.length; i++) {
					targetIDsList += "#" + strArray[i];
				}
			}
		} // end for loop
		sumOfRankValues = sumOfRankValues * 0.85 + 0.15*(1.0)/(double)numUrls;
		
		sb.append(weight);
		sb.append("@"+sumOfRankValues);
		sb.append(targetIDsList);

		context.write(key, new Text(sb.toString()));
	}
}
