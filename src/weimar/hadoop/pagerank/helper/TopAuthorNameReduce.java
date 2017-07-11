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

public class TopAuthorNameReduce extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		Boolean authorimport = false;
		String AuthorName = "";
		String rank ="";
		String PaperNumber = "";
		String CitedNumber ="";
		
		for (Text val : values) {
			if(val.toString().startsWith("@"))
			{
				//authorid
				authorimport = true;
				rank = val.toString().substring(1);
			}
			else if(val.toString().startsWith("C"))
			{
				CitedNumber = val.toString().substring(1);
			}
			else if(val.toString().startsWith("P"))
			{
				PaperNumber = val.toString().substring(1);
			}
			else
			{
				AuthorName += val.toString();
			}
	    }
		if(authorimport == true)
		{
			context.write(key, new Text(AuthorName+ "," + PaperNumber + ","+ CitedNumber + ","+rank));
		}
		
	}
}
