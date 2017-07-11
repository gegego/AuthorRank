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

public class AuthorImportReduce extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		
		Boolean authorimport = false;
		String valueList = "";
		
		for (Text val : values) {
			if(val.toString().startsWith("@"))
			{
				//authorid
				authorimport = true;
			}
			else if(val.toString().startsWith("%"))
			{
				valueList += val.toString();
			}
	    }
		
		if(authorimport == true && !valueList.isEmpty())
		{
			String[] strArray = valueList.split("%");
			for(String strPaper : strArray)
			{
				if(!strPaper.isEmpty())
					context.write(new Text(strPaper), key);
			}
		}
	}
}
