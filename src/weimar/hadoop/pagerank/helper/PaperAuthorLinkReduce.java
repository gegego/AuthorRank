package weimar.hadoop.pagerank.helper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import weimar.hadoop.pagerank.HadoopPageRank;

public class PaperAuthorLinkReduce extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
	
		String Authorid = "";
		String valueList = "";
		for (Text val : values) {
			if(val.toString().startsWith("#"))
			{
				//authorid
				Authorid =  Authorid + val.toString();
			}
			else if(val.toString().startsWith("%"))
			{
				//paperid or authorid
				valueList = valueList + val.toString();
			}
	    }
		
//		if(!Authorid.isEmpty() && !valueList.isEmpty())
//			context.write(new Text(Authorid), new Text(valueList));
		
		String[] autlist = Authorid.split("#");
		for (String author : autlist)
		{
			if(!author.isEmpty() && !valueList.isEmpty())
			{
				String[] vallst = valueList.split("%");
				for (String val : vallst)
				{
					if(!val.isEmpty())
						context.write(new Text(author), new Text(val));
				}
			}				
		}
	}
}
