package weimar.hadoop.pagerank.helper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
		





import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
		
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class AuthorCountingMap extends Mapper<LongWritable, Text, NullWritable, LongWritable>{
    private final static LongWritable one = new LongWritable(1);

	public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

		String[] str = value.toString().split("\t");
		int temp = Integer.parseInt(str[1]);
		
		//System.out.println(temp + ":" +value.toString());
		if(temp == 0)
			context.write(NullWritable.get(), one);
	}
}
