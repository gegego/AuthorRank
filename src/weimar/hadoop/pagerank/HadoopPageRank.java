package weimar.hadoop.pagerank;

import java.io.BufferedReader;
import java.io.BufferedWriter;
		
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
		
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import weimar.hadoop.pagerank.helper.AuthorCountingMap;
import weimar.hadoop.pagerank.helper.AuthorCountingReduce;
import weimar.hadoop.pagerank.helper.AuthorFilteringMap;
import weimar.hadoop.pagerank.helper.AuthorImportMap;
import weimar.hadoop.pagerank.helper.AuthorImportReduce;
import weimar.hadoop.pagerank.helper.CitedCountingMap;
import weimar.hadoop.pagerank.helper.CitedCountingReduce;
import weimar.hadoop.pagerank.helper.CitedNumberMap;
import weimar.hadoop.pagerank.helper.CleanupResultsMap;
import weimar.hadoop.pagerank.helper.CleanupResultsReduce;
import weimar.hadoop.pagerank.helper.CreateGraphMap;
import weimar.hadoop.pagerank.helper.CreateGraphReduce;
import weimar.hadoop.pagerank.helper.EqualMap;
import weimar.hadoop.pagerank.helper.FilterAuthorMap;
import weimar.hadoop.pagerank.helper.FilterAuthorReduce;
import weimar.hadoop.pagerank.helper.PaperAuthorCitedMap;
import weimar.hadoop.pagerank.helper.PaperAuthorLinkMap;
import weimar.hadoop.pagerank.helper.PaperAuthorLinkReduce;
import weimar.hadoop.pagerank.helper.PaperCountingReduce;
import weimar.hadoop.pagerank.helper.PaperNumberMap;
import weimar.hadoop.pagerank.helper.PaperReferenceMap;
import weimar.hadoop.pagerank.helper.TopAuthorNameReduce;

public class HadoopPageRank extends Configured implements Tool{
 
	  public static void main (String[] args) throws Exception{
		System.exit(ToolRunner.run(new HadoopPageRank(), args));
      }
	  
	  public int run(String[] args) throws Exception {

		System.out.println("*********************************************");
        System.out.println("*           Hadoop Author Rank              *");
        System.out.println("*********************************************");
		Configuration config = getConf();

		if (args.length != 1) {
			String errorReport = "Usage:: \n"
					+ "hadoop jar HadoopPageRank.jar "
					+ "[maximum loop count]\n";
			
			System.out.println(errorReport);
			return -1;
		}			

		String data_paperRef = "/corpora/corpus-microsoft-academic-graph/data/PaperReferences.tsv.bz2";
		String data_paperAuthor = "/corpora/corpus-microsoft-academic-graph/data/PaperAuthorAffiliations.tsv.bz2";
		String data_Author = "/corpora/corpus-microsoft-academic-graph/data/Authors.tsv.bz2";
				
		int noIterations = Integer.parseInt(args[0]);
		int outputIndex = 0;
		int numUrls = 0;
		long startTime = System.currentTimeMillis();
		FileSystem fs = FileSystem.get(config);

		
		/////////////////////////////////////////////////////////////
		// Preprocessing step and create weighted graph
		/////////////////////////////////////////////////////////////

		System.out.println("Iteration No:" + noIterations);		
		System.out.println("Hadoop Filtering starts...\n");
		/////////////////////////////////////////////////////////////
		//1. filtering the author with paper number is more than 250   
		/////////////////////////////////////////////////////////////
		Job job1 = new Job(config, "Filtering Author");
		
		job1.setJarByClass(HadoopPageRank.class);
		job1.setMapperClass(FilterAuthorMap.class);
		job1.setReducerClass(FilterAuthorReduce.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job1, new Path(data_paperAuthor));
		if (fs.exists(new Path(String.valueOf(outputIndex))))
			fs.delete(new Path(String.valueOf(outputIndex)), true);
		FileOutputFormat.setOutputPath(job1, new Path(String.valueOf(outputIndex)));
		
		int numReduceTasks = 1;
		job1.setNumReduceTasks(numReduceTasks);
		
		job1.waitForCompletion(true);
		if (!job1.isSuccessful()){
			System.out.println("Hadoop Filtering Author, exit...");
			return -1;
		}
		
		System.out.println("Hadoop Author Counting starts...\n");
		/////////////////////////////////////////////////////////////
		//2. Counting the total number of author 
		/////////////////////////////////////////////////////////////
		Job job2 = new Job(config, "Author Counting");
		
		job2.setJarByClass(HadoopPageRank.class);
		job2.setMapperClass(AuthorCountingMap.class);
		job2.setReducerClass(AuthorCountingReduce.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job2, new Path(String.valueOf(outputIndex)));
		if (fs.exists(new Path(String.valueOf(outputIndex+1))))
			fs.delete(new Path(String.valueOf(outputIndex+1)), true);
		FileOutputFormat.setOutputPath(job2, new Path(String.valueOf(outputIndex+1)));
		
		numReduceTasks = 1;
		job2.setNumReduceTasks(numReduceTasks);
		
		job2.waitForCompletion(true);
		if (!job2.isSuccessful()){
			System.out.println("Hadoop Author Counting failed, exit...");
			return -1;
		}
		
		Path pt=new Path(String.valueOf(outputIndex+1)+"/part-r-00000");
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        line=br.readLine();
        if (line != null){
        	numUrls = Integer.parseInt(line);
            System.out.println("Author Number:"+numUrls);
    		config.setInt("numUrls", numUrls);
        }
        else
        {
        	System.out.println("Hadoop Author Counting failed, exit...");
			return -1;
        }
		
		System.out.println("Hadoop Paper Author Filter starts...\n");
		/////////////////////////////////////////////////////////////
		//3. Filtering the the author paper relationship by the result of setp 1
		///////////////////////////////////////////////////////////// 
		Job job3 = new Job(config, "Paper Author Filter");
		
		job3.setJarByClass(HadoopPageRank.class);
		
		MultipleInputs.addInputPath(job3, new Path(data_paperAuthor),
                TextInputFormat.class, AuthorFilteringMap.class);
		MultipleInputs.addInputPath(job3, new Path(String.valueOf(outputIndex)),
                TextInputFormat.class, AuthorImportMap.class);

		job3.setReducerClass(AuthorImportReduce.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		
		if (fs.exists(new Path(String.valueOf(outputIndex+2))))
			fs.delete(new Path(String.valueOf(outputIndex+2)), true);
		FileOutputFormat.setOutputPath(job3, new Path(String.valueOf(outputIndex+2)));
		
		numReduceTasks = 1;
		job3.setNumReduceTasks(numReduceTasks);
		
		job3.waitForCompletion(true);
		if (!job3.isSuccessful()){
			System.out.println("Hadoop  Author Filter failed, exit...");
			return -1;
		}
		
		
		System.out.println("Hadoop Paper Author Link starts...\n");
		/////////////////////////////////////////////////////////////
		//4. join the author id to the paper reference relation with paper id
		///////////////////////////////////////////////////////////// 
		Job job4 = new Job(config, "Paper Author Link");
		
		job4.setJarByClass(HadoopPageRank.class);
		
		MultipleInputs.addInputPath(job4, new Path(String.valueOf(outputIndex+2)),
                TextInputFormat.class, PaperAuthorLinkMap.class);
		MultipleInputs.addInputPath(job4, new Path(data_paperRef),
                TextInputFormat.class, PaperReferenceMap.class);

		job4.setReducerClass(PaperAuthorLinkReduce.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		
		if (fs.exists(new Path(String.valueOf(outputIndex+3))))
			fs.delete(new Path(String.valueOf(outputIndex+3)), true);
		FileOutputFormat.setOutputPath(job4, new Path(String.valueOf(outputIndex+3)));
		
		numReduceTasks = 10;
		job4.setNumReduceTasks(numReduceTasks);
		
		job4.waitForCompletion(true);
		if (!job4.isSuccessful()){
			System.out.println("Hadoop Paper Author Link failed, exit...");
			return -1;
		}
		
		
		System.out.println("Hadoop Author Cited starts...\n");
		/////////////////////////////////////////////////////////////
		//5. join the author id to the paper reference relation with reference paper id
		/////////////////////////////////////////////////////////////
		Job job5 = new Job(config, "Author Cited");
		
		job5.setJarByClass(HadoopPageRank.class);
		
		MultipleInputs.addInputPath(job5, new Path(String.valueOf(outputIndex+3)),
                TextInputFormat.class, PaperAuthorCitedMap.class);
		MultipleInputs.addInputPath(job5, new Path(String.valueOf(outputIndex+2)),
                TextInputFormat.class, PaperAuthorLinkMap.class);

		job5.setReducerClass(PaperAuthorLinkReduce.class);
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(Text.class);
		
		if (fs.exists(new Path(String.valueOf(outputIndex+4))))
			fs.delete(new Path(String.valueOf(outputIndex+4)), true);
		FileOutputFormat.setOutputPath(job5, new Path(String.valueOf(outputIndex+4)));
		
		numReduceTasks = 10;
		job5.setNumReduceTasks(numReduceTasks);
		
		job5.waitForCompletion(true);
		if (!job5.isSuccessful()){
			System.out.println("Hadoop Author Cited failed, exit...");
			return -1;
		}

		System.out.println("Hadoop Cited Counting starts...\n");
		/////////////////////////////////////////////////////////////
		//6. calculate the cited number of authors
		/////////////////////////////////////////////////////////////

		Job jobData1 = new Job(config, "Cited Counting");

		jobData1.setJarByClass(HadoopPageRank.class);
		jobData1.setMapperClass(CitedCountingMap.class);
		jobData1.setReducerClass(CitedCountingReduce.class);
		jobData1.setOutputKeyClass(Text.class);
		jobData1.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(jobData1, new Path(String.valueOf(outputIndex+4)));
		if (fs.exists(new Path(String.valueOf(outputIndex+30))))
			fs.delete(new Path(String.valueOf(outputIndex+30)), true);
		FileOutputFormat.setOutputPath(jobData1, new Path(String.valueOf(outputIndex+30)));
		
		numReduceTasks = 10;
		jobData1.setNumReduceTasks(numReduceTasks);
		
		jobData1.waitForCompletion(true);
		if (!jobData1.isSuccessful()){
			System.out.println("Hadoop Cited Counting failed, exit...");
			return -1;
		}
		
		System.out.println("Hadoop Paper Counting starts...\n");
		/////////////////////////////////////////////////////////////
		//7. calculate the paper number of authors
		/////////////////////////////////////////////////////////////
		Job jobData2 = new Job(config, "Paper Counting");

		jobData2.setJarByClass(HadoopPageRank.class);
		jobData2.setMapperClass(FilterAuthorMap.class);
		jobData2.setReducerClass(PaperCountingReduce.class);
		jobData2.setOutputKeyClass(Text.class);
		jobData2.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(jobData2, new Path(String.valueOf(data_paperAuthor)));
		if (fs.exists(new Path(String.valueOf(outputIndex+31))))
			fs.delete(new Path(String.valueOf(outputIndex+31)), true);
		FileOutputFormat.setOutputPath(jobData2, new Path(String.valueOf(outputIndex+31)));
		
		numReduceTasks = 10;
		jobData2.setNumReduceTasks(numReduceTasks);
		
		jobData2.waitForCompletion(true);
		if (!jobData2.isSuccessful()){
			System.out.println("Hadoop Cited Counting failed, exit...");
			return -1;
		}
		
		System.out.println("Hadoop CreateGraph starts...\n");
		/////////////////////////////////////////////////////////////
		//8. Create the author reference Graph with weight
		///////////////////////////////////////////////////////////// 
		Job job6 = new Job(config, "CreateGraph");
		
		job6.setJarByClass(HadoopPageRank.class);
		job6.setMapperClass(CreateGraphMap.class);
		job6.setReducerClass(CreateGraphReduce.class);
		job6.setOutputKeyClass(Text.class);
		job6.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job6, new Path(String.valueOf(outputIndex+4)));
		if (fs.exists(new Path(String.valueOf(outputIndex+5))))
			fs.delete(new Path(String.valueOf(outputIndex+5)), true);
		FileOutputFormat.setOutputPath(job6, new Path(String.valueOf(outputIndex+5)));
		
		numReduceTasks = 10;
		job6.setNumReduceTasks(numReduceTasks);
		
		job6.waitForCompletion(true);
		if (!job6.isSuccessful()){
			System.out.println("Hadoop CreateGraph failed, exit...");
			return -1;
		}
		
		/////////////////////////////////////////////////////////////
		//pageRank step
		/////////////////////////////////////////////////////////////
	
		System.out.println("Hadoop PageRank starts...\n");
		/////////////////////////////////////////////////////////////
		//9. page rank for author weighted graph with iteration
		/////////////////////////////////////////////////////////////
		for (int i=0;i<noIterations;i++){
			System.out.println("Hadoop PageRank iteration "+ i +"...\n");
			Job job7 = new Job(config, "HadoopPageRank");
			job7.setJarByClass(HadoopPageRank.class);
			job7.setMapperClass(PageRankMap.class);
			job7.setReducerClass(PageRankReduce.class);
			job7.setOutputKeyClass(Text.class);
			job7.setOutputValueClass(Text.class);
			
			//the output in the current iteration will become input in next iteration.
			FileInputFormat.setInputPaths(job7, new Path(String.valueOf(outputIndex+5)));
			if (fs.exists(new Path(String.valueOf(outputIndex+6))))
				fs.delete(new Path(String.valueOf(outputIndex+6)), true);
			FileOutputFormat.setOutputPath(job7, new Path(String.valueOf(outputIndex+6)));
			
			numReduceTasks = 1;
			job7.setNumReduceTasks(numReduceTasks);
			
			job7.waitForCompletion(true);
			if (!job7.isSuccessful()){
				System.out.format("Hadoop PageRank iteration:{"+ i +"} failed, exit...", i);
				return -1;
			}
			//clean the intermediate data directories.
//			fs.delete(new Path(String.valueOf(outputIndex)), true);
			outputIndex++;
		}
		
		String outputDir = "/user/gofo3028/pagerank";
		
		System.out.println("Hadoop CleanUptResults starts...\n");
		/////////////////////////////////////////////////////////////
		//10. only write out author id and ranking
		/////////////////////////////////////////////////////////////
		Job job8 = new Job(config, "CleanUptResults");

		job8.setJarByClass(HadoopPageRank.class);
		job8.setMapperClass(CleanupResultsMap.class);
		job8.setReducerClass(CleanupResultsReduce.class);
		job8.setOutputKeyClass(Text.class);
		job8.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job8, new Path(String.valueOf(noIterations+5)));
		if (fs.exists(new Path(String.valueOf(outputDir))))
			fs.delete(new Path(String.valueOf(outputDir)), true);
		FileOutputFormat.setOutputPath(job8, new Path(String.valueOf(outputDir)));
		
		numReduceTasks = 1;
		job8.setNumReduceTasks(numReduceTasks);
		
		job8.waitForCompletion(true);
		if (!job8.isSuccessful()){
			System.out.println("Hadoop CleanUptResults failed, exit...");
			return -1;
		}				
				
		//--------------------------------------------------------------------------------------
		/////////////////////////////////////////////////////////////
		//11. get top K entry
		/////////////////////////////////////////////////////////////
        String topOutput = "/user/gofo3028/top";
		TopK.saveTopK(outputDir, topOutput, fs, 20);
		//--------------------------------------------------------------------------------------

        String finaloutput = "/user/gofo3028/final";
        outputIndex = 0;
		System.out.println("Hadoop Author Information Link starts...\n");
		/////////////////////////////////////////////////////////////
		//12. join the author information by author id
		///////////////////////////////////////////////////////////// 
		Job job9 = new Job(config, "Author Information Link");
		
		job9.setJarByClass(HadoopPageRank.class);
		
		MultipleInputs.addInputPath(job9, new Path(String.valueOf(data_Author)),
                TextInputFormat.class, EqualMap.class);
		MultipleInputs.addInputPath(job9, new Path(topOutput),
                TextInputFormat.class, AuthorImportMap.class);
		MultipleInputs.addInputPath(job9, new Path(String.valueOf(outputIndex+30)),
                TextInputFormat.class, CitedNumberMap.class);
		MultipleInputs.addInputPath(job9, new Path(String.valueOf(outputIndex+31)),
                TextInputFormat.class, PaperNumberMap.class);
		
		job9.setReducerClass(TopAuthorNameReduce.class);
		job9.setOutputKeyClass(Text.class);
		job9.setOutputValueClass(Text.class);
		
		if (fs.exists(new Path(String.valueOf(finaloutput))))
			fs.delete(new Path(String.valueOf(finaloutput)), true);
		FileOutputFormat.setOutputPath(job9, new Path(String.valueOf(finaloutput)));
		
		numReduceTasks = 1;
		job9.setNumReduceTasks(numReduceTasks);
		
		job9.waitForCompletion(true);
		if (!job9.isSuccessful()){
			System.out.println("Hadoop Author Name Link failed, exit...");
			return -1;
		}
		
		//--------------------------------------------------------------------------------------
		/////////////////////////////////////////////////////////////
		//13. sort the result of step 12 and output final result
		/////////////////////////////////////////////////////////////
        String finaloutput2 = "/user/gofo3028/final2";
		TopK.saveFinal(finaloutput, finaloutput2, fs);
		//--------------------------------------------------------------------------------------
		
		
		double executionTime = (System.currentTimeMillis() - startTime) / 1000.0;
		System.out.println("########################################################");
		System.out.println("# Hadoop Author Rank Job take " + executionTime + " sec.");
		System.out.println("########################################################");
		return 0;
	}
}
