package com.george.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class PopularKeywords {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\n"); // split text into lines
			for (String line : lines) { // for each line
				line = line.toLowerCase();
				String[] tokens = line.split("\t"); // split each line into words, use tab as delimiter
				String search = tokens[1];
				String[] terms = search.split(" "); // split search terms 
				for (String term : terms) {
					word.set(term);
					context.write(word, one);
				}
			}
		}
	}
	
	public static class StopwordsTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable zero = new IntWritable(0);
		private Text keyword = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\n"); // split text into lines
			for (String line : lines) { // for each line
				line = line.toLowerCase();
				keyword.set(line);
				context.write(keyword, zero);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				if (val.get() == 0) { // if term is included in stopwords, ignore
					return;
				}
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	
	public static class TokenizerMapper_2 extends Mapper<Object, Text, LongWritable, Text> {
		private LongWritable sum = new LongWritable();
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\n"); // split text into lines
			for (String line : lines) { // for each line
				String[] tokens = line.split("\t"); // split each line into words, use tab as delimiter
				String keyword = tokens[0];
				int num = Integer.parseInt(tokens[1]);
				sum.set(num);
				word.set(keyword);
				context.write(sum, word);
				
			}
		}
		
	}
	
	public static class IntSumReducer_2 extends Reducer<LongWritable, Text, Text, LongWritable> {
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(val, key);
			}
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: popularkeywords <in> <stopwords> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "popular keywords");
	    job.setJarByClass(PopularKeywords.class);
	    job.setMapperClass(TokenizerMapper.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, TokenizerMapper.class);
	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, StopwordsTokenizerMapper.class);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
	    int returnCode = job.waitForCompletion(true) ? 0 : 1;
	    // second job
	    Job job_2 = Job.getInstance();
	    job_2.setJarByClass(PopularKeywords.class);
	    job_2.setMapperClass(TokenizerMapper_2.class);
	    //job_2.setCombinerClass(IntSumReducer_2.class);
	    job_2.setReducerClass(IntSumReducer_2.class);
	    job_2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
	    job_2.setOutputKeyClass(Text.class);
	    job_2.setOutputValueClass(LongWritable.class);
	    
	    job_2.setMapOutputKeyClass(LongWritable.class);
	    job_2.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job_2, new Path(otherArgs[2]));
	    FileOutputFormat.setOutputPath(job_2, new Path(otherArgs[otherArgs.length - 1], "second_job"));
	    System.out.println(new Path(otherArgs[1], "part-r-00000").toString());
	    for (Path p : FileInputFormat.getInputPaths(job_2)) {
	    	System.out.println(p.toString());
	    }
	    returnCode = job_2.waitForCompletion(true) ? 0 : 1;
	    System.exit(returnCode);
	}

}
