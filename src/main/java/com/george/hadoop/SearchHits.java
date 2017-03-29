package com.george.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.george.hadoop.SearchAnalytics.IntSumReducer;
import com.george.hadoop.SearchAnalytics.TokenizerMapper;
import com.google.common.base.Charsets;

public class SearchHits {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\n"); // split text into lines
			for (String line : lines) { // for each line
				String[] tokens = line.split("\t"); // split each line into words, use tab as delimiter
				String userID = tokens[0];
				String search = tokens[1];
				String datetime = tokens[2];
				String date = datetime.split(" ")[0];
				String time = datetime.split(" ")[1];
				String rank = null;
				String url = null;
				if (tokens.length == 5) {
					rank = tokens[3];
					url = tokens[4];
				}
				if (tokens.length == 5) {
					word.set("hit");
					context.write(word, one);
				}
				else {
					word.set("miss");
					context.write(word, one);
				}
				word.set("total");
				context.write(word, one);
				
			}
		}
		
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
		
	}
	
	private void computeMean(Path path, Configuration conf)  throws Exception {
		FileSystem fs = FileSystem.get(conf);
		Path file = new Path(path, "part-r-00000");
		if (!fs.exists(file)) {
			throw new IOException("Output not found!");
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));
		String line;
		int hits = 0, misses = 0, tots = 0;
		line = br.readLine();
		while (line != null) { // while line is not empty
			String[] tokens = line.split("\t");
			if (tokens[0].equals("hit")) {
				hits = Integer.parseInt(tokens[1]);
			}
			else if (tokens[0].equals("miss")) {
				misses = Integer.parseInt(tokens[1]);
			}
			else {
				tots = Integer.parseInt(tokens[1]);
			}
			line = br.readLine();
		}
		System.out.printf("hit:\t%f\n", (double)hits / tots);
		System.out.printf("miss:\t%f\n", (double)misses / tots);
	}
	
	public static void main(String[] args) throws Exception {
		SearchHits searchHits = new SearchHits();
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: searchhits <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "search hits");
	    job.setJarByClass(SearchHits.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    for (int i = 0; i < otherArgs.length - 1; ++i) {
	        FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
	    }
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
	    int exitCode = job.waitForCompletion(true) ? 0 : 1;
	    searchHits.computeMean(new Path(otherArgs[1]), conf);
	    System.exit(exitCode);
	}
	
}
