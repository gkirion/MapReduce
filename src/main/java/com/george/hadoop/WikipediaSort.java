package com.george.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;

import com.george.hadoop.WikipediaHistogramFull.IntSumReducer;
import com.george.hadoop.WikipediaHistogramFull.TokenizerMapper;
import com.google.common.base.Charsets;

public class WikipediaSort {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\n"); // split text into lines
			for (String line : lines) { // for each line
				String[] tokens = line.split("_"); // split each line into words, use _ as delimiter
				for (String token : tokens) {
					token = token.toLowerCase();
					word.set(token);
					context.write(word, one);
				}
			}
		}
	}
	
	public static class KeyPartitioner extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numReducers) {
			String token = key.toString();
			char firstChar = token.charAt(0);
			if (firstChar >= '0' && firstChar <= '9') {
				return 0; 
			}
			else if (firstChar >= 'a' && firstChar <= 'z') {
				return (firstChar - 'a' + 2) / 3;
			}
			else { // special character
				return 0;
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
	
	public static class WordMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text out = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\n"); // split text into lines
			for (String line : lines) { // for each line
				String[] tokens = line.split("_"); // split each line into words, use _ as delimiter
				for (String token : tokens) {
					token = token.toLowerCase();
					word.set(token);
					out.set("");
					context.write(word, out);
				}
			}
		}
	}
	
	public static class WordReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			result.set("");
			context.write(key, result);
		}
	}
	
	
	public static void main(String[] args) throws Exception {
	    if (args.length < 2) {
	      System.err.println("Usage: wikipediasort <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    
	    Job tokenizerJob = Job.getInstance(new Configuration(), "wikipedia sort title tokenizer");
	    tokenizerJob.setJarByClass(WikipediaSort.class);
	    TextInputFormat.setInputPaths(tokenizerJob, new Path(args[0]));
	    tokenizerJob.setMapperClass(WordMapper.class);
	    tokenizerJob.setNumReduceTasks(0);
	    tokenizerJob.setOutputKeyClass(Text.class);
	    tokenizerJob.setOutputValueClass(Text.class);
	    tokenizerJob.setOutputFormatClass(SequenceFileOutputFormat.class);
	    SequenceFileOutputFormat.setOutputPath(tokenizerJob, new Path(args[1] + "-map"));
	    int ret = tokenizerJob.waitForCompletion(true) ? 0 : 1;
	    
	    Job sortJob = Job.getInstance(new Configuration(), "wikipedia sort");
	    sortJob.setJarByClass(WikipediaSort.class);
	    sortJob.setNumReduceTasks(10);
	    sortJob.setInputFormatClass(SequenceFileInputFormat.class);
	    SequenceFileInputFormat.setInputPaths(sortJob, new Path(args[1] + "-map"));
	    TextOutputFormat.setOutputPath(sortJob, new Path(args[1]));
	    //sortJob.setMapperClass(Mapper.class);
	    sortJob.setReducerClass(WordReducer.class);
	    sortJob.setMapOutputKeyClass(Text.class);
	    sortJob.setMapOutputValueClass(Text.class);
	    TotalOrderPartitioner.setPartitionFile(sortJob.getConfiguration(), new Path(args[1] + "-part.lst"));
	    InputSampler.Sampler sampler = new InputSampler.RandomSampler(1, 10, 9);
	    InputSampler.writePartitionFile(sortJob, sampler);
	    sortJob.setPartitionerClass(TotalOrderPartitioner.class);
	    ret = sortJob.waitForCompletion(true) ? 0 : 1;
	    
	    System.exit(ret);
	}
	
}
