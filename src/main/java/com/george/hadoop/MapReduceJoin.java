package com.george.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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

import com.george.hadoop.PerDaySearches.IntSumReducer;
import com.george.hadoop.PerDaySearches.TokenizerMapper;

public class MapReduceJoin {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private Text keyword = new Text();
		private Text value = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\n"); // split text into lines
			for (String line : lines) { // for each line
				String[] tokens = line.split("\t"); // split each line into words, use tab as delimiter
				String userID = tokens[0];
				String search = tokens[1];
				String[] terms = search.split(" "); // split search into its terms
				for (String term : terms) {
					keyword.set(term);
					value.set(line);
					context.write(keyword, value);
				}
			}
		}
		
	}
	
	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (Text val : values) {
				String[] tokens = val.toString().split("\t");
				if (tokens.length == 1) {
					return;
				}
				else {
					result.set(val);
				}
			}
			context.write(key, result);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: mapreducejoin <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "map reduce join");
	    job.setJarByClass(MapReduceJoin.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    for (int i = 0; i < otherArgs.length - 1; ++i) {
	        FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
	    }
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	
}
