package com.george.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

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

import com.george.hadoop.SearchAnalytics.IntSumReducer;
import com.george.hadoop.SearchAnalytics.TokenizerMapper;
import com.sun.tools.corba.se.idl.toJavaPortable.Util;
import com.sun.tools.javac.code.Attribute.Array;

public class PopularPages {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private Text word = new Text();
		private Text id = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\n"); // split text into lines
			for (String line : lines) { // for each line
				String[] tokens = line.split("\t"); // split each line into words, use tab as delimiter
				String userID = tokens[0];
				String url = null;
				if (tokens.length == 5) { // if a link has been followed
					url = tokens[4];
					word.set(url);
					id.set(userID);
					context.write(word, id);
				}
				
			}
		}
		
	}
	
	public static class IntSumReducer extends Reducer<Text, Text, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			ArrayList<String> list = new ArrayList<String>();
			for (Text val : values) {
				list.add(val.toString());
			}
			Collections.sort(list);
			String previous = "";
			for (String val : list) {
				if (!val.equals(previous)) {
					sum++;
					previous = val;
				}
			}
			if (sum >= 15) {
				result.set(sum);
				context.write(key, result);
			}
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: popularpages <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "popular pages");
	    job.setJarByClass(PopularPages.class);
	    job.setMapperClass(TokenizerMapper.class);
	    //job.setCombinerClass(IntSumReducer.class);
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
