package com.george.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.george.hadoop.LetterCount.IntSumReducer;
import com.george.hadoop.LetterCount.TokenizerMapper;

public class Twitter {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		
		private Text hour = new Text();
		private Text day = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String lines[] = value.toString().split("\n");
			String data[];
			for (String line : lines) {
				data = line.split(" ");
				hour.set(data[2].split(":")[0]);
				day.set(String.valueOf(lines.length));
				context.write(hour, day);
			}
		}
		
	}
	
	public static class TwitterReducer extends Reducer<Text, Text, Text, Text> {
		
		private Text outputText = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String currDay = "";
			int days = 0;
			int sum = 0;
			for (Text value : values) {
				if (!currDay.equals(value.toString())) {
					currDay = value.toString();
					days = days + 1;
				}
				System.out.println(value.toString());
				days = Integer.parseInt("3");
				sum = sum + 1;
			}
			outputText.set((String.valueOf(sum / (float)days)));
			context.write(key, outputText);
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: twitter <in> [<in>...] <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "twitter");
	    job.setJarByClass(Twitter.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(TwitterReducer.class);
	    job.setReducerClass(TwitterReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    for (int i = 0; i < otherArgs.length - 1; ++i) {
	        FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
	    }
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	

}
