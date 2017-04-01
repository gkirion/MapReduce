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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.base.Charsets;

public class WikipediaHistogramFull {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\n"); // split text into lines
			for (String line : lines) { // for each line
				String[] tokens = line.split("_"); // split each line into words, use _ as delimiter
				for (String token : tokens) {
					token = token.toUpperCase();
					word.set(token);
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
				line = line.toUpperCase();
				keyword.set(line);
				context.write(keyword, zero);
			}
		}
	}
	
	public static class StopwordsEliminatorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				if (val.get() == 0) { // if key is included in stopwords
					return;
				}
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static class ClassifierMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\n"); // split text into lines
			for (String line : lines) { // for each line
				String[] tokens = line.split("\t"); // split each line into words, use \t as delimiter
				for (String token : tokens) {
					char firstChar = token.charAt(0);
					if (firstChar >= '0' && firstChar <= '9') {
						word.set("number");
					}
					else if (firstChar >= 'A' && firstChar <= 'Z') {
						word.set(token.substring(0, 1));
					}
					else {
						word.set("special_character");
					}
					context.write(word, one);
				}
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
	
	public static void computePercentage(Path path, Configuration conf) {
		try {
			FileSystem fileSystem = FileSystem.get(conf);
			Path inputPath = new Path(path, "part-r-00000");
			if (!fileSystem.exists(inputPath)) {
				throw new IOException("Output not found!");
			}
			BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(inputPath), Charsets.UTF_8));
			StringBuffer sb = new StringBuffer();
			String line = br.readLine();
			double total = 0;
			while (line != null) {
				sb.append(line + "\n");
				String[] tokens = line.split("\t");
				total = total + Integer.parseInt(tokens[1]);
				line = br.readLine();
			}
			Path outputPath = new Path(path, "part-r-00001");
			PrintWriter printWriter = new PrintWriter(fileSystem.create(outputPath));
			br.close();
			br = new BufferedReader(new StringReader(sb.toString()));
			line = br.readLine();
			while (line != null) {
				String[] tokens = line.split("\t");
				int value = Integer.parseInt(tokens[1]);
				printWriter.println(tokens[0] + "\t" + Math.round((value / total) * 100) + "%");
				line = br.readLine();
			}
			br.close();
			printWriter.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
	    if (args.length < 3) {
		      System.err.println("Usage: wikipediahistogramfull <in> <stopwords file> <out>");
		      System.exit(2);
		}
	    Job job = Job.getInstance(new Configuration(), "wikipedia histogram full");
	    job.setJarByClass(WikipediaHistogramFull.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(StopwordsEliminatorReducer.class);
	    job.setReducerClass(StopwordsEliminatorReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, StopwordsTokenizerMapper.class);
	    FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
	    int ret = job.waitForCompletion(true) ? 0 : 1;
	    
	    Job classifierJob = Job.getInstance(new Configuration(), "wikipedia word classifier");
	    classifierJob.setJarByClass(WikipediaHistogramFull.class);
	    classifierJob.setMapperClass(ClassifierMapper.class);
	    classifierJob.setCombinerClass(IntSumReducer.class);
	    classifierJob.setReducerClass(IntSumReducer.class);
	    classifierJob.setOutputKeyClass(Text.class);
	    classifierJob.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(classifierJob, new Path(args[args.length - 1]));
	    FileOutputFormat.setOutputPath(classifierJob, new Path(args[args.length - 1], "second_job"));
	    ret = classifierJob.waitForCompletion(true) ? 0 : 1;
	    
	    computePercentage(new Path(args[args.length - 1], "second_job"), classifierJob.getConfiguration());
	    System.exit(ret);
	}

}
