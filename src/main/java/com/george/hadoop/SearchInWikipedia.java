package com.george.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.base.Charsets;


public class SearchInWikipedia {

	public static class SearchMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		//private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\n"); // split text into lines
			for (String line : lines) { // for each line
				line = line.toLowerCase();
				String[] tokens = line.split("\t"); // split each line into words, use tab as delimiter
				String search = tokens[1];
				String[] terms = search.split(" "); // split search terms 
				for (String term : terms) {
					word.set(term);
					context.write(word, key);
				}
				word.set("TOTAL");
				context.write(word, key);
			}
		}
	}
	
	public static class WikipediaMapper extends Mapper<Object, Text, Text, LongWritable> {
		private final static LongWritable out = new LongWritable(-1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\n"); // split text into lines
			for (String line : lines) { // for each line
				String[] tokens = line.split("_"); // split each line into words, use _ as delimiter
				for (String token : tokens) {
					token = token.toLowerCase();
					word.set(token);
					context.write(word, out);
				}
			}
		}
	}
	
	public static class StopwordsTokenizerMapper extends Mapper<Object, Text, Text, LongWritable> {
		private final static LongWritable out = new LongWritable(-2);
		private Text keyword = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\n"); // split text into lines
			for (String line : lines) { // for each line
				keyword.set(line);
				context.write(keyword, out);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private Text word = new Text();
		private LongWritable out = new LongWritable();
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			ArrayList<Long> list = new ArrayList<Long>();
			boolean wikipediaHasIt = false;
			for (LongWritable val : values) {
				if (val.get() == -2) { // if is include in stopwords, ignore it
					return;
				}
				else if (val.get() == -1) { // if exists in wikipedia
					wikipediaHasIt = true;
				}
				else { // key is included in a search
					list.add(val.get());
				}
			}
			if (wikipediaHasIt) {
				word.set("hit");
				for (Long val : list) { 
					out.set(val);
					context.write(word, out);
				}	
			}
			else if (key.toString().equals("TOTAL")) {
				for (Long val : list) { 
					out.set(val);
					context.write(key, out);
				}	
			}
			
		}
	}
	
	public static class WordMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text word = new Text();
		private LongWritable searchId = new LongWritable();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] lines = value.toString().split("\n"); // split text into lines
			for (String line : lines) { // for each line
				String[] tokens = line.split("\t"); // split each line into words, use tab as delimiter
				word.set(tokens[0]);
				searchId.set(Long.parseLong(tokens[1]));
				context.write(word, searchId);
			}
		}
	}
	
	public static class IntSumReducer_2 extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			ArrayList<Long> list = new ArrayList<Long>();
			for (LongWritable val : values) {
				list.add(val.get());
			}
			Collections.sort(list);
			Long previous = new Long(-1);
			int sum = 0;
			for (Long val : list) {
				if (val.longValue() != previous.longValue()) {
					sum++;
					previous = val;
				}
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
			String line = br.readLine();
			int miss = 0, hit = 0, total = 0;
			while (line != null) {
				String[] tokens = line.split("\t");
				if (tokens[0].equals("TOTAL")) {
					total = Integer.parseInt(tokens[1]);
				}
				else {
					hit = Integer.parseInt(tokens[1]);
				}
				line = br.readLine();
			}
			miss = total - hit;
			br.close();
			Path outputPath = new Path(path, "part-r-00001");
			PrintWriter printWriter = new PrintWriter(fileSystem.create(outputPath));
			printWriter.println("hit" + "\t" + hit + "\t" + Math.round((hit / (double)total) * 100) + "%");
			printWriter.println("miss" + "\t" + miss + "\t" + Math.round((miss / (double)total) * 100) + "%");
			printWriter.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length < 2) {
	      System.err.println("Usage: searchinwikipedia <search file> <wikipedia file> <out>");
	      System.exit(2);
	    }
	    
	    Job job = new Job(conf, "search in wikipedia");
	    job.setJarByClass(SearchInWikipedia.class);
	    job.setMapperClass(SearchMapper.class);
	    //job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, SearchMapper.class);
	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, WikipediaMapper.class);
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class, StopwordsTokenizerMapper.class);
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
	    int returnCode = job.waitForCompletion(true) ? 0 : 1;
	    
	    // second job
	    Job job_2 = Job.getInstance(new Configuration());
	    job_2.setJarByClass(SearchInWikipedia.class);
	    job_2.setMapperClass(WordMapper.class);
	    //job_2.setCombinerClass(IntSumReducer_2.class);
	    job_2.setReducerClass(IntSumReducer_2.class);	    
	    job_2.setOutputKeyClass(Text.class);
	    job_2.setOutputValueClass(LongWritable.class);
	    job_2.setMapOutputKeyClass(Text.class);
	    job_2.setMapOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job_2, new Path(otherArgs[otherArgs.length - 1]));
	    FileOutputFormat.setOutputPath(job_2, new Path(otherArgs[otherArgs.length - 1], "second_job"));
	    System.out.println(new Path(otherArgs[1], "part-r-00000").toString());
	    for (Path p : FileInputFormat.getInputPaths(job_2)) {
	    	System.out.println(p.toString());
	    }
	    returnCode = job_2.waitForCompletion(true) ? 0 : 1;
	    computePercentage(new Path(otherArgs[otherArgs.length - 1], "second_job"), job_2.getConfiguration());
	    System.exit(returnCode);
	}
	
}
