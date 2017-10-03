package com.hadoopexample.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MovieRate {

	/**
	 * @param args
	 * @throws Exception 
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws Exception {
		Path job1InputPath = new Path("hdfs://172.17.0.2:9000/hduser/"),
                    job1OutputPath = new Path("hdfs://172.17.0.2:9000/hduser/1_output"),
                    job2OutputPath = new Path("hdfs://172.17.0.2:9000/hduser/2_output");
            Job job1 = Job.getInstance(), job2 = Job.getInstance();

            FileInputFormat.setInputPaths(job1, job1InputPath);
            FileOutputFormat.setOutputPath(job1, job1OutputPath);

            job1.setJarByClass(MovieRate.class);
            job1.setMapperClass(MovieRateMapper.class);
            job1.setReducerClass(MovieRateReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);
            job1.waitForCompletion(true);

            FileInputFormat.setInputPaths(job2, job1OutputPath);
            FileOutputFormat.setOutputPath(job2, job2OutputPath);

            job2.setMapperClass(MovieRateSortMapper.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
		job2.setSortComparatorClass(MyKeyComparator.class);
            job2.waitForCompletion(true);
	}
}

class MovieRateMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] parts = value.toString().trim().split("\\s+");
		context.write(new Text(parts[0]), new IntWritable(1));
	}
}

class MovieRateReducer extends Reducer<Text, IntWritable, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		for(IntWritable value : values) {
			count += value.get();
		}
		context.write(new Text(String.format("%05d", count)), key);
	}
}
/*
class MovieRateSortMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] parts = value.toString().trim().split("\\s+");
		context.write(new Text(parts[0]), new Text(parts[1]));
	}
}*/

class MyKeyComparator extends WritableComparator {
	protected MyKeyComparator() {
		super(Text.class, true);
	}
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        Text key1 = (Text) w1;
        Text key2 = (Text) w2;          
        return -1 * key1.compareTo(key2);
    }
}
