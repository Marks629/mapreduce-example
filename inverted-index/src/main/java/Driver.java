import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {

	public static void main(String[] args) 
    throws ClassNotFoundException, IOException, 
    InterruptedException {
		//job1
		Configuration conf1 = new Configuration();
		
		Job job1 = Job.getInstance();
    job1.setJobName("Word-doc-freq");
		job1.setJarByClass(Driver.class);
		
		job1.setMapperClass(WordDocFreq.Map.class);
		job1.setReducerClass(WordDocFreq.Reduce.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.setInputPaths(job1, new Path(args[0]));
		TextOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);

		//2nd job
		Configuration conf2 = new Configuration();

		Job job2 = Job.getInstance(conf2);
		job2.setJobName("inverted index");
		job2.setJarByClass(Driver.class);

		job2.setMapperClass(InvertedIndex.Map.class);
		job2.setReducerClass(InvertedIndex.Reduce.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.setInputPaths(job2, new Path(args[1]));
		TextOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.waitForCompletion(true);
	}

}
