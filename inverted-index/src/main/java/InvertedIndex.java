
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class InvertedIndex {
	public static class Map 
    extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, 
      Context context) throws IOException, 
      InterruptedException {
			//i\tdoc1,1
      //love\tdoc2,3
      String[] parts = value.toString().split("\t");
      // (i, (doc1,1)), (love, (doc2,3)) ...
			context.write(new Text(parts[0]), new Text(parts[1]));
		}
	}

	public static class Reduce 
    extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, 
      Context context) 
      throws IOException, InterruptedException {
      StringBuilder sb = new StringBuilder();
		  for(Text value : values) {
        sb.append(value.toString() + "\t");
      }
      // (love, doc1,1 \t doc2,3)...
      context.write(key, new Text(sb.toString()));
		}
	}
}
