
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordDocFreq {
	public static class Map 
    extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value, 
      Context context) throws IOException, 
      InterruptedException {
      String docName = ((FileSplit)context.getInputSplit())
        .getPath().getName();
			//i love big data
			String[] words = value.toString().trim().toLowerCase()
        .replaceAll("[^a-z]", " ").split("\\s+");
			//((i,doc1), 1), ((love,doc1), 1), ((big,doc1), 1)...
      for(String word : words) {
			  context.write(new Text(word + ',' + docName), 
          new IntWritable(1));
      }
		}
	}

	public static class Reduce 
    extends Reducer<Text, IntWritable, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, 
      Context context) 
      throws IOException, InterruptedException {
			  //((i,doc1), 1), ((love,doc1), 1), ((big,doc1), 1)...
			  //((i,doc1), 2), ((big,doc1), 3) ...
      int count = 0;
      String[] parts = key.toString().split(",");
      String outputKey = parts[0],
             docName = parts[1];
		  for(IntWritable value : values) 
        count ++;
      // (i, (doc1,3)), (love, (doc1,1)), 
      // (big, (doc1,4)) ...
      context.write(new Text(outputKey), 
        new Text(docName + "," + count));
		}
	}
}
