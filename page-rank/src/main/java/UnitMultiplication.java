import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {
    static private Logger logger = Logger.getLogger(UnitMultiplication.class);

    public static class TransitionMapper 
        extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, 
            Context context) throws IOException, 
            InterruptedException {

            //input: fromPage\t toPage1,toPage2 ...
            //output: fromPage \t toPage1 
            String[] parts = value.toString().trim().split("\t");
            if(parts.length != 2) return;

            String[] outputValues = parts[1].split(",");
            for(String outputValue : outputValues) {
                context.write(new Text(parts[0]), new Text(outputValue));
            }
        }
    }

    public static class PRMapper
        extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, 
          Context context) throws IOException, 
          InterruptedException {
          //input: Page \t 1.0
          //output: Page \t PR=1.0
          String[] parts = value.toString().trim().split("\t");
        
          context.write(new Text(parts[0]), 
              new Text("PR=" + parts[1]));
        }
    }

    public static class MultiplicationReducer 
        extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, 
            Context context) throws IOException, 
            InterruptedException {

            //input: <fromPage, toPage>, <fromPage, pageRank>
            //target: get the unit multiplication
            List<String> outEdges = new ArrayList<String>();
            double pageRank = 0;
            for(Text value : values) {
                if(value.toString().startsWith("PR")) {
                    // it is the page rank
                    pageRank = Double.valueOf(
                        value.toString().split("=")[1]);
                } else {
                    // it is an out edge
                    outEdges.add(value.toString());
                }
            }
            Text contrib = new Text(String.valueOf(
                pageRank * 1.0 / outEdges.size()));

            for(String edge : outEdges) {
                context.write(new Text(edge), contrib);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), 
            TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), 
            TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, 
            new Path(args[2]));
        job.waitForCompletion(true);
    }

}
