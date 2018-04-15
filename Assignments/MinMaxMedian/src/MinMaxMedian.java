import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
 public class MinMaxMedian {
     
       public static class MinMap extends Mapper<LongWritable, Text, Text, MMM>
       {
         private Integer Minimum;
         private Integer Maximum;
         private Double Median;
	     private MMM result = new MMM();
     
         public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Context context) throws IOException, InterruptedException {
         	String number = value.toString();
     
          	Minimum = Integer.parseInt(number));
         	Maximum = Integer.parseInt(number));
         	Median = Double.parseDouble(number));
     
         	result.setMinimum(Minimum);
         	result.setMaximum(Maximum);
         	result.setMedian(Median);
   
         context.write(new Text("Minimum/Maximum/Median"), result);
    }
 }

 public static class MinReduce extends Reducer<Text, MMM, NullWritable, Text> {
 	      private MMM result = new MMM();
    	  List<Double> listNumbers = new ArrayList();
     
          public void reduce(Text key, Iterable<MMM> values, Reducer<Text, MMM,Context context)
           throws IOException, InterruptedException
          {
        	  result.setMinimum(null);
        	  result.setMaximum(null);
    	      result.setMedian(null);
     
         for (MMM val : values) {
           Integer Minimum = val.getMinimum();
           Integer Maximum = val.getMaximum();
           Double Median = val.getMedian();
     
           listNumbers.add(Median);
     
           if ((result.getMinimum() == null) || (Minimum.compareTo(result.getMinimum()) < 0)) {
             	result.setMinimum(Minimum);
             }
           if ((result.getMaximum() == null) || (Maximum.compareTo(result.getMaximum()) > 0)) {
             	result.setMaximum(Maximum);
             }
           }
     
         Collections.sort(listNumbers);
         int size = listNumbers.size();
     
         if (size % 2 == 0) {
           int half = size / 2;
     
           result.setMedian(((listNumbers.get(half - 1)) + (listNumbers.get(half)))/ 2.0D);
           } else {
           int half = (size + 1) / 2;
           this.result.setMedian(listNumbers.get(half - 1));
           }
         context.write(NullWritable.get(), new Text(result.toString()));
         }
       }


   public static void main(String[] args) throws Exception {
     Configuration conf = new Configuration();
     String[] programArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
     
     if (programArgs.length != 2) {
       System.err.println("Usage: <inputFile> <outputFile>");
       System.exit(2);
     }
 
       Job job = Job.getInstance(conf);
       job.setJobName("MinMaxMedian");
       job.setJarByClass(MinMaxMedian.class);
       job.setMapperClass(MinMaxMedian.MinMap.class);
       job.setReducerClass(MinMaxMedian.MinReduce.class);
     
       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(MMM.class);
     
       job.setOutputKeyClass(NullWritable.class);
       job.setOutputValueClass(Text.class);
     
       FileInputFormat.addInputPath(job, new Path(programArgs[0]));
       FileOutputFormat.setOutputPath(job, new Path(programArgs[1]));
     
       System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
 }

