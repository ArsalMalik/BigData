   import java.io.IOException;
   import java.io.PrintStream;
   import java.util.Iterator;
   import org.apache.hadoop.conf.Configuration;
   import org.apache.hadoop.fs.Path;
   import org.apache.hadoop.io.LongWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Job;
   import org.apache.hadoop.mapreduce.Mapper;
   import org.apache.hadoop.mapreduce.Mapper.Context;
   import org.apache.hadoop.mapreduce.Reducer;
   import org.apache.hadoop.mapreduce.Reducer.Context;
   import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
   import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
   import org.apache.hadoop.util.GenericOptionsParser;
   
   public class MeanVariance
   {

   	 public static class MeanMap extends Mapper<LongWritable, Text, Text, Text>
     {
       public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
         throws IOException, InterruptedException
       {
         context.write(new Text("Mapper"), value);
       }
     }

     public static class MeanCombiner extends Reducer<Text, Text, Text, Text>
     {
       public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Context context)
         throws IOException, InterruptedException
       {
         Integer count = 0;
         Double sum = 0.0D;
         Double SquaredValue = 0.0D;
   
         Iterator itr = values.iterator();
         while (itr.hasNext()) {
           String text = ((Text)itr.next()).toString();
           Double value = Double.parseDouble(text);

           sum += value;
           SquaredValue = SquaredValue + Math.pow(value, 2.0D));
         }
         Double mean = sum / count;
   
         context.write(new Text("Mean    count   SquaredValue"), new Text(mean + "/t" + count + "/t" + SquaredValue));
       }
     }
     
     public static class MeanReducer extends Reducer<Text, Text, Text, Text>
     {
       public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
         throws IOException, InterruptedException
       {
         Double sum = 0.0D;
         Double SquaredValue = 0.0D;
         Integer totalCount = 0;
   
         Iterator itr = values.iterator();
         while (itr.hasNext()) {
           String text = ((Text)itr.next()).toString();
           String[] token = text.split("/t");

           Double mean = Double.parseDouble(token[0]);
           Integer count = Integer.parseInt(token[1]);
           Double SquareValue = Double.parseDouble(token[2]);
           sum += mean * count;
           totalCount = totalCount + count;
           SquaredValue = SquaredValue + SquareValue;
         }
         Double mean = sum / totalCount;
         Double expectedSquaredValue = SquaredValue / totalCount;
         Double SquaredMean = Math.pow(mean, 2.0D);
  
         Double variance = expectedSquaredValue - SquaredMean;
   
         context.write(new Text("Mean = " + mean.toString()), new Text("\tVariance = " + variance.toString()));
       }
     }
   
   public static void main(String[] args) throws Exception {
     Configuration conf = new Configuration();
     String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
   
     if (otherArgs.length != 2) {
         System.err.println("Usage: <inputFile> <outputFile>");
         System.exit(2);
       }
   
       Job job = Job.getInstance(conf, "MeanVariance");
       job.setJarByClass(MeanVariance.class);
       job.setMapperClass(MeanVariance.MeanMap.class);
       job.setCombinerClass(MeanVariance.MeanCombiner.class);
       job.setReducerClass(MeanVariance.MeanReducer.class);
   
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(Text.class);
   
       FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
       FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
   
       System.exit(job.waitForCompletion(true) ? 0 : 1);
     }
     
   }
