import java.io.IOException;
import java.io.PrintStream;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

public class MatrixMultiplication
{
  public static void main(String[] args)
    throws Exception
  {
    Configuration conf = new Configuration();
    String[] programArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (programArgs.length != 3) {
      System.err.println("Usage: <inputFile> <outputIFile> <outputFFile>");
      System.exit(2);
    }
    Path inputFile = new Path(args[0]);
    Path outputFile = new Path(args[1]);
    Path finalOutputFile = new Path(args[2]);

    Job job = Job.getInstance(conf);
    job.setJarByClass(MatrixMultiplication.class);
    job.setJobName("Matrix Multiplication");

    job.setMapperClass(MatrixMultiplication.matMultMap.class);
    job.setReducerClass(MatrixMultiplication.matMultReduce.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, inputFile);
    FileOutputFormat.setOutputPath(job, outputFile);

    boolean success = job.waitForCompletion(true);

    if (success) {
      Configuration conf2 = new Configuration();

      Job job2 = Job.getInstance(conf2);
      job2.setJarByClass(MatrixMultiplication.class);

      job2.setMapperClass(MatrixMultiplication.aggregateMap.class);
      job2.setReducerClass(MatrixMultiplication.aggregateReduce.class);

      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(FloatWritable.class);

      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(FloatWritable.class);

      FileInputFormat.addInputPath(job2, outputFile);
      FileOutputFormat.setOutputPath(job2, finalOutputFile);

      System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
  }

  public static class aggregateReduce extends Reducer<Text, FloatWritable, Text, FloatWritable>
  {
    private FloatWritable ans = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
      float sum = 0.0F;
      for (FloatWritable val : values) {
        sum += val.get();
      }
      ans.set(sum);
      context.write(key, ans);
    }
  }

  public static class aggregateMap extends Mapper<LongWritable, Text, Text, FloatWritable>
  {
    private FloatWritable ans = new FloatWritable(0.0F);

    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Context context) throws IOException, InterruptedException {
      StringTokenizer str = new StringTokenizer(value.toString(), "\n");

      while (str.hasMoreTokens()) {
        String line = str.nextToken();
        String[] tokens = line.split(",");

        if (tokens.length == 3) {
          String i = tokens[0];
          String k = tokens[1];
          String v = tokens[2];
          ans.set(Float.parseFloat(v));

          context.write(new Text(i + "," + k), ans);
        }
      }
    }
  }

  public static class matMultReduce extends Reducer<Text, Text, NullWritable, Text>
  {
    public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Context context)
      throws IOException, InterruptedException
    {
      ArrayList listA = new ArrayList();
      ArrayList listB = new ArrayList();

      for (Text val : values) {
        String[] value = val.toString().split(",");
        if (value[0].equals("A"))
          listA.add(new AbstractMap.SimpleEntry(Integer.parseInt(value[1]), Float.parseFloat(value[2])));
        else {
          listB.add(new AbstractMap.SimpleEntry(Integer.parseInt(value[1])), Float.parseFloat(value[2]))));
        }

      }

      String i, k;
      float aij, bjk;

      Text outputV = new Text();
      for (Map.Entry a : listA) {
        i = Integer.toString(a.getKey());
        aij = a.getValue();
        for (Map.Entry b : listB) {
          k = Integer.toString(b.getKey());
          bjk = b.getValue();
          Float result = aij * bjk;
          if (result != 0.0F) {
            outputV.set(i + "," + k + "," + result.toString());
            context.write(NullWritable.get(), outputV);
          }
        }
      }

    }
  }

  public static class matMultMap extends Mapper<LongWritable, Text, Text, Text>
  {
    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Context context)
      throws IOException, InterruptedException
    {
      Text outputK = new Text();
      Text outputV = new Text();

      String line = value.toString();
      String[] values = line.trim().split(",");
      if (Integer.parseInt(values[3].trim()) != 0)
        if (values[0].trim().equals("A")) {
          outputK.set(values[2].trim());
          outputV.set("A," + values[1].trim() + "," + values[3].trim());
          context.write(outputK, outputV);
        } else if (values[0].trim().equals("B")) {
          outputK.set(values[1].trim());
          outputV.set("B," + values[2].trim() + "," + values[3].trim());
          context.write(outputK, outputV);
        }
    }
  }
}