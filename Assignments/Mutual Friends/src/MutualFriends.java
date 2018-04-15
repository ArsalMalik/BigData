   import java.io.IOException;
   import java.io.PrintStream;
   import java.util.Arrays;
   import java.util.Iterator;
   import java.util.LinkedList;
   import java.util.List;
   import java.util.StringTokenizer;
   import org.apache.commons.logging.Log;
   import org.apache.commons.logging.LogFactory;
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
   
   public class MutualFriends
   {
     static Log logMap = LogFactory.getLog(MutualFriends.Map.class);
   		

   	 public static class Map extends Mapper<LongWritable, Text, Text, Text>{

       public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Context context> 
         throws IOException, InterruptedException {

         Configuration conf = context.getConfiguration();
         String[] User = new String[2];
         User[0] = conf.get("User1");
         User[1] = conf.get("User2");
         Arrays.sort(User);
   
         StringTokenizer str = new StringTokenizer(value.toString(), "\n");
   
         while (str.hasMoreTokens()) {
           String line = str.nextToken();
   
           String[] lineArray = line.split("\t");
           if (lineArray.length == 2) {
             String[] friendArray = lineArray[1].split(",");
             String[] tempArray = new String[2];
             for (int i = 0; i < friendArray.length; i++) {
               tempArray[0] = friendArray[i];
               tempArray[1] = lineArray[0];
               Arrays.sort(tempArray);
   
               if ((tempArray[0].equals(User[0])) && (tempArray[1].equals(User[1]))) {
                 context.write(new Text(tempArray[0] + "\t" + tempArray[1]), new Text(lineArray[1]));
                 MutualFriends.logMap.info("map output:key=" + tempArray[0] + " " + tempArray[1] + " value = " + lineArray[1]);
               }
             }
           }
         }
       }
     }


     public static class Reduce extends Reducer<Text, Text, Text, Text>
     {
       public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Context context)
         throws IOException, InterruptedException
       {
         Text[] texts = new Text[2];
         int index = 0;
         Iterator value = values.iterator();
         while (value.hasNext()) {
           texts[(index++)] = new Text((Text)value.next());
         }
         String[] list1 = texts[0].toString().split(",");
         String[] list2 = texts[1].toString().split(",");
         List list = new LinkedList();
         for (String friend1 : list1) {
           for (String friend2 : list2) {
             if (friend1.equals(friend2)) {
               list.add(friend1);
             }
           }
         }
         
         StringBuffer sb = new StringBuffer();
         for (int i = 0; i < list.size(); i++) {
           sb.append((String)list.get(i));
           if (i != list.size() - 1)
             sb.append(",");
         }
         if (sb.length() == 0) {
           sb.append("They have no mutual/common friends!");
         }
         context.write(key, new Text(sb.toString()));
       }
     }

     	
     public static void main(String[] args)
       throws Exception
     {
       Configuration conf = new Configuration();
       String[] programArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
       if (programArgs.length != 4) {
        System.err.println("Usage: <input file> <output file> <user1> <user2>");
         System.exit(2);
       }
   
        conf.set("User1", args[2]);
        conf.set("User2", args[3]);
   
        Job job = Job.getInstance(conf);
        job.setJobName("MutualFriends");
        job.setJarByClass(MutualFriends.class);
        job.setMapperClass(MutualFriends.Map.class);
        job.setReducerClass(MutualFriends.Reduce.class);
   
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
   
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
   
        FileInputFormat.addInputPath(job, new Path(programArgs[0]));
   
        FileOutputFormat.setOutputPath(job, new Path(programArgs[1]));
   
       System.exit(job.waitForCompletion(true) ? 0 : 1);
     }
   
   }
