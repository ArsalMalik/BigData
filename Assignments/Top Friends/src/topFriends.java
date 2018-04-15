    import java.io.IOException;
    import java.io.PrintStream;
    import java.util.Arrays;
    import java.util.Comparator;
    import java.util.Iterator;
    import java.util.LinkedList;
    import java.util.List;
    import java.util.Map.Entry;
    import java.util.NavigableMap;
    import java.util.Set;
    import java.util.StringTokenizer;
    import java.util.TreeMap;
    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.io.IntWritable;
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
    
    public class topFriends
    {

      public static class Map extends Mapper<LongWritable, Text, Text, Text>
      {
        public void map(LongWritable key, Text value, Context context)
          throws IOException, InterruptedException
        {
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
    
                context.write(new Text(tempArray[0] + "\t" + tempArray[1]), new Text(lineArray[1]));
              }
            }
          }
        }
      }
      

      public static class Reduce extends Reducer<Text, Text, Text, IntWritable>
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
          context.write(key, new IntWritable(list.size()));
        }
      }
    
      //Comparator Class
      public static class Friends
      {
        private int count;
    
        public Friends(int count)
        {
          this.count = count;
        }
    
        public int getCount() {
          return this.count;
        }
    
        static class friendsComp implements Comparator<topFriends.Friends>
        {
          public int compare(topFriends.Friends f1, topFriends.Friends f2)
          {
            if (f1.getCount() > f2.getCount()) {
              return 1;
            }
            return -1;
          }
        }


      //Second Job
      public static class topFriendsMap extends Mapper<LongWritable, Text, NullWritable, Text>
      {
        public static TreeMap<topFriends.Friends, Text> topFriendsMap = new TreeMap(new topFriends.Friends.friendsComp());
    
        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Context context) {
          String line = value.toString();
          String[] tokens = line.split("\t");
          int count = Integer.parseInt(tokens[2]);
          topFriendsMap.put(new topFriends.Friends(count), new Text(value));
    
          Iterator itr = topFriendsMap.entrySet().iterator();
          Map.Entry entry = null;
    
          while (topFriendsMap.size() > 10) {
            entry = (Map.Entry)itr.next();
            itr.remove();
          }
        }
    
        protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException
        {
          for (Text top : topFriendsMap.values())
            context.write(NullWritable.get(), top);
        }
      }

      //Second Job's Reducer
      public static class topFriendsReducer extends Reducer<NullWritable, Text, NullWritable, Text>
      {
        public static TreeMap<topFriends.Friends, Text> topFriendsMap = new TreeMap(new topFriends.Friends.friendsComp());
    
        public void reduce(NullWritable key, Iterable<Text> value, Reducer<NullWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
          for (Text values : value) {
            line = values.toString();
            if (line.length() > 0) {
              String[] tokens = line.split("\t");
              int count = Integer.parseInt(tokens[2]);
              topFriendsMap.put(new topFriends.Friends(count), new Text(values));
            }
          }
          String line;
          Iterator itr = topFriendsMap.entrySet().iterator();
          Map.Entry entry = null;
    
          while (topFriendsMap.size() > 10) {
            entry = (Map.Entry)itr.next();
            itr.remove();
          }
    
          for (Text top : topFriendsMap.descendingMap().values())
            context.write(NullWritable.get(), top);
        }
      }


      //Driver 
      public static void main(String[] args)
        throws Exception
      {
        Configuration conf = new Configuration();
        String[] programArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (programArgs.length != 3) {
          System.err.println("Usage: <input file> <Intermediate output file> <final output file>");
          System.exit(2);
        }
    
        Path inputFile = new Path(programArgs[0]);
        Path intermediateOutputFile = new Path(programArgs[1]);
        Path finalOutputFile = new Path(programArgs[2]);
    
        Job job = Job.getInstance(conf);
        job.setJobName("Friends with most number of mutual Friends!");
        job.setJarByClass(topFriends.class);
        job.setMapperClass(topFriends.Map.class);
        job.setReducerClass(topFriends.Reduce.class);
    
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
    
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    
        FileInputFormat.addInputPath(job, inputFile);
    
       FileOutputFormat.setOutputPath(job, intermediateOutputFile);
    
        boolean success = job.waitForCompletion(true);
        if (success)
        {
          Configuration conf2 = new Configuration();
    
          Job job2 = Job.getInstance(conf2);
          job2.setJarByClass(topFriends.class);
    
          job2.setMapperClass(topFriends.topFriendsMap.class);
          job2.setReducerClass(topFriends.topFriendsReducer.class);
    
          job2.setMapOutputKeyClass(NullWritable.class);
          job2.setMapOutputValueClass(Text.class);
    
          job2.setOutputKeyClass(NullWritable.class);
          job2.setOutputValueClass(Text.class);
    
          FileInputFormat.addInputPath(job2, intermediateOutputFile);
          FileOutputFormat.setOutputPath(job2, finalOutputFile);
    
          System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
      }
    
      }
      
    }
