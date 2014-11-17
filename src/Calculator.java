package hadoop.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.*;
import java.lang.Math.*;

public class Calculator {

    static ArrayList movieIDs;
    public static class MovieMapper extends Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString(), "::");
            IntWritable key_mid = new IntWritable();
            Text value_null = new Text();

            String mid = itr.nextToken();
            int int_mid = Integer.parseInt(mid);
            key_mid.set(int_mid);
            movieIDs.add(int_mid);

            value_null.set("");
            context.write(key_mid, value_null);
        }
    }

    public static class UserRatingReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values, 
                Context context) throws IOException, InterruptedException {

            int mid = key.get();
            int target_mid;

            String ratings_1 = getRatings(mid);
            if (ratings_1 == null) {
                context.write(key, new Text("ALL -1"));
                return;
            }
            StringTokenizer itr = new StringTokenizer(ratings_1, "|");
            Map<Integer, Double> ratings_1_map = new HashMap<Integer, Double>();

            while (itr.hasMoreTokens()) {
                StringTokenizer child = new StringTokenizer(itr.nextToken(), " ");
                int item_key = Integer.parseInt(child.nextToken());
                Double item_value = Double.parseDouble(child.nextToken());
                ratings_1_map.put(item_key, item_value);
            }
            
            for (int i = 0; i < 10681; i++) {
                target_mid = (int)movieIDs.get(i);
                if (target_mid > mid) {
                    double sum = 0;
                    String ratings_2 = getRatings(target_mid);
                    if (ratings_2 == null) {
                        context.write(key, new Text(String.valueOf(target_mid) + " -1"));
                        continue;
                    }
                    itr = new StringTokenizer(ratings_2, "|");
                    Map<Integer, Double> ratings_2_map = new HashMap<Integer, Double>();
                    while (itr.hasMoreTokens()) {
                        StringTokenizer child = new StringTokenizer(itr.nextToken(), " ");
                        int item_key = Integer.parseInt(child.nextToken());
                        Double item_value = Double.parseDouble(child.nextToken());
                        ratings_2_map.put(item_key, item_value);
                    }

                    Iterator keys = ratings_1_map.keySet().iterator();
                    while (keys.hasNext()) {
                        Integer key_ = (Integer)keys.next();
                        if (ratings_2_map.containsKey(key_))
                            sum += Math.pow(ratings_1_map.get(key_) - ratings_2_map.get(key_), 2);
                    }
                    sum = 1 / Math.sqrt(sum + 1);

                    result.set(String.valueOf(target_mid) + " " + String.valueOf(sum));
                    context.write(key, result);
                }
            }
        }
    }


    public static Map<Integer, String> db;
    public static void loadDB() throws IOException, UnsupportedEncodingException {
        String ratings = new String(readFile("/movies/one_step1/part-r-00000"));
        StringTokenizer itr = new StringTokenizer(ratings, "\n");
        while (itr.hasMoreTokens()) {
            String line = itr.nextToken();
            String[] items = line.split(":");
            if (!items[0].equals("") && !items[1].equals(""))
                db.put(Integer.parseInt(items[0]), items[1]);
        }
    }

    public static String getRatings(int mid) throws IOException, UnsupportedEncodingException {
        return db.get(mid);
        //return "1 2|2 3.5|213 4.5";
    }

    //static public int count;
    //public static class MoviePartitioner extends Partitioner<IntWritable, Text> {
    //    @Override
    //    public int getPartition(IntWritable key, Text value, int numOfPartitions) {
    //        //int destination = key.get() / 1000;
    //        //return destination;
    //        int destination = 0;
    //        for (int i = 0; i <= 10681; i++) {
    //            if ((Integer)movieIDs.get(i) > key.get())
    //                return key.get();
    //        }
    //        return 1;
    //    }
    //}

    public static byte[] readFile(String file_path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(file_path);
        byte[] buffer = new byte[1];

        if(fs.exists(path)){
            FSDataInputStream is = fs.open(path);
            FileStatus status = fs.getFileStatus(path);
            buffer = new byte[Integer.parseInt(String.valueOf(status.getLen()))];
            is.readFully(0, buffer);
            is.close();
            fs.close();
        }

        return buffer;
    }

    public static void main(String[] args) throws Exception {
        Calculator.db = new HashMap<Integer, String>();
        Calculator.loadDB();
        Calculator.movieIDs = new ArrayList<Integer>();
        Configuration conf = new Configuration();

        // set separator between key and value to ":", default is "\t"
        conf.set("mapreduce.output.textoutputformat.separator", ":");


        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(Calculator.class);
        job.setMapperClass(MovieMapper.class);
        job.setCombinerClass(UserRatingReducer.class);
        job.setReducerClass(UserRatingReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
