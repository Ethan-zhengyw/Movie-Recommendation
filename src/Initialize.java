/*
 * This mapreduce job will save movie and corresponding rating into different page
 * [MovieID1:UserID1 Rating1g|UserID2 Rating2...]
 * [MovieID2:UserID1 Rating1g|UserID2 Rating2...]
 *  ...
 *  each page has 20 movie
 */
package hadoop.group;

import org.apache.hadoop.conf.Configuration;
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

public class Initialize {

    public static class MovieMapper extends Mapper<Object, Text, IntWritable, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString(), "::");
            IntWritable key_mid = new IntWritable();
            Text value_uid_score = new Text();

            String uid = itr.nextToken();
            String mid = itr.nextToken();
            key_mid.set(Integer.parseInt(mid));

            if (!uid.equals("")) {
                value_uid_score.set(uid + " " + itr.nextToken());
                context.write(key_mid, value_uid_score);
            }
        }
    }

    public static class UserRatingReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values, 
                Context context) throws IOException, InterruptedException {

            String uid_score = "";
            for (Text val : values) {
                uid_score += val + "|";
            }

            result.set(uid_score.substring(0, uid_score.length() - 1));
            context.write(key, result);
            //context.write((new IntWritable(key.get())), result);
        }
    }

    public static class MoviePartitioner extends Partitioner<IntWritable, Text> {
        @Override
        public int getPartition(IntWritable key, Text value, int numOfPartitions) {
            int destination = key.get() / 20;
            return destination;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // set separator between key and value to ":", default is "\t"
        conf.set("mapreduce.output.textoutputformat.separator", ":");


        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");

        conf.setInt("reduce.tasks.num", 200); //-------------
        job.setNumReduceTasks(200);               //-----------
        job.setPartitionerClass(MoviePartitioner.class);

        job.setJarByClass(Initialzie.class);
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
