package com.dima.hadoop.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.text.SimpleDateFormat;
import java.util.Date;

public class WordCount {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println(String.format("Invalid number of arguments. Got %d, expected %d.", args.length, 2));
            System.exit(1);
        }

        String inputPath = args[0];
        int numReduceTasks = 1;
        try {
            numReduceTasks = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Got invalid number of reduce tasks.");
            System.exit(1);
        }
        System.out.println("Set number of reduce tasks to: " + numReduceTasks);

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(numReduceTasks);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd-HHmmss");
        FileOutputFormat.setOutputPath(job, new Path("output-" + sf.format(new Date()).toString()));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
