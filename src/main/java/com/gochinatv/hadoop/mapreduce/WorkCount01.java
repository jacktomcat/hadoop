package com.gochinatv.hadoop.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WorkCount01 {
   
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "worldcount");
        
        job.setJarByClass(WorkCount01.class);
		job.setMapperClass(MyMap.class);
		job.setReducerClass(MyReduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job,new Path("hdfs://192.168.3.191/zhuhh/worldcount.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.3.191/zhuhh-output1"));
		
		job.waitForCompletion(true);
	}
}
