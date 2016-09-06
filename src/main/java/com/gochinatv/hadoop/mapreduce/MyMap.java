package com.gochinatv.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMap extends Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println("======================map===============");
		String line = value.toString();
        String[] worlds = line.split(" ");
        for (String world : worlds) {
        	context.write(new Text(world), new IntWritable(1));
		}
	}
	
}
