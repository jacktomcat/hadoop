package com.gochinatv.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> iterable,
			Context context) throws IOException, InterruptedException {
		System.out.println("======================reducer===============");
		int sum = 0;
        for (IntWritable val : iterable) {
           sum += val.get();
        }
        context.write(key, new IntWritable(sum));
        
	}

	
}
