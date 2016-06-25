
import java.io.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WebLogReducer extends Reducer <WebLogWritable, IntWritable, Text, IntWritable>
 {
  private IntWritable result = new IntWritable();
  private Text ip = new Text();

  public void reduce(WebLogWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
  {
	  
	  System.out.println("this is reducer");
    int sum = 0;
    ip = key.getIp(); 
    
    for (IntWritable val : values) 
    {
      sum++ ;
    }
    result.set(sum);
    context.write(ip, result);
  }
 }