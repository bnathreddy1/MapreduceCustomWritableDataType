
import java.io.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Weblogs {

public static void main(String[] args) throws Exception 
 {
      Configuration conf = new Configuration();
      Job job = new Job();
      job.setJobName("WebLog Reader");

      job.setJarByClass(Weblogs.class);

      job.setMapperClass(WebLogMapper.class);
      job.setReducerClass(WebLogReducer.class);


      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapOutputKeyClass(WebLogWritable.class);
      job.setMapOutputValueClass(IntWritable.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
      //setting the second argument as a path in a path variable	         
      Path outputPath = new Path(args[1]);
      
      //deleting the output path automatically from hdfs so that we don't have delete it explicitly	         
      outputPath.getFileSystem(conf).delete(outputPath);

      System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}
