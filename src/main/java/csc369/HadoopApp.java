package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> <input dir> <output dir>");
	    System.exit(-1);
	} else if ("Query1".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(Query1.ReducerImpl.class);
		job.setMapperClass(Query1.MapperImpl.class);
		job.setOutputKeyClass(Query1.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query1.OUTPUT_VALUE_CLASS);
	} else if ("Query2".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(Query2.ReducerImpl.class);
		job.setMapperClass(Query2.MapperImpl.class);
		job.setOutputKeyClass(Query2.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query2.OUTPUT_VALUE_CLASS);
	} else if ("Query3".equalsIgnoreCase(otherArgs[0])) {
		job.setReducerClass(Query3.ReducerImpl.class);
		job.setMapperClass(Query3.MapperImpl.class);
		job.setOutputKeyClass(Query3.OUTPUT_KEY_CLASS);
		job.setOutputValueClass(Query3.OUTPUT_VALUE_CLASS);
	} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
