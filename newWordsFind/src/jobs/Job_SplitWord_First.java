package jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import map.Map_SplitWord_First;
import reduce.Reduce_SplitWord_First;

/*
 * 用于切分文本为单词
 * 当词库为空的时候,将文本切分为单字
 */
public class Job_SplitWord_First {
	private static final String JOB_NAME = "Job_SplitWord_First";

	public static void main(String[] args) {
		try {			
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

			if (otherArgs.length != 2) {
				System.err.println("Usage: wordcount <input> <output>");
				System.exit(2);
			}

			System.err.println(otherArgs);
			System.out.println(otherArgs);
			Path input = new Path(otherArgs[0]);
//			Path output_tmp = new Path("/output/job_SplitWord_tmp");
			Path output = new Path(otherArgs[1]);

			FileSystem fs = FileSystem.get(conf);
//			fs.delete(output_tmp, true);
			fs.delete(output, true);

			Job job = Job.getInstance(conf);
			job.setJobName(JOB_NAME);
			job.setJarByClass(Job_SplitWord_First.class);
			
			job.setMapperClass(Map_SplitWord_First.class);
			job.setReducerClass(Reduce_SplitWord_First.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, output);
			job.setNumReduceTasks(5);
			
			int isTrue = job.waitForCompletion(true) ? 0 : 1;
			System.exit(isTrue);

		
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}

	}
}
