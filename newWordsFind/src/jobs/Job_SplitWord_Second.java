package jobs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import map.Map_SplitWord_Second;
import reduce.Reduce_SplitWord_Second;


/*
 * 用于切分文本为单词
 * 当词库为空的时候,将文本切分为单字
 */
public class Job_SplitWord_Second {
	private static final String JOB_NAME = "Job_SplitWord_Second";
	private static Logger logger=LoggerFactory.getLogger(Job_SplitWord_Second.class);

	public static void main(String[] args) {
		try {
			
			Configuration conf = new Configuration();
			System.err.println(args);
			
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

			if (otherArgs.length != 4) {
				System.err.println("Usage: wordcount <input> <output> <pre> <size>");
				System.exit(2);
			}

			Path input = new Path(otherArgs[0]);
//			Path output_tmp = new Path("/output/job_SplitWord_tmp");
			Path output = new Path(otherArgs[1]);
			String resultPath = otherArgs[2];
			String size = otherArgs[3];
			
//			conf.set("PRE", resultPath);
			conf.set("SIZE", size);

			FileSystem fs = FileSystem.get(conf);
//			fs.delete(output_tmp, true);
			fs.delete(output, true);

			Job job = Job.getInstance(conf);
			job.setJobName(JOB_NAME);
			job.setJarByClass(Job_SplitWord_Second.class);
			job.addCacheFile(new URI(resultPath));
			
			job.setMapperClass(Map_SplitWord_Second.class);
			job.setReducerClass(Reduce_SplitWord_Second.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, input);
			FileOutputFormat.setOutputPath(job, output);
			job.setNumReduceTasks(1);
			
			int isTrue = job.waitForCompletion(true) ? 0 : 1;
			System.exit(isTrue);

//			Job secondjob = Job.getInstance(conf);
//			secondjob.setJobName(JOB_NAME + "Second");
//			secondjob.setJarByClass(Job_SplitWord.class);
//			secondjob.setMapperClass(Map_SplitWord_Second.class);
//			secondjob.setReducerClass(Reduce_SplitWord_Second.class);
//			secondjob.setMapOutputKeyClass(NullWritable.class);
//			secondjob.setMapOutputValueClass(Text.class);
//			secondjob.setOutputKeyClass(NullWritable.class);
//			secondjob.setOutputValueClass(Text.class);
//			FileInputFormat.addInputPath(secondjob, output_tmp);
//			FileOutputFormat.setOutputPath(secondjob, output);

//			ControlledJob jobCont = new ControlledJob(conf);
//			ControlledJob secondjobCont = new ControlledJob(conf);
//			jobCont.setJob(job);
//			secondjobCont.setJob(secondjob);
//			secondjobCont.addDependingJob(jobCont);
//			JobControl jobControl = new JobControl(JOB_NAME);
//			jobControl.addJob(jobCont);
//			jobControl.addJob(secondjobCont);

//			Thread thread = new Thread(jobControl);
//			thread.start();
//			while (true) {
//				if (jobControl.allFinished()) {
//					jobControl.stop();
//					System.exit(0);
//				}
//				if (jobControl.getFailedJobList().size() > 0) {
//					jobControl.stop();
//					System.exit(-1);
//				}
//			}
		
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}

	}
}
