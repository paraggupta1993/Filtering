/*
  javac -cp ${HADOOP_CLASSPATH} -d fwk FrameWork.java  TokenizerMapper.java IntSumReducer.java 
  jar -cvf fwk.jar -C fwk/ .
  hadoop jar fwk.jar hadoop.FrameWork /user/parag/input /user/parag/fwk_output
*/

package hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class FrameWork extends Configured implements Tool {

  static int MAX_ITERATION = 2;

  static int printUsage() {
    System.out.println("<jobname> <input_path> <output_path>\n");
    return -1;
  }

  private Job getJob(String jobname, int mappers, int reducers) throws IOException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, jobname); // jobname
    job.setJarByClass(FrameWork.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    //job.setNumMapTasks(mappers); 
    //job.setNumReduceTasks(reducers); 

    // the keys are the unique identifiers for a Node (ints in this case).
    //job.setOutputKeyClass(IntWritable.class);
    // the values are the string representation of a Node
    ///job.setOutputValueClass(Text.class);

    return job;
  }

  public int run(String[] args) throws Exception {
    String jobname = args[0];
    String input_path = args[1];
    String output_path = args[2];
    String final_output_path;

    int iterationCount = 0;
    int mappers = 1;
    int reducers = 1;

    Job job;
    FileSystem fs = FileSystem.get(new Configuration());

    while (iterationCount < MAX_ITERATION) {
      
      //DeleteAllFiles.deleteFile(user,"tmp", jobname + "-" + iterationCount);

      output_path = "/tmp/" + jobname + "-" + (iterationCount + 1);
      job = getJob(jobname, mappers, reducers);
      FileInputFormat.addInputPath(job, new Path(input_path));
      FileOutputFormat.setOutputPath(job, new Path(output_path));

      job.waitForCompletion(true);

      input_path = output_path;
      iterationCount++;
    }

    //if (iterationCount >= MAX_ITERATION)
      //return 2; 

    return 0;
  }


  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FrameWork(), args);
    System.exit(res);
  }
}



  


