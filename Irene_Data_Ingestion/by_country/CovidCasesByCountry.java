import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import java.nio.file.Paths;

public class CovidCasesByCountry {
      public static void main(String[] args) throws Exception {
  
  if (args.length != 2) {
    System.err.println("Usage: covid cases by country <covid path> <output path>");
    System.exit(-1);
  }
  // Create a new job
  Job job = Job.getInstance();
  job.setJarByClass(CovidCasesByCountry.class);
  job.setJobName("COVID Data Processing");

  // Set input and output paths
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));

  // Set the Mapper and Reducer classes
  job.setMapperClass(CovidCasesMapper.class);
  job.setNumReduceTasks(1);
  job.setReducerClass(CovidCasesReducer.class);

  // Specify the output types produced by the Mapper
  job.setMapOutputKeyClass(Text.class);
  job.setMapOutputValueClass(CovidDataWritable.class);

  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(Text.class);
  // Set the number of Reduce tasks
  job.setNumReduceTasks(1);

  // Exit with a success or failure code
  System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
