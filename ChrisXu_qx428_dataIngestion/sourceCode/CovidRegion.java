import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

/**
 * country + US states
 * 1. total cases grouped by country including ECON stats
 * 2. total cases grouped by US states
 * 3. cases grouped by date (from country or US states data)
 * 4. cases grouped by date and country
 * 5. cases grouped by date and US states
 */


public class CovidRegion {


  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: CovidRegion <hospitalization path> <econ path> <output_with_date path> <output_without_date path>");
      System.exit(-1);
    }
    
    String hospitalizationPath = args[0];
    String econPath = args[1];
    String outputPath1 = args[2];
    String outputPath2 = args[3];

    Job job = Job.getInstance();
    job.setJarByClass(CovidRegion.class);
    job.setJobName("Covid by region and date");

    FileInputFormat.addInputPath(job, new Path(hospitalizationPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath1));
    
    job.setMapperClass(CovidRegionDateMapper.class);
    job.setReducerClass(CovidRegionDateReducer.class);
    job.setNumReduceTasks(1);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    job.waitForCompletion(true);

    Job job2 = Job.getInstance();
    job2.setJarByClass(CovidRegion.class);
    job2.setJobName("Covid by region + econ data ");
    
    FileInputFormat.addInputPath(job2, new Path(hospitalizationPath));
    FileInputFormat.addInputPath(job2, new Path(econPath));
    FileOutputFormat.setOutputPath(job2, new Path(outputPath2));

    job2.setMapperClass(CovidRegionOnlyMapper.class);
    job2.setReducerClass(CovidRegionOnlyReducer.class);
    job2.setNumReduceTasks(1);

    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    System.exit(job2.waitForCompletion(true) ? 0 : 1);

  }
}