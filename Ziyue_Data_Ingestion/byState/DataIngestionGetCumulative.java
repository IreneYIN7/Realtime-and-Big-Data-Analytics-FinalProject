import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
public class DataIngestionGetCumulative {
    public static void main(String[] args)throws Exception{
        if (args.length != 6) {
            System.err.println("Usage: DataIngestion <input path> <output path> <column_indx1> <column_indx2> <column_indx3> <delimiter>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        conf.set("indx1", args[2]);
        conf.set("indx2", args[3]);
        conf.set("indx3", args[4]);
        conf.set("DELIMITER", args[5]);
        Job job = Job.getInstance(conf);
        job.setJarByClass(DataIngestionGetCumulative.class);
        job.setJobName("DataIngestionGetCumulative");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CumulativeMapperByState.class);
	job.setReducerClass(CumulativeReducer.class);

        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CumulativeWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
