import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class DataIngestionMinMax{
    public static void main(String[] args) throws Exception{
        if (args.length != 5) {
            System.err.println("Usage: DataIngestion <input path> <output path> <column_key> <column_value> <delimiter>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        conf.set("DELIMITER", args[4]);
        conf.set("COLUMN_KEY", args[2]);
        conf.set("COLUMN_VALUE", args[3]);
        Job job = Job.getInstance(conf);
        job.setJarByClass(DataIngestionMinMax.class);
        job.setJobName("DataIngestion");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(DataIngestionMinMaxMapper.class);
        job.setCombinerClass(DataIngestionMinMaxReducer.class);
        job.setReducerClass(DataIngestionMinMaxReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxCountTuple.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
