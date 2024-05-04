import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Economy {
    public static void main(String[] args) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: SearchTrend <input.txt path> <output path>");
            System.exit(-1);
        }
        String inputPath = args[0];
        String outputPath = args[1];

        Job job = Job.getInstance();
        job.setJarByClass(Economy.class);
        job.setJobName("Economy");

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(EconomyMapper.class);
        job.setReducerClass(EconomyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
    }
}
