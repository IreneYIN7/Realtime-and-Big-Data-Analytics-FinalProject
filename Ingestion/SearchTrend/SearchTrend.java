
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class SearchTrend {
    public static void main(String[] args) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: SearchTrend <input.txt path> <output path>");
            System.exit(-1);
        }
        String inputPath = args[0];
        String outputPath = args[1];

        Job job = Job.getInstance();
        job.setJarByClass(SearchTrend.class);
        job.setJobName("SearchTrendByCountry");

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "/byCountry"));

        job.setMapperClass(SearchTrendByCountryMapper.class);
        job.setReducerClass(SearchTrendReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);
        job.waitForCompletion(true);

	Job job2 = Job.getInstance();
	job2.setJarByClass(SearchTrend.class);
        job2.setJobName("SearchTrendByState");

        FileInputFormat.addInputPath(job2, new Path(inputPath));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath + "/byState"));

        job2.setMapperClass(SearchTrendByStateMapper.class);
        job2.setReducerClass(SearchTrendReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setNumReduceTasks(1);
        job2.waitForCompletion(true);
    }
}
