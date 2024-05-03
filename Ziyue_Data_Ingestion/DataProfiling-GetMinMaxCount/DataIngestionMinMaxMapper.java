import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class DataIngestionMinMaxMapper extends Mapper<Object, Text, Text, MinMaxCountTuple>{
    private Text areaCode = new Text();
    private MinMaxCountTuple outTuple = new MinMaxCountTuple();
    public void map(Object key, Text val, Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        String DELIMITER = conf.get("DELIMITER");
        int COLUMN_KEY = Integer.parseInt(conf.get("COLUMN_KEY"));
        int COLUMN_VALUE = Integer.parseInt(conf.get("COLUMN_VALUE"));
	    String vals = val.toString();
      	if(vals == null || vals.length() == 0) return;
	    String[] tokens = vals.split(DELIMITER);
        //matches("^-?\\d+$")
        // use regex to determine whether it is a legal number input
        // only match string like "-1229" or "453"
	    if(tokens.length > COLUMN_KEY && tokens.length > COLUMN_VALUE && tokens[COLUMN_VALUE].matches("^-?\\d+$")){
	        long num = Long.parseLong(tokens[COLUMN_VALUE]);
            areaCode.set(tokens[COLUMN_KEY]);
            outTuple.setMax(num);
            outTuple.setMin(num);
            outTuple.setCount(1);
	    }
        context.write(areaCode, outTuple);
    }
}

