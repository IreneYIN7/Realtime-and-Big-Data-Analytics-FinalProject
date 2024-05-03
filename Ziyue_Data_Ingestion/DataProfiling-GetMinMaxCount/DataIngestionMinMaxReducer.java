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
public class DataIngestionMinMaxReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple>{
    private MinMaxCountTuple result = new MinMaxCountTuple();
    public void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException{
        // initialize result
        result.setMin(Long.MAX_VALUE);
        result.setMax(Long.MIN_VALUE);
        result.setCount(0);
        int sum = 0;
        for(MinMaxCountTuple val: values){
            if(result.getMin() == Long.MAX_VALUE || val.getMin() < result.getMin()  ){
                result.setMin(val.getMin());
            }
            if(result.getMax() == Long.MIN_VALUE || val.getMax() > result.getMax()) {
                result.setMax(val.getMax());
            }
            sum += val.getCount();
        }
        result.setCount(sum);
        context.write(key, result);
    }
}
