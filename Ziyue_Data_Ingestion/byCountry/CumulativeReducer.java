import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class CumulativeReducer extends Reducer<Text, CumulativeWritable, Text, Text>{
    @Override
    public void reduce(Text key, Iterable<CumulativeWritable> values, Context context)
            throws IOException, InterruptedException {
        for(CumulativeWritable val: values) context.write(new Text(key+","+val.toString()), new Text(""));  // Output the line as is.
    }

}
