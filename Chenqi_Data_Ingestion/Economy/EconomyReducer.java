import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class EconomyReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, new Text(""));  // Output the line as is.
    }

}
