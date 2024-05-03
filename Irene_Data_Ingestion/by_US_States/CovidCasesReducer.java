import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CovidCasesReducer extends
    Reducer<Text, CovidDataWritable, NullWritable, Text> {
  // Define MISSING_DATA as a constant
  private static final int MISSING_DATA = -1;

  @Override
  public void reduce(Text key, Iterable<CovidDataWritable> values, Context context)
    throws IOException, InterruptedException {
      int sumConfirmed = 0;
      int sumRecovered = 0;
      for (CovidDataWritable val: values){
        int newConfirmed = val.getnewConfirmed().get();
        int newRecovered = val.getnewRecovered().get();
        sumConfirmed += (newConfirmed == MISSING_DATA) ? 0 : newConfirmed;
        sumRecovered += (newRecovered == MISSING_DATA) ? 0 : newRecovered;
      }
      String keyString = key.toString();
      Text output = new Text(
          keyString + "," + String.valueOf(sumConfirmed) + ","+String.valueOf(sumRecovered));
      
      context.write(NullWritable.get(), output);
  }
}
