import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CovidDataWritable implements Writable {
    private Text date;
    private Text key;
    private IntWritable newConfirmed;
    private IntWritable newDeceased;
    private IntWritable newRecovered;
    private IntWritable newTested;
    private IntWritable cumulativeConfirmed;
    private IntWritable cumulativeDeceased;
    private IntWritable cumulativeRecovered;
    private IntWritable cumulativeTested;

    public CovidDataWritable() {
        this.date = new Text();
        this.key = new Text();
        this.newConfirmed = new IntWritable();
        this.newDeceased = new IntWritable();
        this.newRecovered = new IntWritable();
        this.newTested = new IntWritable();
        this.cumulativeConfirmed = new IntWritable();
        this.cumulativeDeceased = new IntWritable();
        this.cumulativeRecovered = new IntWritable();
        this.cumulativeTested = new IntWritable();
    }

    // Implement the serialization and deserialization methods
    @Override
    public void write(DataOutput out) throws IOException {
        date.write(out);
        key.write(out);
        newConfirmed.write(out);
        newDeceased.write(out);
        newRecovered.write(out);
        newTested.write(out);
        cumulativeConfirmed.write(out);
        cumulativeDeceased.write(out);
        cumulativeRecovered.write(out);
        cumulativeTested.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        date.readFields(in);
        key.readFields(in);
        newConfirmed.readFields(in);
        newDeceased.readFields(in);
        newRecovered.readFields(in);
        newTested.readFields(in);
        cumulativeConfirmed.readFields(in);
        cumulativeDeceased.readFields(in);
        cumulativeRecovered.readFields(in);
        cumulativeTested.readFields(in);
    }

    // Getters and setters for all fields
    public Text getlocationKey() {
        return key;
    }
    public void setlocationKey(Text val) {
        this.key.set(val.toString());
    }

    public IntWritable getnewConfirmed() {
        return new IntWritable(newConfirmed.get());
    }
    public void setnewConfirmed(IntWritable val) {
        this.newConfirmed.set(val.get());
    }

    public IntWritable getnewDeceased() {
        return new IntWritable(newDeceased.get());
    }
    public void setnewDeceased(IntWritable val) {
        this.newDeceased.set(val.get());
    }

    public IntWritable getnewRecovered() {
      return new IntWritable(newRecovered.get());
    }
    public void setnewRecovered(IntWritable val) {
        this.newRecovered.set(val.get());
    }

    public IntWritable getnewTested() {
      return new IntWritable(newTested.get());
    }
    public void setnewTested(IntWritable val) {
        this.newTested.set(val.get());
    }

    public IntWritable getcumulativeConfirmed() {
        return new IntWritable(cumulativeConfirmed.get());
    }
    public void setcumulativeConfirmed(IntWritable val) {
        this.cumulativeConfirmed.set(val.get());
    }

    public IntWritable getcumulativeDeceased() {
      return new IntWritable(cumulativeDeceased.get());
    }
    public void setcumulativeDeceased(IntWritable val) {
        this.cumulativeDeceased.set(val.get());
    }

    public IntWritable getcumulativeRecovered() {
      return new IntWritable(cumulativeRecovered.get());
    }
    public void setcumulativeRecovered(IntWritable val) {
        this.cumulativeRecovered.set(val.get());
    }

    public IntWritable getcumulativeTested() {
      return new IntWritable(cumulativeTested.get());
    }
    public void setcumulativeTested(IntWritable val) {
        this.cumulativeTested.set(val.get());
    }

    // Implement toString for easy printing of the data
    @Override
    public String toString() {
        return date.toString() + ',' + key.toString() + ',' +
               newConfirmed.get() + ',' + newDeceased.get() + ',' +
               newRecovered.get() + ',' + newTested.get() + ',' +
               cumulativeConfirmed.get() + ',' + cumulativeDeceased.get() + ',' +
               cumulativeRecovered.get() + ',' + cumulativeTested.get();
    }
}

