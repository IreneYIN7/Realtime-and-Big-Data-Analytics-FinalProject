import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CovidDataWritable implements Writable {
    private Text date;
    private Text key;
    private LongWritable newConfirmed;
    private LongWritable newDeceased;
    private LongWritable newRecovered;
    private LongWritable newTested;
    private LongWritable cumulativeConfirmed;
    private LongWritable cumulativeDeceased;
    private LongWritable cumulativeRecovered;
    private LongWritable cumulativeTested;

    public CovidDataWritable() {
        this.date = new Text();
        this.key = new Text();
        this.newConfirmed = new LongWritable();
        this.newDeceased = new LongWritable();
        this.newRecovered = new LongWritable();
        this.newTested = new LongWritable();
        this.cumulativeConfirmed = new LongWritable();
        this.cumulativeDeceased = new LongWritable();
        this.cumulativeRecovered = new LongWritable();
        this.cumulativeTested = new LongWritable();
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

    public LongWritable getnewConfirmed() {
        return new LongWritable(newConfirmed.get());
    }
    public void setnewConfirmed(LongWritable val) {
        this.newConfirmed.set(val.get());
    }

    public LongWritable getnewDeceased() {
        return new LongWritable(newDeceased.get());
    }
    public void setnewDeceased(LongWritable val) {
        this.newDeceased.set(val.get());
    }

    public LongWritable getnewRecovered() {
      return new LongWritable(newRecovered.get());
    }
    public void setnewRecovered(LongWritable val) {
        this.newRecovered.set(val.get());
    }

    public LongWritable getnewTested() {
      return new LongWritable(newTested.get());
    }
    public void setnewTested(LongWritable val) {
        this.newTested.set(val.get());
    }

    public LongWritable getcumulativeConfirmed() {
        return new LongWritable(cumulativeConfirmed.get());
    }
    public void setcumulativeConfirmed(LongWritable val) {
        this.cumulativeConfirmed.set(val.get());
    }

    public LongWritable getcumulativeDeceased() {
      return new LongWritable(cumulativeDeceased.get());
    }
    public void setcumulativeDeceased(LongWritable val) {
        this.cumulativeDeceased.set(val.get());
    }

    public LongWritable getcumulativeRecovered() {
      return new LongWritable(cumulativeRecovered.get());
    }
    public void setcumulativeRecovered(LongWritable val) {
        this.cumulativeRecovered.set(val.get());
    }

    public LongWritable getcumulativeTested() {
      return new LongWritable(cumulativeTested.get());
    }
    public void setcumulativeTested(LongWritable val) {
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

