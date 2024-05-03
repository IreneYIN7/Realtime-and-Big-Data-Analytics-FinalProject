import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;

import javax.naming.Context;

public class CovidCasesMapper extends Mapper<LongWritable, Text, Text, CovidDataWritable> {
  private CovidDataWritable outTuple = new CovidDataWritable();
  private Map<String, String> stateMap = getStateMap();

  @Override
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

      String line = value.toString();
      String[] fields = line.split(",");
    
      // Assume that the CSV has the correct number of fields
      if (fields.length == 10 && fields[1].startsWith("US_")) {
        String locationKey = fields[1];
        outTuple.setlocationKey(new Text(locationKey));
        outTuple.setnewConfirmed(parseIntWritable(fields[2]));
        outTuple.setnewDeceased(parseIntWritable(fields[3]));
        outTuple.setnewRecovered(parseIntWritable(fields[4]));
        outTuple.setnewTested(parseIntWritable(fields[5]));
        outTuple.setcumulativeConfirmed(parseIntWritable(fields[6]));
        outTuple.setcumulativeDeceased(parseIntWritable(fields[7]));
        outTuple.setcumulativeRecovered(parseIntWritable(fields[8]));
        outTuple.setcumulativeTested(parseIntWritable(fields[9]));

        // Assuming 'fields[1]' contains state code after 'US_' prefix
        String stateCode = locationKey.split("_")[1];
        String stateName = stateMap.getOrDefault(stateCode, "Unknown");

        context.write(new Text(fields[0] + "," + stateName), outTuple);
      }

      
  }

  private IntWritable parseIntWritable(String number) {
    // if missing value, default to -1.
    return new IntWritable(number != null && !number.isEmpty() ? Integer.parseInt(number) : -1);
  }

  private static Map<String, String> getStateMap() {
      Map<String, String> stateMap = new HashMap<>();
      stateMap.put("AL", "Alabama");
      stateMap.put("AL", "Alabama");
      stateMap.put("AK", "Alaska");
      stateMap.put("AZ", "Arizona");
      stateMap.put("AR", "Arkansas");
      stateMap.put("AS", "American Samoa");
      stateMap.put("CA", "California");
      stateMap.put("CO", "Colorado");
      stateMap.put("CT", "Connecticut");
      stateMap.put("DE", "Delaware");
      stateMap.put("DC", "District of Columbia");
      stateMap.put("FL", "Florida");
      stateMap.put("GA", "Georgia");
      stateMap.put("GU", "Guam");
      stateMap.put("HI", "Hawaii");
      stateMap.put("ID", "Idaho");
      stateMap.put("IL", "Illinois");
      stateMap.put("IN", "Indiana");
      stateMap.put("IA", "Iowa");
      stateMap.put("KS", "Kansas");
      stateMap.put("KY", "Kentucky");
      stateMap.put("LA", "Louisiana");
      stateMap.put("ME", "Maine");
      stateMap.put("MD", "Maryland");
      stateMap.put("MA", "Massachusetts");
      stateMap.put("MI", "Michigan");
      stateMap.put("MN", "Minnesota");
      stateMap.put("MS", "Mississippi");
      stateMap.put("MO", "Missouri");
      stateMap.put("MT", "Montana");
      stateMap.put("NE", "Nebraska");
      stateMap.put("NV", "Nevada");
      stateMap.put("NH", "New Hampshire");
      stateMap.put("NJ", "New Jersey");
      stateMap.put("NM", "New Mexico");
      stateMap.put("NY", "New York");
      stateMap.put("NC", "North Carolina");
      stateMap.put("ND", "North Dakota");
      stateMap.put("MP", "Northern Mariana Islands");
      stateMap.put("OH", "Ohio");
      stateMap.put("OK", "Oklahoma");
      stateMap.put("OR", "Oregon");
      stateMap.put("PA", "Pennsylvania");
      stateMap.put("PR", "Puerto Rico");
      stateMap.put("RI", "Rhode Island");
      stateMap.put("SC", "South Carolina");
      stateMap.put("SD", "South Dakota");
      stateMap.put("TN", "Tennessee");
      stateMap.put("TX", "Texas");
      stateMap.put("UT", "Utah");
      stateMap.put("VT", "Vermont");
      stateMap.put("VA", "Virginia");
      stateMap.put("VI", "Virgin Islands");
      stateMap.put("WA", "Washington");
      stateMap.put("WV", "West Virginia");
      stateMap.put("WI", "Wisconsin");
      stateMap.put("WY", "Wyoming");
      
      // Return the mutable HashMap
      return stateMap;
  }
}
