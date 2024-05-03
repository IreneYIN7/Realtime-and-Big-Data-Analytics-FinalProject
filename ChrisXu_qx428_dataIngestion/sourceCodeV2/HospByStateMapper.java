
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.naming.Context;

public class HospByStateMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final String DEFAULT_VALUE = "0.00";
    private final Map<String, String> stateMap = getStateMap();
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] data = line.split(",",-1);
        String location = data[1];
        String countryName = stateMap.getOrDefault(location, "Unknown");
        if (!countryName.equals("Unknown")) {
            StringBuilder sb = new StringBuilder();
            sb.append(data[0]);
            sb.append(",");
            sb.append(countryName);
            sb.append(",");
            String new_hospitalized_patients = data[2].trim().isEmpty() ? DEFAULT_VALUE : data[2];
            sb.append(new_hospitalized_patients);
            sb.append(",");
            String cumulative_hospitalized_patients = data[3].trim().isEmpty() ? DEFAULT_VALUE : data[3];
            sb.append(cumulative_hospitalized_patients);
            sb.append(",");
            String new_intensive_care_patients = data[5].trim().isEmpty() ? DEFAULT_VALUE : data[5];
            sb.append(new_intensive_care_patients);
            sb.append(",");
            String cumulative_intensive_care_patients = data[6].trim().isEmpty() ? DEFAULT_VALUE : data[6];
            sb.append(cumulative_intensive_care_patients);
            
            context.write(new Text(sb.toString()), new Text(""));
        }
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
