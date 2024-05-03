import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.HashMap;
import java.util.Map;
import java.io.IOException;

public class CumulativeMapperByState extends Mapper<Object, Text, Text, CumulativeWritable>{
    private Text date_area = new Text();
    private CumulativeWritable outTuple = new CumulativeWritable();
    private final Map<String, String> stateMap = getStateMap();
    public void map(Object key, Text val, Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        String DELIMITER = conf.get("DELIMITER");
        int indx1 = Integer.parseInt(conf.get("indx1"));
        int indx2 = Integer.parseInt(conf.get("indx2"));
        int indx3 = Integer.parseInt(conf.get("indx3"));
        String vals = val.toString();
        if(vals == null || vals.length() == 0) return;
        String[] tokens = vals.split(DELIMITER);
        String location = tokens[1];
        if(location.lastIndexOf("_") == 2){
            String stateCode = location.split("_")[1];
            String stateName = stateMap.getOrDefault(stateCode, "Unknown");
            outTuple.setCumulative_persons_vaccinated(0l);
            outTuple.setCumulative_persons_fully_vaccinated(0l);
            outTuple.setCumulative_vaccine_doses_administered(0l);
            date_area.set(tokens[0] + ","+ stateName);
            // use regex to determine whether it is a legal number input
            // only match string like "-1229" or "453"
            if(tokens.length > indx1 && tokens[indx1].matches("^-?\\d+$")){
                outTuple.setCumulative_persons_vaccinated(Long.parseLong(tokens[indx1]));
            }
            if(tokens.length > indx2 && tokens[indx2].matches("^-?\\d+$")){
                outTuple.setCumulative_persons_fully_vaccinated(Long.parseLong(tokens[indx2]));
            }
            if(tokens.length > indx3 && tokens[indx3].matches("^-?\\d+$")){
                outTuple.setCumulative_vaccine_doses_administered(Long.parseLong(tokens[indx3]));
            }
            context.write(date_area, outTuple);
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
