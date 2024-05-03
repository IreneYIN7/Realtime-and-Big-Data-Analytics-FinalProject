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
  private Map<String, String> countryMap = getCountryMap();

  @Override
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

      String line = value.toString();
      String[] fields = line.split(",");
    
      // Assume that the CSV has the correct number of fields
      if (fields.length == 10) {
        String locationKey = fields[1];
        outTuple.setlocationKey(new Text(locationKey));
        outTuple.setnewConfirmed(parseLongWritable(fields[2]));
        outTuple.setnewDeceased(parseLongWritable(fields[3]));
        outTuple.setnewRecovered(parseLongWritable(fields[4]));
        outTuple.setnewTested(parseLongWritable(fields[5]));
        outTuple.setcumulativeConfirmed(parseLongWritable(fields[6]));
        outTuple.setcumulativeDeceased(parseLongWritable(fields[7]));
        outTuple.setcumulativeRecovered(parseLongWritable(fields[8]));
        outTuple.setcumulativeTested(parseLongWritable(fields[9]));

        String countryCode;
        String countryName;
        // Check if the location key contains an underscore
        if (locationKey.contains("_")) {
            // Extract the country code
            countryCode = locationKey.split("_")[0];
            // Retrieve the country name from the country map
            countryName = countryMap.getOrDefault(countryCode, "Unknown");
        } else {
            // Use the location key as the country code
            countryCode = locationKey;
            // Retrieve the country name directly using the location key
            countryName = countryMap.getOrDefault(locationKey, "Unknown");
        }

        context.write(new Text(fields[0] + "," + countryName), outTuple);
      }

      
  }

  private LongWritable parseLongWritable(String number) {
    // If the number is null or empty, default to 0.
    if (number == null || number.isEmpty()) {
        return new LongWritable(-1);
    }
    try {
        // Parse the number as a Long
        return new LongWritable(Long.parseLong(number));
    } catch (NumberFormatException e) {
        // Log the error and return -1
        // System.err.println("Error parsing number: " + e.getMessage());
        return new LongWritable(-1);
    }
    
  }


  private static Map<String, String> getCountryMap() {
    Map<String, String> countryMap = new HashMap<>();
    countryMap.put("AF", "Afghanistan");
    countryMap.put("AX", "Aland Islands");
    countryMap.put("AL", "Albania");
    countryMap.put("DZ", "Algeria");
    countryMap.put("AS", "American Samoa");
    countryMap.put("AD", "Andorra");
    countryMap.put("AO", "Angola");
    countryMap.put("AI", "Anguilla");
    countryMap.put("AQ", "Antarctica");
    countryMap.put("AG", "Antigua and Barbuda");
    countryMap.put("AR", "Argentina");
    countryMap.put("AM", "Armenia");
    countryMap.put("AW", "Aruba");
    countryMap.put("AU", "Australia");
    countryMap.put("AT", "Austria");
    countryMap.put("AZ", "Azerbaijan");
    countryMap.put("BS", "Bahamas");
    countryMap.put("BH", "Bahrain");
    countryMap.put("BD", "Bangladesh");
    countryMap.put("BB", "Barbados");
    countryMap.put("BY", "Belarus");
    countryMap.put("BE", "Belgium");
    countryMap.put("BZ", "Belize");
    countryMap.put("BJ", "Benin");
    countryMap.put("BM", "Bermuda");
    countryMap.put("BT", "Bhutan");
    countryMap.put("BO", "Bolivia");
    countryMap.put("BA", "Bosnia and Herzegovina");
    countryMap.put("BW", "Botswana");
    countryMap.put("BV", "Bouvet Island");
    countryMap.put("BR", "Brazil");
    countryMap.put("IO", "British Indian Ocean Territory");
    countryMap.put("BN", "Brunei Darussalam");
    countryMap.put("BG", "Bulgaria");
    countryMap.put("BF", "Burkina Faso");
    countryMap.put("BI", "Burundi");
    countryMap.put("KH", "Cambodia");
    countryMap.put("CM", "Cameroon");
    countryMap.put("CA", "Canada");
    countryMap.put("CV", "Cape Verde");
    countryMap.put("KY", "Cayman Islands");
    countryMap.put("CF", "Central African Republic");
    countryMap.put("TD", "Chad");
    countryMap.put("CL", "Chile");
    countryMap.put("CN", "China");
    countryMap.put("CX", "Christmas Island");
    countryMap.put("CC", "Cocos (Keeling) Islands");
    countryMap.put("CO", "Colombia");
    countryMap.put("KM", "Comoros");
    countryMap.put("CG", "Congo");
    countryMap.put("CD", "Congo, Democratic Republic of the");
    countryMap.put("CK", "Cook Islands");
    countryMap.put("CR", "Costa Rica");
    countryMap.put("CI", "Cote d'Ivoire");
    countryMap.put("HR", "Croatia");
    countryMap.put("CU", "Cuba");
    countryMap.put("CY", "Cyprus");
    countryMap.put("CZ", "Czech Republic");
    countryMap.put("DK", "Denmark");
    countryMap.put("DJ", "Djibouti");
    countryMap.put("DM", "Dominica");
    countryMap.put("DO", "Dominican Republic");
    countryMap.put("EC", "Ecuador");
    countryMap.put("EG", "Egypt");
    countryMap.put("SV", "El Salvador");
    countryMap.put("GQ", "Equatorial Guinea");
    countryMap.put("ER", "Eritrea");
    countryMap.put("EE", "Estonia");
    countryMap.put("ET", "Ethiopia");
    countryMap.put("FK", "Falkland Islands (Malvinas)");
    countryMap.put("FO", "Faroe Islands");
    countryMap.put("FJ", "Fiji");
    countryMap.put("FI", "Finland");
    countryMap.put("FR", "France");
    countryMap.put("GF", "French Guiana");
    countryMap.put("PF", "French Polynesia");
    countryMap.put("TF", "French Southern Territories");
    countryMap.put("GA", "Gabon");
    countryMap.put("GM", "Gambia");
    countryMap.put("GE", "Georgia");
    countryMap.put("DE", "Germany");
    countryMap.put("GH", "Ghana");
    countryMap.put("GI", "Gibraltar");
    countryMap.put("GR", "Greece");
    countryMap.put("GL", "Greenland");
    countryMap.put("GD", "Grenada");
    countryMap.put("GP", "Guadeloupe");
    countryMap.put("GU", "Guam");
    countryMap.put("GT", "Guatemala");
    countryMap.put("GG", "Guernsey");
    countryMap.put("GN", "Guinea");
    countryMap.put("GW", "Guinea-Bissau");
    countryMap.put("GY", "Guyana");
    countryMap.put("HT", "Haiti");
    countryMap.put("HM", "Heard Island and McDonald Islands");
    countryMap.put("VA", "Holy See (Vatican City State)");
    countryMap.put("HN", "Honduras");
    countryMap.put("HK", "Hong Kong");
    countryMap.put("HU", "Hungary");
    countryMap.put("IS", "Iceland");
    countryMap.put("IN", "India");
    countryMap.put("ID", "Indonesia");
    countryMap.put("IR", "Iran, Islamic Republic of");
    countryMap.put("IQ", "Iraq");
    countryMap.put("IE", "Ireland");
    countryMap.put("IM", "Isle of Man");
    countryMap.put("IL", "Israel");
    countryMap.put("IT", "Italy");
    countryMap.put("JM", "Jamaica");
    countryMap.put("JP", "Japan");
    countryMap.put("JE", "Jersey");
    countryMap.put("JO", "Jordan");
    countryMap.put("KZ", "Kazakhstan");
    countryMap.put("KE", "Kenya");
    countryMap.put("KI", "Kiribati");
    countryMap.put("KP", "Korea, Democratic People's Republic of");
    countryMap.put("KR", "Korea, Republic of");
    countryMap.put("KW", "Kuwait");
    countryMap.put("KG", "Kyrgyzstan");
    countryMap.put("LA", "Lao People's Democratic Republic");
    countryMap.put("LV", "Latvia");
    countryMap.put("LB", "Lebanon");
    countryMap.put("LS", "Lesotho");
    countryMap.put("LR", "Liberia");
    countryMap.put("LY", "Libyan Arab Jamahiriya");
    countryMap.put("LI", "Liechtenstein");
    countryMap.put("LT", "Lithuania");
    countryMap.put("LU", "Luxembourg");
    countryMap.put("MO", "Macao");
    countryMap.put("MK", "Macedonia, the Former Yugoslav Republic of");
    countryMap.put("MG", "Madagascar");
    countryMap.put("MW", "Malawi");
    countryMap.put("MY", "Malaysia");
    countryMap.put("MV", "Maldives");
    countryMap.put("ML", "Mali");
    countryMap.put("MT", "Malta");
    countryMap.put("MH", "Marshall Islands");
    countryMap.put("MQ", "Martinique");
    countryMap.put("MR", "Mauritania");
    countryMap.put("MU", "Mauritius");
    countryMap.put("YT", "Mayotte");
    countryMap.put("MX", "Mexico");
    countryMap.put("FM", "Micronesia, Federated States of");
    countryMap.put("MD", "Moldova, Republic of");
    countryMap.put("MC", "Monaco");
    countryMap.put("MN", "Mongolia");
    countryMap.put("ME", "Montenegro");
    countryMap.put("MS", "Montserrat");
    countryMap.put("MA", "Morocco");
    countryMap.put("MZ", "Mozambique");
    countryMap.put("MM", "Myanmar");
    countryMap.put("NA", "Namibia");
    countryMap.put("NR", "Nauru");
    countryMap.put("NP", "Nepal");
    countryMap.put("NL", "Netherlands");
    countryMap.put("AN", "Netherlands Antilles");
    countryMap.put("NC", "New Caledonia");
    countryMap.put("NZ", "New Zealand");
    countryMap.put("NI", "Nicaragua");
    countryMap.put("NE", "Niger");
    countryMap.put("NG", "Nigeria");
    countryMap.put("NU", "Niue");
    countryMap.put("NF", "Norfolk Island");
    countryMap.put("MP", "Northern Mariana Islands");
    countryMap.put("NO", "Norway");
    countryMap.put("OM", "Oman");
    countryMap.put("PK", "Pakistan");
    countryMap.put("PW", "Palau");
    countryMap.put("PS", "Palestinian Territory, Occupied");
    countryMap.put("PA", "Panama");
    countryMap.put("PG", "Papua New Guinea");
    countryMap.put("PY", "Paraguay");
    countryMap.put("PE", "Peru");
    countryMap.put("PH", "Philippines");
    countryMap.put("PN", "Pitcairn");
    countryMap.put("PL", "Poland");
    countryMap.put("PT", "Portugal");
    countryMap.put("PR", "Puerto Rico");
    countryMap.put("QA", "Qatar");
    countryMap.put("RE", "Reunion");
    countryMap.put("RO", "Romania");
    countryMap.put("RU", "Russian Federation");
    countryMap.put("RW", "Rwanda");
    countryMap.put("BL", "Saint Barthelemy");
    countryMap.put("SH", "Saint Helena");
    countryMap.put("KN", "Saint Kitts and Nevis");
    countryMap.put("LC", "Saint Lucia");
    countryMap.put("MF", "Saint Martin");
    countryMap.put("PM", "Saint Pierre and Miquelon");
    countryMap.put("VC", "Saint Vincent and the Grenadines");
    countryMap.put("WS", "Samoa");
    countryMap.put("SM", "San Marino");
    countryMap.put("ST", "Sao Tome and Principe");
    countryMap.put("SA", "Saudi Arabia");
    countryMap.put("SN", "Senegal");
    countryMap.put("RS", "Serbia");
    countryMap.put("SC", "Seychelles");
    countryMap.put("SL", "Sierra Leone");
    countryMap.put("SG", "Singapore");
    countryMap.put("SK", "Slovakia");
    countryMap.put("SI", "Slovenia");
    countryMap.put("SB", "Solomon Islands");
    countryMap.put("SO", "Somalia");
    countryMap.put("ZA", "South Africa");
    countryMap.put("GS", "South Georgia and the South Sandwich Islands");
    countryMap.put("ES", "Spain");
    countryMap.put("LK", "Sri Lanka");
    countryMap.put("SD", "Sudan");
    countryMap.put("SR", "Suriname");
    countryMap.put("SJ", "Svalbard and Jan Mayen");
    countryMap.put("SZ", "Swaziland");
    countryMap.put("SE", "Sweden");
    countryMap.put("CH", "Switzerland");
    countryMap.put("SY", "Syrian Arab Republic");
    countryMap.put("TW", "Taiwan, Province of China");
    countryMap.put("TJ", "Tajikistan");
    countryMap.put("TZ", "Tanzania, United Republic of");
    countryMap.put("TH", "Thailand");
    countryMap.put("TL", "Timor-Leste");
    countryMap.put("TG", "Togo");
    countryMap.put("TK", "Tokelau");
    countryMap.put("TO", "Tonga");
    countryMap.put("TT", "Trinidad and Tobago");
    countryMap.put("TN", "Tunisia");
    countryMap.put("TR", "Turkey");
    countryMap.put("TM", "Turkmenistan");
    countryMap.put("TC", "Turks and Caicos Islands");
    countryMap.put("TV", "Tuvalu");
    countryMap.put("UG", "Uganda");
    countryMap.put("UA", "Ukraine");
    countryMap.put("AE", "United Arab Emirates");
    countryMap.put("GB", "United Kingdom");
    countryMap.put("US", "United States");
    countryMap.put("UM", "United States Minor Outlying Islands");
    countryMap.put("UY", "Uruguay");
    countryMap.put("UZ", "Uzbekistan");
    countryMap.put("VU", "Vanuatu");
    countryMap.put("VE", "Venezuela");
    countryMap.put("VN", "Viet Nam");
    countryMap.put("VG", "Virgin Islands, British");
    countryMap.put("VI", "Virgin Islands, U.S.");
    countryMap.put("WF", "Wallis and Futuna");
    countryMap.put("EH", "Western Sahara");
    countryMap.put("YE", "Yemen");
    countryMap.put("ZM", "Zambia");
    countryMap.put("ZW", "Zimbabwe");
    
    // Return the mutable HashMap
    return countryMap;
  }
}
