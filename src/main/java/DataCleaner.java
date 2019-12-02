import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;

public class DataCleaner {

    private Reader fileReader;
    private String cleanedJSONFileLocation;
    private BufferedWriter jsonWriter;

    //Default constructor
    public DataCleaner() throws IOException {

        System.out.println("Starting data cleaning process.");

        InputStream inputFileStream = DataCleaner.class.getClassLoader().getResourceAsStream("carpark-rates.csv");
        fileReader = new InputStreamReader(inputFileStream);

        Properties prop = new Properties();
        prop.load(DataRetrieval.class.getClassLoader().getResourceAsStream("config.properties"));

        cleanedJSONFileLocation = prop.get("cleanedoutput.filename").toString();
        jsonWriter = new BufferedWriter(new FileWriter(cleanedJSONFileLocation));
    }

    public static void main (String[] args) throws Exception{

        DataCleaner application = new DataCleaner();
        application.processCarparkRateCSVFile();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Executing shutdown hook...");
                application.shutDown();
            }
        });

    }



    public void processCarparkRateCSVFile() throws Exception {

        Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(fileReader);

        for (CSVRecord record : records) {

            String carpark = record.get("carpark");
            String category = record.get("category");
            String weekdays_rate_1 = replaceLastPunctuation(record.get("weekdays_rate_1"), ".");
            String weekdays_rate_2 = replaceLastPunctuation(record.get("weekdays_rate_2"), ".");
            String saturday_rate = record.get("saturday_rate");
            String sunday_publicholiday_rate = record.get("sunday_publicholiday_rate");

            processAllRateDescriptions(carpark, category, weekdays_rate_1, weekdays_rate_2, saturday_rate, sunday_publicholiday_rate);
        }
    }

    //Method which stores the logic to check if each rate description string should be processed or not
    public void processAllRateDescriptions(String carpark, String category, String weekdays_rate_1, String weekdays_rate_2, String saturday_rate, String sunday_publicholiday_rate) throws IOException {

        //If weekday carpark rate does not contain any monetary value, i.e. its just description, ignore it.
        if (removeDescriptionWithNoValues(weekdays_rate_1) == true) {
            System.out.println("    Skipping " + carpark + " with weekday_rate_1_description " + weekdays_rate_1);
            persistJSONObjectToFile(carpark, category, new JSONObject[6]);
            return;
        }

        JSONObject weekdays_rate_1_obj = parseIndividualRateDescription(weekdays_rate_1);
        JSONObject weekdays_rate_2_obj = parseIndividualRateDescription(weekdays_rate_2);

        //If both objects are the same, discard one
        if ((weekdays_rate_1_obj != null && weekdays_rate_2_obj != null) && weekdays_rate_1_obj.toString().compareToIgnoreCase(weekdays_rate_2_obj.toString()) == 0) {
            weekdays_rate_2_obj = null;
        }

        JSONObject saturday_rate_1_obj, saturday_rate_2_obj, sunday_publicholiday_rate_1_obj, sunday_publicholiday_rate_2_obj;

        //If saturday's rate is same as weekday, copy it
        if (saturday_rate.toLowerCase().contains("same")) {
            saturday_rate_1_obj = weekdays_rate_1_obj;
            saturday_rate_2_obj = weekdays_rate_2_obj;
        }

        else {
            saturday_rate_1_obj = parseIndividualRateDescription(saturday_rate);
            saturday_rate_2_obj = null;
        }

        //If sunday's rate is same as weekday, copy it
        if (sunday_publicholiday_rate.toLowerCase().contains("same") && sunday_publicholiday_rate.toLowerCase().contains("wkday")) {
            sunday_publicholiday_rate_1_obj = weekdays_rate_1_obj;
            sunday_publicholiday_rate_2_obj = weekdays_rate_2_obj;
        }

        //If sunday's rate is same as saturday, copy it
        else if (sunday_publicholiday_rate.toLowerCase().contains("same") && sunday_publicholiday_rate.toLowerCase().contains("saturday")) {
            sunday_publicholiday_rate_1_obj = saturday_rate_1_obj;
            sunday_publicholiday_rate_2_obj = saturday_rate_2_obj;
        }

        else {
            sunday_publicholiday_rate_1_obj = parseIndividualRateDescription(sunday_publicholiday_rate);
            sunday_publicholiday_rate_2_obj = null;
        }

        JSONObject[] ratesJSONObjectArr = new JSONObject[6];
        ratesJSONObjectArr[0] = weekdays_rate_1_obj;
        ratesJSONObjectArr[1] = weekdays_rate_2_obj;
        ratesJSONObjectArr[2] = saturday_rate_1_obj;
        ratesJSONObjectArr[3] = saturday_rate_2_obj;
        ratesJSONObjectArr[4] = sunday_publicholiday_rate_1_obj;
        ratesJSONObjectArr[5] = sunday_publicholiday_rate_2_obj;

        persistJSONObjectToFile(carpark, category, ratesJSONObjectArr);

    }


    //Process the string representation of the parking rate
    public JSONObject parseIndividualRateDescription(String inputRateString) {

        String[] inputRateStringTokenized = inputRateString.split(":");

        if (inputRateStringTokenized.length > 1) {

            //Process the Time constraint factor and its corresponding parking rate
            String time = inputRateStringTokenized[0];
            String charges = inputRateStringTokenized[1];

            if (time.toLowerCase().contains("am") && time.toLowerCase().contains("pm") && time.toLowerCase().contains("daily") == false && time.length() <=15) { //15 character because longest length is 12.00am-12.00pm

                Float[] startEndTime = processTimeInAmPmFormat(time);

                if (startEndTime != null) { //Process the parking rate if the time is present

                    if (charges.toLowerCase().contains("$") && StringUtils.countMatches(charges, "$") == 1 && charges.contains("sub") == false) { //If it is a simple $X per X hr/min rate

                        charges = charges.replace("/", " per ").replace("for", " per ").replaceAll("  ", " ");

                        Float[] pricePerUnitTime = processRateInAmountPerTime(charges);

                        if (pricePerUnitTime != null) {

                            return constructCarparkChargeJSONObject(startEndTime[0], startEndTime[1], pricePerUnitTime[0], pricePerUnitTime[1], -2f, -2f, "pricePerUnitTime");
                        }
                    }

                    else if (charges.contains("sub")) { //If it is a more complex rate with subsequent charges e.g. $1.40 per 1st hr; $0.80 per sub 30 mins

                        Float[] pricePerUnitTimeWithSub = processRatesInAmountPerTimeWithSubCondition(charges);

                        if (pricePerUnitTimeWithSub != null) {

                            return constructCarparkChargeJSONObject(startEndTime[0], startEndTime[1], pricePerUnitTimeWithSub[0], pricePerUnitTimeWithSub[1], pricePerUnitTimeWithSub[2], pricePerUnitTimeWithSub[3], "pricePerUnitTime");
                        }
                    }

                    else { //Sample data: 7am-9.59pm: $0.036 per min/$2.16 per hr, 6am-6pm: $0.05 per min/$3 per hr, 8.30am-5pm: $1.20 per ½ hr (max $22.90)
//                        System.out.println("CURRENTLY PROCEs" + inputRateString);
//                        System.out.println("OTHERSSSSSSSSS" + inputRateString);
                    }
                }
            }

            else if (time.toLowerCase().trim().startsWith("daily")) {

                charges = charges.replace("/", " per ").replace("for", " per ").replaceAll("  ", " ");

                if (charges.contains("sub") == false) { //If it is a simple $X per X hr/min rate

                    Float[] pricePerUnitTime = processRateInAmountPerTime(charges);

                    if (pricePerUnitTime != null) {
                        return constructCarparkChargeJSONObject(-2f, -2f, pricePerUnitTime[0], pricePerUnitTime[1], -2f, -2f, "dailyPricePerUnitTime");
                    }
                }

                else { //Compute sub charges

                    Float[] pricePerUnitTimeWithSub = processRatesInAmountPerTimeWithSubCondition(charges);

                    if (pricePerUnitTimeWithSub != null) {
                        return constructCarparkChargeJSONObject(-2f, -2f, pricePerUnitTimeWithSub[0], pricePerUnitTimeWithSub[1], pricePerUnitTimeWithSub[2], pricePerUnitTimeWithSub[3], "dailyPricePerUnitTime");
                    }
                }
            }

            else if (time.toLowerCase().trim().startsWith("aft") && charges.toLowerCase().trim().contains("per entry")) { //Aft 5pm: $1 per entry
                float timeAfter = Float.parseFloat(time.replace("After", "").replace("Aft", "").replace("am", "").replace("pm", ""));

                if (time.contains("pm")) {
                    timeAfter += 12f;
                }

                String[] chargesTokenized = charges.split(" ");
                float perEntryRate = 0f;

                for (String curStr : chargesTokenized) {
                    if (curStr.startsWith("$")) {
                        perEntryRate = Float.parseFloat(curStr.replace("$", ""));
                    }
                }

                return constructCarparkChargeJSONObject(timeAfter, -2f, perEntryRate, -1f, -2f, -2f, "pricePerEntry");

            }

            else {
                //System.out.println("ERROR " + inputRateString);
            }

        }

        else {
//            System.out.println("NO colon Delimiter" + inputRateString);
        }

        return null;

    }


    //Construct the JSONObject to be output into a flat file given all the input attributes
    public JSONObject constructCarparkChargeJSONObject(float startTime, float endTime, float baseRate, float baseRateTimeUnitInMins, float subsequentRate, float subsequentRateTimeUnitInMins, String type) {

        JSONObject startEndTimeJsonObj = new JSONObject();
        if (startTime != -2f) startEndTimeJsonObj.put("startTime", startTime);
        if (endTime != -2f) startEndTimeJsonObj.put("endTime", endTime);

        JSONObject pricePerUnitTimeJsonObj = new JSONObject();
        if (baseRate != -2f) pricePerUnitTimeJsonObj.put("baseRate", baseRate);
        if (baseRateTimeUnitInMins != -2f) pricePerUnitTimeJsonObj.put("baseRateTimeUnitInMins", baseRateTimeUnitInMins);
        if (subsequentRate != -2f) pricePerUnitTimeJsonObj.put("subsequentRate", subsequentRate);
        if (subsequentRate != -2f) pricePerUnitTimeJsonObj.put("subsequentRateTimeUnitInMins", subsequentRateTimeUnitInMins);

        JSONObject returnJsonObject = new JSONObject();
        if (type.compareToIgnoreCase("dailyPricePerUnitTime") != 0 ) returnJsonObject.put("timing", startEndTimeJsonObj); //dailyPricePerUnitTime has no
        returnJsonObject.put(type, pricePerUnitTimeJsonObj);

        return returnJsonObject;
    }


    //Process parking rate in the form of "$1.40 per 1st hr; $0.80 per sub 30 mins"
    public Float[] processRatesInAmountPerTimeWithSubCondition(String inputRateString) {

        //Replace to standard units
        inputRateString = standardizeRateData(inputRateString);
        inputRateString = inputRateString.replace("sub.", "sub. ").replaceAll("  ", " ");
        inputRateString = inputRateString.replaceAll("1st 3 hrs", "1st 180mins").replaceAll("1st 2 hrs", "1st 120mins").replaceAll("1st 1½ hrs", "1st 90mins");
        inputRateString = inputRateString.replace("sub. hr", "sub. 60mins").replace("sub. min", "sub. 1mins");

        String[] inputRateStringTokenized = inputRateString.split(" ");

        //Look for pattern of $XXX --> 1st hour --> $XXX --> "sub" --> X hr
        int check = 0;
        ArrayList<String> attributeList = new ArrayList<String>();

        for (int i=0; i<inputRateStringTokenized.length; i++) {

            if ((check == 0 || check == 2) && inputRateStringTokenized[i].trim().startsWith("$")) {
                attributeList.add(inputRateStringTokenized[i]);
                check++;
            }

            else if (check == 1 && inputRateStringTokenized[i].trim().startsWith("1st")) { //Extract first ? hours
                if (i < inputRateStringTokenized.length-1) {
                    String firstNHours = inputRateStringTokenized[i+1].replace(";", "");
                    attributeList.add(firstNHours);
                    check++;
                    i++;
                }
            }

            else if (check == 3 && inputRateStringTokenized[i].trim().toLowerCase().startsWith("sub")) { //Extract subsequent ? minutes
                String subsequentUnitOfMeasurement = inputRateStringTokenized[i+1];

                if (subsequentUnitOfMeasurement.contains("mins")) { //Replace hours with 60 minutes
                    subsequentUnitOfMeasurement = subsequentUnitOfMeasurement.replace("mins", "");
                }
                attributeList.add(subsequentUnitOfMeasurement);
                break;
            }

            else if (check == 3 && inputRateStringTokenized[i].trim().toLowerCase().endsWith("mins")) { //If the word "sub" comes after
                String subsequentUnitOfMeasurement = inputRateStringTokenized[i].replace("mins", "");
                attributeList.add(subsequentUnitOfMeasurement);
                break;
            }
        }

        return processSearchedPatternForAmountPerTimeWithSubCondition(attributeList);
    }

    //Process searched pattern
    public Float[] processSearchedPatternForAmountPerTimeWithSubCondition(ArrayList<String> attributeList) {

        //Post process the extracted items if there are 4 elements extracted
        if (attributeList.size() == 4) {

            //Process the base rate
            Float baseRate = Float.parseFloat(attributeList.get(0).replace("$", "").trim());

            //Process the base rate time unit
            String baseRateTimeStr = replaceLastPunctuation(attributeList.get(1), ",");
            Float baseRateTime = 0f;

            if (baseRateTimeStr.contains("mins")) {
                baseRateTime = Float.parseFloat(baseRateTimeStr.replace("mins", "").trim());
            }

            else if (baseRateTimeStr.compareToIgnoreCase("hr") == 0) { //If is per 1 hour, convert to 60 mins
                baseRateTime = 60f;
            }

            else {
                return null;
            }

            //Process the subsequent rate
            String subsequentRateStr = attributeList.get(2).replace("$", "").replace("/min", "").trim();
            Float subsequentRate = Float.parseFloat(subsequentRateStr);


            //Process the subsequent base rate time unit
            Float subsequentRateTime = 0f;
            String subsequentBaseRateTimeStr = attributeList.get(3);

            if (subsequentBaseRateTimeStr.compareToIgnoreCase("hr") == 0) { //If is per 1 hour, convert to 60 mins
                subsequentRateTime = 60f;
            }

            else {
                subsequentRateTime = Float.parseFloat(attributeList.get(3).replaceAll("[^\\d.]", ""));
            }

            return new Float[]{baseRate, baseRateTime, subsequentRate, subsequentRateTime};
        }

        return null;
    }

    //Process parking rate in the form of "$1.50 per 30 mins"
    public Float[] processRateInAmountPerTime(String inputRateString) {

        //Replace to standard units
        inputRateString = standardizeRateData(inputRateString);

        //Replace min to mins for standardization also
        if (inputRateString.endsWith("min")) {
            inputRateString = inputRateString.replace("min", "mins");
        }

        String[] inputRateStringTokenized = inputRateString.split(" per ");

        float price= -1f;
        float rateBaseAmount = -1f;

        if (inputRateStringTokenized.length > 1) {
            if (inputRateStringTokenized[0].trim().startsWith("$")) {
                price = Float.parseFloat(inputRateStringTokenized[0].replace("$", "").replace("\u00A0", "").trim());
            }

            if (inputRateStringTokenized[1].trim().endsWith("mins")) {
                rateBaseAmount = Float.parseFloat(inputRateStringTokenized[1].replace("mins", "").replace("\u00A0", "").trim());
            }

            if (price >= 0f && rateBaseAmount >= 0f) {
                return new Float[]{price, rateBaseAmount};
            }
        }

        return null;
    }

    //Process time in the form of "12am-12pm"
    public Float[] processTimeInAmPmFormat(String inputTimeString) {

        inputTimeString = inputTimeString.replace("\u00A0", "").trim(); //Replace &nbsp and replace blank spaces
        inputTimeString = inputTimeString.replaceAll(" to ", "-"); //Some description uses the word " to " in replacement of the " - "
        String[] inputTimeStringTokenized = inputTimeString.split("-");

        if (inputTimeStringTokenized.length > 1 && inputTimeStringTokenized[0].contains("am") && inputTimeStringTokenized[1].contains("pm")) {
            float am = Float.parseFloat(inputTimeStringTokenized[0].replace("am", "").replace("\u00A0", "").trim());
            float pm = Float.parseFloat(inputTimeStringTokenized[1].replace("pm", "").replace("\u00A0", "").trim());
            float pm24Hours = pm + 12.0f;

            return new Float[]{am, pm24Hours};
        }

        return null;
    }

    //To perform some generic cleaning and data standardization on the carpark rates that is shared across different functions
    public String standardizeRateData (String inputRateString) {
        inputRateString = inputRateString.replace("1½hrs", "90mins").replace("1½ hrs", "90mins");
        inputRateString = inputRateString.replace("1½hr", "90mins");
        inputRateString = inputRateString.replace("½ hr", "30mins");
        inputRateString = inputRateString.replace("½ hr", "30mins");
        inputRateString = inputRateString.replace("1/2 hr" , "30mins");
        inputRateString = inputRateString.replace("2hrs", "120mins");
        inputRateString = inputRateString.replace("2-hrs", "120mins");
        inputRateString = inputRateString.replace("2 hrs", "120mins");
        inputRateString = inputRateString.replace("2 hr", "120mins");
        inputRateString = inputRateString.replace("3hrs", "180mins");
        inputRateString = inputRateString.replace(" 1hr", " 60mins");
        inputRateString = inputRateString.replace("per hr", "per 60mins");
        inputRateString = inputRateString.replace("per min", "per 1mins");
        inputRateString = inputRateString.replace("Free ", "$0 ");
        return inputRateString;
    }

    public String replaceLastPunctuation (String inputString, String punctuation) {

        if (inputString.endsWith(punctuation)) {
            StringBuilder b = new StringBuilder(inputString);
            b.replace(inputString.lastIndexOf(punctuation), inputString.lastIndexOf(punctuation) + 1, "" );
            return (b.toString());
        }

        return inputString;
    }

    //Write the JSON Objects to a physical file
    public boolean persistJSONObjectToFile(String carpark, String category, JSONObject[] inputJSONObjects) throws IOException {

        //Write the objects back into the file
        JSONObject carparkObject = new JSONObject();
        carparkObject.put("name", carpark);
        carparkObject.put("category", category);

        if (inputJSONObjects[0] != null) carparkObject.put("weekdays_rate_1", inputJSONObjects[0]);
        if (inputJSONObjects[1] != null) carparkObject.put("weekdays_rate_2", inputJSONObjects[1]);
        if (inputJSONObjects[2] != null) carparkObject.put("saturday_rate_1", inputJSONObjects[2]);
        if (inputJSONObjects[3] != null) carparkObject.put("saturday_rate_2", inputJSONObjects[3]);
        if (inputJSONObjects[4] != null) carparkObject.put("sunday_publicholiday_rate_1", inputJSONObjects[4]);
        if (inputJSONObjects[5] != null) carparkObject.put("sunday_publicholiday_rate_2", inputJSONObjects[5]);

        jsonWriter.write(carparkObject.toString());
        jsonWriter.newLine();

        return true;
    }

    //Check if the carpark rate description contains any amount
    public boolean removeDescriptionWithNoValues(String inputString) {

        if (inputString.toLowerCase().contains("free") == false && inputString.toLowerCase().contains("$") == false) {
            return true;
        }

        return false;
    }

    private void shutDown() {

        try {
            fileReader.close();
            jsonWriter.flush();
            jsonWriter.close();
            System.out.println("Application terminated gracefully.");
        }

        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
