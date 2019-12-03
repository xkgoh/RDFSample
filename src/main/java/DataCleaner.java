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
    private static boolean shutdown = false;

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
        application.shutDown();
        shutdown = true;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                if (shutdown == false) {
                    System.out.println("Executing shutdown hook...");
                    application.shutDown();
                }
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
    public void processAllRateDescriptions(String carpark, String category, String weekdaysRate1, String weekdaysRate2, String saturdayRate, String sundayPublicholidayRate) throws IOException {
//
//        System.out.println(carpark + " " + category + " [] " + weekdaysRate1 + " [] " + weekdaysRate2 + " [] " + saturdayRate + " [] " + sundayPublicholidayRate);

        //If weekday carpark rate does not contain any monetary value, i.e. its just description, ignore it.
        if (removeDescriptionWithNoValues(weekdaysRate1) == true) {
            System.out.println("    Skipping " + carpark + " with weekday_rate_1_description " + weekdaysRate1);
            persistJSONObjectToFile(carpark, category, new JSONObject[6]);
            return;
        }

        JSONObject weekdaysRate1Obj = parseIndividualRateDescription(weekdaysRate1);
        JSONObject weekdaysRate2Obj = parseIndividualRateDescription(weekdaysRate2);

        //If both objects are the same, discard one
        if ((weekdaysRate1Obj != null && weekdaysRate2Obj != null) && weekdaysRate1Obj.toString().compareToIgnoreCase(weekdaysRate2Obj.toString()) == 0) {
            weekdaysRate2Obj = null;
        }

        JSONObject saturdayRate1Obj, saturdayRate2Obj, sundayPublicholidayRate1Obj, sundayPublicholidayRate2Obj;

        //If saturday's rate is same as weekday, copy it
        if (saturdayRate.toLowerCase().contains("same")) {
            saturdayRate1Obj = weekdaysRate1Obj;
            saturdayRate2Obj = weekdaysRate2Obj;
        }

        else {
            saturdayRate1Obj = parseIndividualRateDescription(saturdayRate);
            saturdayRate2Obj = null;
        }

        //If sunday's rate is same as weekday, copy it
        if (sundayPublicholidayRate.toLowerCase().contains("same") && sundayPublicholidayRate.toLowerCase().contains("wkday")) {
            sundayPublicholidayRate1Obj = weekdaysRate1Obj;
            sundayPublicholidayRate2Obj = weekdaysRate2Obj;
        }

        //If sunday's rate is same as saturday, copy it
        else if (sundayPublicholidayRate.toLowerCase().contains("same") && sundayPublicholidayRate.toLowerCase().contains("saturday")) {
            sundayPublicholidayRate1Obj = saturdayRate1Obj;
            sundayPublicholidayRate2Obj = saturdayRate2Obj;
        }

        else {
            sundayPublicholidayRate1Obj = parseIndividualRateDescription(sundayPublicholidayRate);
            sundayPublicholidayRate2Obj = null;
        }

        JSONObject[] ratesJSONObjectArr = new JSONObject[6];
        ratesJSONObjectArr[0] = weekdaysRate1Obj;
        ratesJSONObjectArr[1] = weekdaysRate2Obj;
        ratesJSONObjectArr[2] = saturdayRate1Obj;
        ratesJSONObjectArr[3] = saturdayRate2Obj;
        ratesJSONObjectArr[4] = sundayPublicholidayRate1Obj;
        ratesJSONObjectArr[5] = sundayPublicholidayRate2Obj;

        persistJSONObjectToFile(carpark, category, ratesJSONObjectArr);

    }


    //Process the string representation of the parking rate
    public JSONObject parseIndividualRateDescription(String inputRateString) {

        if (inputRateString.length() < 2) {
            return null;
        }

        String[] inputRateStringTokenized = inputRateString.split(":");

        if (inputRateStringTokenized.length > 1) {

            //Process the Time constraint factor and its corresponding parking rate
            String time = inputRateStringTokenized[0];
            String charges = inputRateStringTokenized[1];

            if (time.toLowerCase().trim().startsWith("daily")) {

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

//            //Takes care of time which starts and ends at the same AM/PM e.g. 9am to 10am
//            if ((time.toLowerCase().contains("am") || time.toLowerCase().contains("pm")) && time.toLowerCase().contains("daily") == false && time.length() <=15) { //15 character because longest length is 12.00am-12.00pm
//
//            }


            else if ((time.toLowerCase().contains("am") || time.toLowerCase().contains("pm")) && time.length() <=15) {
//                    time.toLowerCase().contains("daily") == false && time.toLowerCase().contains("aft") == false) { //15 character because longest length is 12.00am-12.00pm

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


            else {
//                System.out.println("ERROR " + inputRateString + " (ERROR) " + time);
            }

        }

        else { //For items with no possibly no timing parameters (Cos they dont have the ":" delimiter)
            if (!inputRateString.contains("am ") && !inputRateString.contains("pm ") && inputRateString.length() > 2) {

                if (inputRateString.contains("per entry")) {
                    String[] chargesTokenized = inputRateString.split(" ");
                    float perEntryRate = 0f;
                    for (String curStr : chargesTokenized) {
                        if (curStr.startsWith("$")) {
                            perEntryRate = Float.parseFloat(curStr.replace("$", ""));
                        }
                    }

                    return constructCarparkChargeJSONObject(0f, 23.59f, perEntryRate, -1f, -2f, -2f, "pricePerEntry");
                }
            }

            else if (inputRateString.contains(" - ")) { //6.30am to 6.30pm - $2 per hour
                inputRateStringTokenized = inputRateString.split(" - ");
                if (inputRateStringTokenized[0].contains("am") && inputRateStringTokenized[0].contains("pm")) {

                    Float[] startEndTime = processTimeInAmPmFormat(inputRateStringTokenized[0]);
                    Float[] pricePerUnitTime = null;

                    if (inputRateStringTokenized[1].contains("sub")) {
                        pricePerUnitTime = processRatesInAmountPerTimeWithSubCondition(inputRateStringTokenized[1]);
                    }

                    if ((pricePerUnitTime != null) && (startEndTime != null) ) {
                        return constructCarparkChargeJSONObject(startEndTime[0], startEndTime[1], pricePerUnitTime[0], pricePerUnitTime[1], pricePerUnitTime[2], pricePerUnitTime[3], "pricePerUnitTime");
                    }
                }

                else if (inputRateStringTokenized[0].contains("After")) { //After 4pm - $4 flat
                    Float timeAfter = Float.parseFloat(inputRateStringTokenized[0].replace("After", "").replace("Aft", "").replace("am", "").replace("pm", ""));
                    Float perEntryRate = Float.parseFloat(inputRateStringTokenized[1].replace(" flat", "").replace("$", ""));
                    return constructCarparkChargeJSONObject(timeAfter, -2f, perEntryRate, -1f, -2f, -2f, "pricePerEntry");
                }
            }

            else {
                if (inputRateString.contains("sub")) {
                    Float[] pricePerUnitTime = processRatesInAmountPerTimeWithSubCondition(inputRateString);
                    if (pricePerUnitTime!= null) {
                        return constructCarparkChargeJSONObject(0f, 23.59f, pricePerUnitTime[0], pricePerUnitTime[1], pricePerUnitTime[2], pricePerUnitTime[3], "pricePerUnitTime");
                    }
                }
            }
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

        if (inputTimeStringTokenized.length > 1) {

            float startTime = 0f;
            float endTime = 0f;

            if (inputTimeStringTokenized[0].contains("am")) {
                startTime = Float.parseFloat(inputTimeStringTokenized[0].replace("am", "").replace("\u00A0", "").trim());
            }

            else if (inputTimeStringTokenized[0].contains("pm")) {
                startTime = Float.parseFloat(inputTimeStringTokenized[0].replace("pm", "").replace("\u00A0", "").trim());
                startTime = startTime + 12.0f;
            }

            if (inputTimeStringTokenized[1].contains("am")) {
                endTime = Float.parseFloat(inputTimeStringTokenized[1].replace("am", "").replace("\u00A0", "").trim());
            }

            else if (inputTimeStringTokenized[1].contains("pm")) {
                endTime = Float.parseFloat(inputTimeStringTokenized[1].replace("pm", "").replace("\u00A0", "").trim());
                endTime = endTime + 12.0f;
            }



//            if (inputTimeStringTokenized[0].contains("am") && inputTimeStringTokenized[1].contains("pm")) {
//                float am = Float.parseFloat(inputTimeStringTokenized[0].replace("am", "").replace("\u00A0", "").trim());
//                float pm = Float.parseFloat(inputTimeStringTokenized[1].replace("pm", "").replace("\u00A0", "").trim());
//                float pm24Hours = pm + 12.0f;
//
//                return new Float[]{am, pm24Hours};
//            }

            //If there are two AM


            //If there are two PMs

            //Return the value
//            float pm24Hours = pm + 12.0f;
            return new Float[]{startTime, endTime};
        }

        return null;
    }

    //To perform some generic cleaning and data standardization on the carpark rates that is shared across different functions
    public String standardizeRateData (String inputRateString) {
        inputRateString = inputRateString.replace("1½hrs", "90mins").replace("1½ hrs", "90mins");
        inputRateString = inputRateString.replace("1½hr", "90mins");
        inputRateString = inputRateString.replace("2½ hrs", "150mins").replace("2½hrs", "150mins");
        inputRateString = inputRateString.replace("½ hour", "30mins");
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

            try {
                while(true) jsonWriter.flush();
            }

            catch (IOException e) {
                System.out.println("Application terminated gracefully.");
            }

        }

        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
