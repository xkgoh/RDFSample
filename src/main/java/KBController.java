import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.json.JSONObject;

import java.io.*;
import java.util.Properties;
import java.util.UUID;

public class KBController {

    private int BATCH_SIZE = 1;

    private HTTPRepository repositoryManager;
    private RepositoryConnection repositoryConnection;

    private static ValueFactory vf = SimpleValueFactory.getInstance();

    //Base URL
    private String base = "http://cdit#";

    //List of Global IRIs

    //List of entities
    private IRI carparkEntity = vf.createIRI(base, "Carpark");
    private IRI locationCategoryEntity = vf.createIRI(base, "LocationCategory");
    private IRI carparkChargeEntity = vf.createIRI(base, "CarparkCharge");
    private IRI carparkChargeTypeEntity = vf.createIRI(base, "CarparkChargeType");


    //List of global attribute (Entity(IRI) to literal)
    private IRI nameAttribute = vf.createIRI(base, "name");
    private IRI startTimeAttribute = vf.createIRI(base, "startTime");
    private IRI endTimeAttribute = vf.createIRI(base, "endTime");
    private IRI baseRateAttribute = vf.createIRI(base, "baseRate");
    private IRI baseRateTimeUnitInMinsAttribute = vf.createIRI(base, "baseRateTimeUnitInMins");
    private IRI subsequentRateAttribute = vf.createIRI(base, "subsequentRate");
    private IRI subsequentRateTimeUnitInMinsAttribute = vf.createIRI(base, "subsequentRateTimeUnitInMins");

    //List of global realationships (Entity(IRI) to entity(IRI))
    private IRI hasLocationCategory = vf.createIRI(base, "hasLocationCategory");
    private IRI isInLocationCategory = vf.createIRI(base, "isInLocationCategory");

    private IRI hasWeekdayCarparkCharges = vf.createIRI(base, "hasWeekdayCarparkCharges"); //Domain: CarparkEntity, Range: CarparkChargeEntity
    private IRI isChargedByCarparkOnWeekday = vf.createIRI(base, "isChargedByCarparkOnWeekday"); //Inverse of hasCarparkCharges

    private IRI hasSaturdayCarparkCharges = vf.createIRI(base, "hasSaturdayCarparkCharges"); //Domain: CarparkEntity, Range: CarparkChargeEntity
    private IRI isChargedByCarparkOnSaturday = vf.createIRI(base, "isChargedByCarparkOnSaturday"); //Inverse of hasCarparkCharges

    private IRI hasSundayCarparkCharges = vf.createIRI(base, "hasSundayCarparkCharges"); //Domain: CarparkEntity, Range: CarparkChargeEntity
    private IRI isChargedByCarparkOnSunday = vf.createIRI(base, "isChargedByCarparkOnSunday"); //Inverse of hasCarparkCharges

    private IRI hasCarparkChargeType = vf.createIRI(base, "hasCarparkChargeType"); //Domain: CarparkChargeEntity, Range: CarparkChargeType
    private IRI isChargedTypeOfCarpark = vf.createIRI(base, "isChargedByCarpark"); //Inverse of hasCarparkChargeType

    private BufferedReader jsonReader;

    //Default constructor
    public KBController() {

        try {

            System.out.println("Starting data transformation and ingestion process..");

            Properties prop = new Properties();
            prop.load(DataRetrieval.class.getClassLoader().getResourceAsStream("config.properties"));

            BATCH_SIZE = Integer.parseInt(prop.get("graphdb.batchsize").toString());

            //Initialize connection to database
            String databaseURL = prop.get("graphdb.url").toString();
            repositoryManager = new HTTPRepository(databaseURL);
            repositoryConnection = repositoryManager.getConnection();

            String cleanedJSONFileLocation = prop.get("cleanedoutput.filename").toString();
            jsonReader = new BufferedReader(new FileReader(cleanedJSONFileLocation));

            System.out.println("Database connected.");
        }

        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main (String[] args) {

        KBController application = new KBController();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Executing shutdown hook...");
                application.shutDown();
            }
        });

        application.readJCleandedSONFile();

    }

    public void readJCleandedSONFile() {

        try {

            String curLine;
            int counter = 0;
            Model model = new TreeModel(); //Create new inmem database to store cache

            while ((curLine = jsonReader.readLine()) != null) {

                JSONObject curCarpark = new JSONObject(curLine);
                model = processCarparkJSONObject(curCarpark, model);

                if (counter > BATCH_SIZE) {
                    writeToDatabase(model); //Flush to database
                    model = new TreeModel(); //Reinitialize the model
                    counter = 0; //Reset counter
                }

                counter++;
            }

            if (!model.isEmpty()) {
                writeToDatabase(model); //Flush to database
            }
        }

        catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Map the JSON object to the graph schema
    public Model processCarparkJSONObject(JSONObject inputJsonObj, Model model) {

        if (!inputJsonObj.has("name") || !inputJsonObj.has("category")) {
            return model;
        }

        System.out.println("    Currently processing: "+inputJsonObj.get("name"));

        //Setting the carpark entity and attributes
        IRI curCarPark = vf.createIRI(base, inputJsonObj.get("name").toString().replaceAll(" ", ""));
        model.add(curCarPark, RDF.TYPE, carparkEntity);
        model.add(curCarPark, nameAttribute, vf.createLiteral(inputJsonObj.get("name").toString()));

        //Setting the location entity and attribute
        IRI curlocationCategory = vf.createIRI(base, inputJsonObj.get("category").toString().replaceAll(" ", ""));
        model.add(curlocationCategory, RDF.TYPE, locationCategoryEntity);
        model.add(curlocationCategory, nameAttribute, vf.createLiteral(inputJsonObj.get("category").toString()));

        //Adding the link between carpark entity and location entity
        model.add(curCarPark, hasLocationCategory, curlocationCategory);
        model.add(curlocationCategory, isInLocationCategory, curCarPark);

        model = checkRateAvailabilityAndProcessRate(curCarPark, inputJsonObj, model);

        return model;

    }

    //Check the types of parking rates available in the JSON object, and execute the corresponding function to process it
    public Model checkRateAvailabilityAndProcessRate(IRI carparkEntity, JSONObject carparkJsonObject, Model model) {

        if (carparkJsonObject.has("weekdays_rate_1")) {
            JSONObject jsonObj = carparkJsonObject.getJSONObject("weekdays_rate_1");
            model = processCarparkChargesType(carparkEntity, jsonObj, model, "weekday");
        }

        if (carparkJsonObject.has("weekdays_rate_2")) {
            JSONObject jsonObj = carparkJsonObject.getJSONObject("weekdays_rate_2");
            model = processCarparkChargesType(carparkEntity, jsonObj, model,"weekday");
        }

        if (carparkJsonObject.has("saturday_rate_1")) {
            JSONObject jsonObj = carparkJsonObject.getJSONObject("saturday_rate_1");
            model = processCarparkChargesType(carparkEntity, jsonObj, model,"saturday");
        }

        if (carparkJsonObject.has("saturday_rate_2")) {
            JSONObject jsonObj = carparkJsonObject.getJSONObject("saturday_rate_2");
            model = processCarparkChargesType(carparkEntity, jsonObj, model,"saturday");
        }

        if (carparkJsonObject.has("sunday_publicholiday_rate_1")) {
            JSONObject jsonObj = carparkJsonObject.getJSONObject("sunday_publicholiday_rate_1");
            model = processCarparkChargesType(carparkEntity, jsonObj, model,"sunday");
        }

        if (carparkJsonObject.has("sunday_publicholiday_rate_2")) {
            JSONObject jsonObj = carparkJsonObject.getJSONObject("sunday_publicholiday_rate_2");
            model = processCarparkChargesType(carparkEntity, jsonObj, model,"sunday");
        }

        return model;
    }

    //Map the details of the parking rates to the graph schema
    public Model processCarparkChargesType(IRI carparkEntityIRI, JSONObject carparkChargesJsonObj, Model model, String dayOfWeek) {

        //Generate a unique identifier to bind the information together
        UUID uuid = UUID.randomUUID();
        IRI carparkChargeEntityIRI = vf.createIRI(base, uuid.toString());
        model.add(carparkChargeEntityIRI, RDF.TYPE, carparkChargeEntity); //Allows us to quickly identify if a node with a UUID is of a carparkCharge or not. UUID can be used to bind other types of info.

        //Process per-entry type of parking
        if (carparkChargesJsonObj.has("pricePerEntry") && carparkChargesJsonObj.has("timing")) {

            IRI pricePerEntryChargeTypeEntity = vf.createIRI(base, "PricePerEntry");

            model.add(pricePerEntryChargeTypeEntity, RDF.TYPE, carparkChargeTypeEntity);
            model.add(carparkChargeEntityIRI, hasCarparkChargeType, pricePerEntryChargeTypeEntity);
            model.add(pricePerEntryChargeTypeEntity, isChargedTypeOfCarpark, carparkChargeEntityIRI);

            JSONObject timingDetails = carparkChargesJsonObj.getJSONObject("timing");
            JSONObject rateDetails = carparkChargesJsonObj.getJSONObject("pricePerEntry");

            //Bind the charges and timing details to the unique identifier
            model = addTimeDetailsToModel(carparkChargeEntityIRI, timingDetails, model);
            model = addRateDetailsToModel(carparkChargeEntityIRI, rateDetails, model);
        }

        else if (carparkChargesJsonObj.has("pricePerUnitTime") && carparkChargesJsonObj.has("timing")) {

            IRI pricePerUnitTimeChargeTypeEntityIRI = vf.createIRI(base, "PricePerUnitTime");

            model.add(pricePerUnitTimeChargeTypeEntityIRI, RDF.TYPE, carparkChargeTypeEntity);
            model.add(carparkChargeEntityIRI, hasCarparkChargeType, pricePerUnitTimeChargeTypeEntityIRI);
            model.add(pricePerUnitTimeChargeTypeEntityIRI, isChargedTypeOfCarpark, carparkChargeEntityIRI);

            JSONObject timingDetails = carparkChargesJsonObj.getJSONObject("timing");
            JSONObject rateDetails = carparkChargesJsonObj.getJSONObject("pricePerUnitTime");

            //Bind the charges and timing details to the unique identifier
            model = addTimeDetailsToModel(carparkChargeEntityIRI, timingDetails, model);
            model = addRateDetailsToModel(carparkChargeEntityIRI, rateDetails, model);
        }

        else if (carparkChargesJsonObj.has("dailyPricePerUnitTime")) {

            IRI dailyPricePerUnitTimeChargeTypeEntityIRI = vf.createIRI(base, "DailyPricePerUnitTime");

            model.add(dailyPricePerUnitTimeChargeTypeEntityIRI, RDF.TYPE, carparkChargeTypeEntity);
            model.add(carparkChargeEntityIRI, hasCarparkChargeType, dailyPricePerUnitTimeChargeTypeEntityIRI);
            model.add(dailyPricePerUnitTimeChargeTypeEntityIRI, isChargedTypeOfCarpark, carparkChargeEntityIRI);

            JSONObject rateDetails = carparkChargesJsonObj.getJSONObject("dailyPricePerUnitTime");

            //For daily rate, assume it is same rate throughout the day, 7 days a week
            model.add(carparkChargeEntityIRI, startTimeAttribute, vf.createLiteral(0f));
            model.add(carparkChargeEntityIRI, endTimeAttribute, vf.createLiteral(24.59f));

            model = addRateDetailsToModel(carparkChargeEntityIRI, rateDetails, model);
        }

        //Link the carpark charges IRI back to the carpark entity IRI
        model = addCarparkEntityToCharparkChargeEntityLink(model, carparkEntityIRI, carparkChargeEntityIRI, dayOfWeek);


        return model;
    }

    //Check for the existance of time attribute and add it to the model
    public Model addTimeDetailsToModel(IRI carparkChargeEntityIRI, JSONObject timingDetails, Model model) {

        if (timingDetails.has("startTime")) {
            model.add(carparkChargeEntityIRI, startTimeAttribute, vf.createLiteral(timingDetails.getFloat("startTime")));
        }

        if (timingDetails.has("endTime")) {
            model.add(carparkChargeEntityIRI, endTimeAttribute, vf.createLiteral(timingDetails.getFloat("endTime")));
        }

        return model;
    }

    //Check for the existance of rate attribute and add it to the model
    public Model addRateDetailsToModel(IRI carparkChargeEntityIRI, JSONObject rateDetails, Model model) {

        if (rateDetails.has("baseRate")) {
            model.add(carparkChargeEntityIRI, baseRateAttribute, vf.createLiteral(rateDetails.getFloat("baseRate")));
        }

        if (rateDetails.has("baseRateTimeUnitInMins")) {
            model.add(carparkChargeEntityIRI, baseRateTimeUnitInMinsAttribute, vf.createLiteral(rateDetails.getFloat("baseRateTimeUnitInMins")));
        }

        if (rateDetails.has("subsequentRate")) {
            model.add(carparkChargeEntityIRI, subsequentRateAttribute, vf.createLiteral(rateDetails.getFloat("subsequentRate")));
        }

        if (rateDetails.has("subsequentRateTimeUnitInMins")) {
            model.add(carparkChargeEntityIRI, subsequentRateTimeUnitInMinsAttribute, vf.createLiteral(rateDetails.getFloat("subsequentRateTimeUnitInMins")));
        }

        return model;
    }

    //Add the linkages between the carpark entity and the carpark charge entity
    public Model addCarparkEntityToCharparkChargeEntityLink(Model model, IRI carparkEntityIRI, IRI carparkChargeEntityIRI, String dayOfWeek) {

        if (dayOfWeek.compareToIgnoreCase("weekday") == 0) {
            model.add(carparkEntityIRI, hasWeekdayCarparkCharges, carparkChargeEntityIRI);
            model.add(carparkChargeEntityIRI, isChargedByCarparkOnWeekday, carparkEntityIRI);
        }

        else if (dayOfWeek.compareToIgnoreCase("saturday") == 0) {
            model.add(carparkEntityIRI, hasSaturdayCarparkCharges, carparkChargeEntityIRI);
            model.add(carparkChargeEntityIRI, isChargedByCarparkOnSaturday, carparkEntityIRI);
        }

        else if (dayOfWeek.compareToIgnoreCase("sunday") == 0) {
            model.add(carparkEntityIRI, hasSundayCarparkCharges, carparkChargeEntityIRI);
            model.add(carparkChargeEntityIRI, isChargedByCarparkOnSunday, carparkEntityIRI);
        }

        return model;
    }

    //Write a model to the database
    public void writeToDatabase(Model model) {
        repositoryConnection.add(model);
        System.out.println("Model added to graphdb.");
    }

    private void shutDown() {
        repositoryConnection.close();
        System.out.println("Connection Terminated.");
    }

}
