import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;

import java.io.IOException;
import java.util.Properties;

public class DataRetrieval {

    private static HTTPRepository repositoryManager;
    private static RepositoryConnection repositoryConnection;

    //Default constructor to initialize database
    public DataRetrieval() throws IOException {

        Properties prop = new Properties();
        prop.load(DataRetrieval.class.getClassLoader().getResourceAsStream("config.properties"));

        String databaseURL = prop.get("graphdb.url").toString();

        //Initialize connection to database
        repositoryManager = new HTTPRepository(databaseURL);
        repositoryConnection = repositoryManager.getConnection();

        System.out.println("Connected to database at " + databaseURL);

    }

    public static void main (String[] args) throws Exception{

        DataRetrieval application = new DataRetrieval();
        application.getCategoryWithMostCarpark();
        application.getMaxAverageRateForEachRegion("Saturday", "cdit:hasSaturdayCarparkCharges", "18");
//        application.getMaxAverageRateForEachRegion("Weekday", "cdit:hasWeekdayCarparkCharges", "18");
//        application.getMaxAverageRateForEachRegion("Sunday", "cdit:hasSundayCarparkCharges", "18");

    }

    public void getMaxAverageRateForEachRegion(String dayOfWeek, String rateRelationshipIRI, String curTimeInDecimal) {

        String queryString =
                "PREFIX cdit:<http://cdit#> " +

                "SELECT DISTINCT ?carparkName1 ?maxRate ?locationCategoryIRI " +
                "WHERE { " +
                    "{ " +
                        //Subquery to "join" the name of the carpark back to the extracted max value
                        "SELECT (ABS(?baseRate1/?baseRateTimeUnitInMins1) AS ?avgRate) ?carparkName1 ?maxRate ?locationCategoryIRI " +
                        "WHERE { " +
                            "?carparkIRI1 " + rateRelationshipIRI + "?carparkChargeIRI1. " +
                            "?carparkIRI1 cdit:name ?carparkName1. " +
                            "?carparkChargeIRI1 cdit:baseRate ?baseRate1. " +
                            "?carparkChargeIRI1 cdit:baseRateTimeUnitInMins ?baseRateTimeUnitInMins1. " +

                            "{ " +
                                //Subquery to extract the max for each category
                                "SELECT (MAX(ABS(?baseRate/?baseRateTimeUnitInMins)) AS ?maxRate) ?locationCategoryIRI  " +
                                "WHERE { " +
                                    "{ " +
                                        //Looks for a pattern where only the start time satisfies the time constraint, if only the start time is present
                                        "?carparkIRI a cdit:Carpark. " +
                                        "?carparkIRI " + rateRelationshipIRI + " ?carparkChargeIRI. " +
                                        "?carparkIRI cdit:hasLocationCategory ?locationCategoryIRI. " +
                                        "?carparkChargeIRI cdit:startTime ?startTime. " +
                                        "FILTER NOT EXISTS {?carparkChargeIRI cdit:endTime ?endTime.} " +
                                        "FILTER (?startTime <= 18) " +
                                        "?carparkChargeIRI cdit:baseRate ?baseRate. " +
                                        "?carparkChargeIRI cdit:baseRateTimeUnitInMins ?baseRateTimeUnitInMins. " +
                                    "} " +
                                    "UNION " +
                                    "{ "+
                                        //#Looks for a pattern where BOTH the start & end time satisfies the time constraint, if only the start & end time is present
                                        "?carparkIRI a cdit:Carpark. " + //For performance reasons. Selecting this will make the query fast. (Select than filter)
                                        "?carparkIRI " + rateRelationshipIRI + " ?carparkChargeIRI. " +
                                        "?carparkIRI cdit:hasLocationCategory ?locationCategoryIRI. " +
                                        "?carparkChargeIRI cdit:startTime ?startTime. " +
                                        "?carparkChargeIRI cdit:endTime ?endTime. " +
                                        "FILTER (?endTime >= 18 && ?startTime <= 18) " +
                                        "?carparkChargeIRI cdit:baseRate ?baseRate. " +
                                        "?carparkChargeIRI cdit:baseRateTimeUnitInMins ?baseRateTimeUnitInMins. " +
                                    "} " +
                                "} " +
                                "GROUP BY ?locationCategoryIRI " +
                            "} " +
                        "} " +
                    "} " +
                    //Filter where the average rate is the max rate obtained from the subquery
                    "FILTER (?avgRate = ?maxRate) " +
                "} " +
                "ORDER BY (?locationCategoryIRI) ";

        TupleQuery query = repositoryConnection.prepareTupleQuery(queryString);

        TupleQueryResult result = query.evaluate();

        System.out.println("--> Highest " + dayOfWeek + " Rate for Each Category:");

        while (result.hasNext()) {
            BindingSet solution = result.next();
            System.out.println("Carpark Name: " + solution.getValue("carparkName1").stringValue() + ". Max Rate: " + solution.getValue("maxRate").stringValue() + ". Location Category: " + solution.getValue("locationCategoryIRI").stringValue());
        }
    }


    public void getCategoryWithMostCarpark (){

        String queryString = "PREFIX cdit:<http://cdit#> " +
                "SELECT ((COUNT(?carpark)) as ?count) ?name " +
                "WHERE { " +
                    "?locationCategory cdit:isInLocationCategory ?carpark. " +
                    "?locationCategory cdit:name ?name. " +
                "} " +
                "GROUP BY ?name " +
                "ORDER BY DESC (?count) " +
                "LIMIT 1 ";

        TupleQuery query = repositoryConnection.prepareTupleQuery(queryString);

        TupleQueryResult result = query.evaluate();

        while (result.hasNext()) {
            BindingSet solution = result.next();
            System.out.println("--> Category With Most Carparks:");
            System.out.println("No of carpark: " + solution.getValue("count").stringValue());
            System.out.println("Location: " + solution.getValue("name").stringValue());
        }

    }













}
