public class ApplicationDemo {

    public static void main(String[] args) throws Exception {

        //Executes the data cleaning pipe
        DataCleaner.main(null);

        //Transform and load the data into the KB
        KBController.main(null);

        //Retrieve the data for the two given queries
        DataRetrieval.main(null);
    }
}
