# RDFSample
cdit assignment on data engineering

## Objectives: ##
1. Design a database used to contain Carpark Rate Data.
2. Build an ETL pipeline to ingest the dataset.
3. Query the dataset to find the most number of carparks per category, and the most expesive rate for each time category for each category.

## Installation Instructions ##
Before executing a copy of the code, install a copy of graphdb:
1. Download a copy of graphdb off the following link: https://www.ontotext.com/products/graphdb/graphdb-free/
2. Follow the desktop installation instructions at http://graphdb.ontotext.com/documentation/8.7/free/quick-start-guide.html
3. Follow any on screen prompts that appear to install the database.
4. Once installation is completed, start the database by clicking on the application icon and open up the workbench in a browser at http://localhost:7200/
5. On the menu on the left, select SETUP followed by REPOSITORIES
6. After the Repositories page finish loading, click "Create new repository" (Orange button on the screen)
7. Fill in the following details, leaving the rest unchanged:
 {"Repository ID": "TestData", "Repository Title": "TestData", "Base URL": "http://cdit#"}
8. Click Create
9. Connect the repository by clicking on the "little plug icon" on the left hand side of the newly created repository.
10. Obtain the repository URL by clicking on the "little chain icon" on the right hand side of the newly created repository.

Next set the configuration file which can be found in the resources folder
1. Set graphdb.url to the repository URL in Step 10 (above) (If localhost does not work)
2. Set cleanedoutput.filename to any location to store the cleaned file to be used for ingestion (This parameter must be modified to point to a directory in your pc)
3. Set graphdb.batchsize to the batch size for the number of records to be read before doing a bulk insertion

## Code Description ##
1. DataCleaner.java: Incharge of processing the raw CSV file downloaded from https://data.gov.sg/dataset/carpark-rates. It contains all the cleaning rules to process the data to a structured form.
2. KBController.java: KnowledgeBase controller incharge of reading from the cleaned file, transforming the data into appropriate Graph Tuples and store them in an in-memory data structure, before inserting into the database in batches.
3. Data Retrieval: Executes SPARQL queries to retrieve the two queries described in the objectives.

## Execution Instruction ##
Each java file can be executed individually by running the main methods within it. To run the entire ETL pipeline, simply run ApplicationDemo.java which executes the individual java files in the correct ETL sequence. 

Alternatively, execute each java class in the same sequence as in the Code Description section. The output of the 2 queries in the objective will be printed on the console.

## Computation of Most Expensive Rate ##
For this project, the most expensive rate (i.e. maximum rate), is defined as the highest cost per minute at a particular time (Default 8pm). 

E.g. At 6pm, Carpark A charges $7/entry and Carpark B charges $1.50/hr. Since we have no details on the duration of parking, rate of Carpark A is assumed to be $7/min and Carpark B to be $0.025/min.

## Thank you :) ##
    
