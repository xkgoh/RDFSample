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
4. Once installation is compeleted, start the database by clicking on the application icon and open the workbench at http://localhost:7200/
5. On the menu on the left, select SETUP followed by REPOSITORIES
6. After the Repositories page finish loading, click "Create new repository" (Orange button on the screen)
7. Fill in the following details, leaving the rest unchanged:
 - Repository ID: TestData
 - Repository Title: TestData
 - Base URL: http://cdit#
8. Click Create
9. Connect the repository by clicking on the "little plug icon" on the left hand side of the newly created repository.
