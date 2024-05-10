## Homework 2

# Cleaning and Transforming Data

To clean and transform my data, I went through the process of extracting the data out of my Azure blob container and working on it within my Jupyter notebook. This included removing any NULL values or rows containing "S" values as this createdy incomplete instances. Additionally, I updated the values of my percentage columns (from 90 to .90) as well as changed the data types of multiple columns. Once this was complete I began mapping the dimension tables. For example, with mapping my dim_grade table, the grade id's 1, 2, 3 will now associate with grades 4, 5, and 6 within the data set. By mapping these values I can ensure better data integrity and more consistent results. 

# Loading Data
## Datawarehouse Used: Azure Database for PostgreSQL

Once the data was cleaned, transformed, and mapped, I went through the process of loading the data into my Postgres datawarehouse. This process began by running my dimensional modeling SQL script within DataGrip to create the tables within my Postgres DW. Once these tables were created in the DW, I went back to my Jupyter notebook to push the data I was transforming into their respective tables within Postgres.
