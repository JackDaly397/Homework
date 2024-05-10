## Homework 2

# Cleaning and Transforming Data

To clean and transform my data, I went through the process of extracting the data out of my Azure blob container and working on it within my Jupyter notebook. This included removing any NULL values or rows containing "S" values as this createdy incomplete instances. Additionally, I updated the values of my percentage columns (from 90 to .90) as well as changed the data types of multiple columns. Once this was complete I began mapping the dimension tables. For example, with mapping my dim_grade table, the grade id's 1, 2, 3 will now associate with grades 4, 5, and 6 within the data set. By mapping these values I can ensure better data integrity and more consistent results. 
