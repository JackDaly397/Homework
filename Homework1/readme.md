<span style="font-size:0.5em;">Homework 1 </span>
##NYC School Attendance Data 2013-2019

###About the Data

#Data: 
https://data.cityofnewyork.us/Education/2013-2019-Attendance-Results-School/vww9-qguh/about_data

#Link to data dictionary:
https://data.cityofnewyork.us/api/views/vww9-qguh/files/c638864d-7cbd-449c-9920-b4b374898e21?download=true&filename=2014-2019%20Attendance%20DD%20-%20School.xlsx

The data used in this pipeline is provided by NYC Open Data. It contains attendance results for students in public schools, charter schools, and home schooling across NYC. The data is broken down into multiple demographic variables and provides important information on the attendance records of New York's next generation.

##Extraction and Storage
#Storage of Choice
Azure Blob Container

To extract the data from NYC Open Data, I had to use Socrata to connect to the API and pull in the data through chunks. Once this data was pulled into my Jupyter Notebook, I was able to upload it into my blob container in Azure.

##Data Modeling

Once the data was uploaded into my blob container, I worked on the dimensional modeling of the tables in DB Schema. Within this dimensional model I created a star schema with my attendance metrics in the Facts table, such as number of day presents and number of days absent, and then separated the remaining dimensions into their own tables, such as time, school, and demographic variable.