This project ranks the airport based on the number of flights landing in the airport from different airports. 

We are given a dataset containing the source and destination airport id for the different flights and we are constructing a spark dataframe and then using it to calculate the page rank of each airport. 

This project uses map-reduce to group the airports on the destination airport ids, calculate the page rank and then sort them based on their pagerank scores. 

This was completely coded in IntelliJ IDEA and executed using the AWS Elastic Map Reduce cluster. 

Dataset :  https://assignment2-kxn180028.s3.amazonaws.com/ONTIME_REPORTING.csv

EC2 EMR parameters : 
	--class "PageRank" 
	s3://assignment2-kxn180028/airportpagerank_2.11-0.1.jar 
	s3://assignment2-kxn180028/ONTIME_REPORTING.csv 
	15 
	s3://assignment2-kxn180028/OutputQ1
