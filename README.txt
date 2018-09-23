Please read this file to do the setup to run the code.

1. LIBRARY DEPENDENCY 

   The code needs following libraries to run :
    a. SPARK - 2.3.0
    b. SCALA - 2.11
    c. Java  - 1.8 

2. The code has been developed & run on local machine. Thus, while creating SparkSession object, the URL of the master has been set to 'LOCAL'.
   To run the code in a cluster, please set the URL of the master in SparkSession's master().

3. The input/output file paths have been hard coded considering the setup on local machine, to keep the code simple. But these paths could be changed as per the case. 
   Please substitute these paths appropriately to run the code as per environment in which it is supposed to run. 
   Currently it is considered that the project has a folder 'InputFiles' from where input files will be read and has a folder named 'OutputFolder' in which output         files will be created. 

4. Code has been designed with try-catch block to print if there is any issue with the file paths.   

5. Considering the behaviour of Spark's write method, it creates multiple part files inside the output folder. Code has been designed to create a single part file         inside output folder path.

 
                    	 