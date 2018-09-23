package ubs.assignment.src 

//imports
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.io.FileNotFoundException
import org.apache.spark.sql.AnalysisException

// Scala Object definition
object UBS_Assignment {
  
  //main function definition
  def main(args : Array[String]) {
             
      // creating spark session object 
      val session = SparkSession
                    .builder()
                    .appName("UBS_Assignment")
                    .master("local[*]")   // Replace local with Master's URL
                    .getOrCreate()
      
     import session.implicits._               
                    
     try {
                 // Creating DataFrame of 'Start of day positions of instruments' by reading input CSV file 
                 val InputStartOfDayPositionDF = session.read.format("csv").option("header","true").option("mode","DROPMALFORMED").load("..//UBS_Assignment//InputFiles//Input_StartOfDay_Positions.txt")               
                 // The input/output file paths could be changed as per the case. Currently they are hard-coded. These can also be read from properties file
                 
                 // Creating DataFrame of 'Transactions on instruments' by reading input JSON file of transactions 
                 val InputTransactionsDF = session.read.option("multiline","true").option("mode","DROPMALFORMED").json("..//UBS_Assignment//InputFiles//Input_Transactions.txt") 
                 // The input/output file paths could be changed as per the case. Currently they are hard-coded. These can also be read from properties file
                 
                 /* 
                  Inner joining InputStartOfDayPosition DataFrame with InputTransactions DataFrame to find out the
                  pair of TractionTypes & AccountType which results in 'ADDITION' of TransactionQuantity to the input start of day quantity for the instruments
                  and setting the TransactionQuantity as 'Positive' 
                 */
                  val TransactionsInvolvingQuantityAddition = InputStartOfDayPositionDF
                                                             .join(InputTransactionsDF
                                                              ,InputStartOfDayPositionDF("Instrument") === InputTransactionsDF("Instrument")
                                                                && (
                                                                    (InputTransactionsDF("TransactionType") === "B" && InputStartOfDayPositionDF("AccountType")==="E")
                                                                    ||
                                                                    (InputTransactionsDF("TransactionType") === "S" && InputStartOfDayPositionDF("AccountType")==="I")                                                            
                                                                )
                                                              ,"inner")
                                                             .select(InputStartOfDayPositionDF("Instrument")
                                                                   ,InputStartOfDayPositionDF("Account")
                                                                   ,InputStartOfDayPositionDF("AccountType")
                                                                   ,InputStartOfDayPositionDF("Quantity")
                                                                   ,InputTransactionsDF("TransactionType")
                                                                   ,InputTransactionsDF("TransactionQuantity"))
                 
                 /* 
                  Inner joining InputStartOfDayPosition DataFrame with InputTransactions DataFrame to find out the
                  pair of TractionTypes & AccountType which results in 'SUBTRACTION' of TransactionQuantity from the input start of day quantity for the instruments
                  and setting the TransactionQuantity as 'Negative' 
                 */                                
                 val TransactionsInvolvingQuantitySubtraction = InputStartOfDayPositionDF
                                                           .join(InputTransactionsDF
                                                              ,InputStartOfDayPositionDF("Instrument") === InputTransactionsDF("Instrument")
                                                                && (
                                                                    (InputTransactionsDF("TransactionType") === "B" && InputStartOfDayPositionDF("AccountType")==="I")
                                                                    ||
                                                                    (InputTransactionsDF("TransactionType") === "S" && InputStartOfDayPositionDF("AccountType")==="E")                                                            
                                                                )
                                                              ,"inner")
                                                           .select(InputStartOfDayPositionDF("Instrument")
                                                                   ,InputStartOfDayPositionDF("Account")
                                                                   ,InputStartOfDayPositionDF("AccountType")
                                                                   ,InputStartOfDayPositionDF("Quantity")
                                                                   ,InputTransactionsDF("TransactionType")
                                                                   ,-InputTransactionsDF("TransactionQuantity"))                                
                                   
                 /*
                 Applying 'UNION' operation on  TransactionsInvolvingQuantityAddition & TransactionsInvolvingQuantitySubtraction to get DataFrame of all the input transactions.
                 Grouping this DataFrame by 'INSTRUMENT' & 'ACCOUNT_TYPE' and summing up quantities for such each groups and calling it as 'Delta'
                 */                                
                val AllTransactionsWithQuantityDelta = TransactionsInvolvingQuantityAddition
                                                              .union(TransactionsInvolvingQuantitySubtraction)
                                                              .groupBy("Instrument","AccountType")
                                                              .sum("TransactionQuantity").withColumnRenamed("sum(TransactionQuantity)", "Delta")
                                                              .na.fill(0, Seq("Delta"))
                
                 /*                     
                  Now joining InputStartOfDayPositionDF DataFrame with AllTransactions DataFrame in 'Leftouter' type join mode to get all the fields from InputStartOfDayPositionDF
                  which were not part of group by aggregation and then setting 'Quantity' as sum of 'Quantity' & 'Delta' to get final resultant 'Quantity'.  
                  'Leftouter' join is used so that all the Instruments from  InputStartOfDayPositionDF are included even though there were no transactions in InputTransactions file
                  for those instruments
                 */
                val FinalPositionOfInstruments = InputStartOfDayPositionDF
                                                        .join(AllTransactionsWithQuantityDelta, Seq("Instrument","AccountType"),"leftouter")
                                                        .na.fill(0, Seq("Delta"))
                                                        .withColumn("Quantity", ($"Quantity" + $"Delta").cast(IntegerType))
                                                        .select("Instrument","Account","AccountType","Quantity","Delta")
                                          
               //Saving the result in output file in CSV format having header row in overwrite mode, using coalesce(1) to force the output to be written to a single part file 
               FinalPositionOfInstruments.coalesce(1)
                                           .write.format("com.databricks.spark.csv")
                                           .option("header", "true")
                                           .mode("overwrite")
                                           .save("..//UBS_Assignment//OutputFolder//Expected_EndOfDay_Positions.txt")           
               // The input/output file paths could be changed as per the case. Currently they are hard-coded. These can also be read from properties file                           
                
                FinalPositionOfInstruments.select("Instrument","Delta").createOrReplaceTempView("FinaloutputTable")
                
                //finding instrument with max quantity netchange 
                val maxNetChange = session.sqlContext.sql("select * from FinaloutputTable order by abs(Delta) desc")
                println("Max Quantity NetChange: " + maxNetChange.head())
                
                //finding instrument with min quantity netchange
                val minNetChange = session.sqlContext.sql("select * from FinaloutputTable order by abs(Delta)")
                println("Minimum Quantity NetChange" + minNetChange.head())
                               
                
     } catch {
           // printing the cause & stack trace if exception while creating or processing the dataframe arises
          case e: org.apache.spark.sql.AnalysisException => {
            println("\n Cause : " + e.getCause)
            println("\n StackTrace : " + e.printStackTrace())
          }
          case io: java.io.IOException => {
            //file input/output exceptions to be handled here
            println("\n Cause : " + io.getCause)
            println("\n StackTrace : " + io.printStackTrace())
          }
    } finally {
          // closing spark session 
          session.close()   
          println("Closed spark session")
    }
  }
}