# scala-maven-hbase-spark
Scala code to read and update hbase table based on specific value in a column qualifier

1. Spark Executable jar generated thru Maven project  
1. Intellij based project. Open and do 'mvn package'  
1. For Spark submit test,   
Used this approach in (AWS node) custom ubuntu
spark-submit --packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 --repositories https://repo.hortonworks.com/content/groups/public/   
--class com.practice.HbaseHistory --master local /home/ubuntu/hbase-history-1.0.0-SNAPSHOT.jar  > output.txt 2>&1
  
For Hadoop node in Hortonworks HDP or HDF  
spark-submit --class com.practice.HbaseHistory --master local /home/ubuntu/hbase-history-1.0.0-SNAPSHOT.jar > output.txt 2>&1  
  
output.txt is for final log, 2>&1 to redirect both standard output and error info while running with Spark  

## Extension of work  
Direct Spark RDD way did not work in some places including hadoop node (HDP) for spark-hbase-connector.  
Current code can be updated further to do  
1. Read hbase table data using RDD or DF  
1. Do bulk Hbase put  



