package spark_txns_flatten
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object spark_txns_flatten {
  
  def main(args:Array[String]):Unit={  

    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
       val sc = new SparkContext(conf) 
        sc   .setLogLevel("Error")
        
        
       val data = sc.textFile("hdfs:/user/cloudera/spark_dir/txns")
       val gym_data = data.filter(x=>x.contains("Gymnastics"))
       val cash_data = gym_data.filter(x=>x.contains("cash"))
       val flat_data = cash_data.flatMap(x=>x.split(","))
       flat_data.saveAsTextFile("hdfs:/user/cloudera/gym_tar_flatten")
  
  
  
}
}