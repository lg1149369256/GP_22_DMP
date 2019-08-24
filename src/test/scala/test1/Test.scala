package test1
import com.utils.AliMapUtil
import org.apache.spark.{SparkConf, SparkContext}
object Test{
  def main (args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
     val  sc = new SparkContext(conf)
      val list = List("116.31003,39.991957")
    val rdd = sc.makeRDD(list)

    val bs= rdd.map(x=>{
      val arr = x.split(",")
      AliMapUtil.getBusinessFromAliMap(arr(0).toDouble,arr(1).toDouble)
    }).foreach(println)

    sc.stop()
  }
}