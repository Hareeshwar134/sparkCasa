package sparkcasePack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object caseObj {
  
  case class columns(category:String,product:String,city:String,state:String,spendby:String)
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("First").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
    
    println("----rawdata----")
    val data=sc.textFile("file:///C:/data/txns")
    data.foreach(println)
    
    println("----mapsplitwithcomma----")
    val gymdata=data.map(x=>x.split(","))
    gymdata.foreach(println)
    
     println("----mapsplit----")
    val mapsplit=gymdata.map(x=>(x(4),x(5),x(6),x(7),x(8)))
    mapsplit.foreach(println)
    
    println("----colsplit----")
    val colrdd=gymdata.map(x=>columns(x(4),x(5),x(6),x(7),x(8))) //schema rdd
    colrdd.foreach(println)
    
      println("----filtersplit----")
    val filterdata=colrdd.filter(x=>x.product.contains("Gymnastics") & x.spendby.contains("cash")) //transformation
    filterdata.foreach(println) //action 
    
    println("----Task1----")
    val task= gymdata.map(x=>(x(4),1))
    task.foreach(println)
    
  }
}