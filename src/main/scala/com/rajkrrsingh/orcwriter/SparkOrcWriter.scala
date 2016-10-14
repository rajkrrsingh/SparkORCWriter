import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkOrcWriter {

  case class Contact(name: String, phone: String)
  case class Person(name: String, age: Int, contacts: Seq[Contact])
  case class Foobar(foo: String, bar: Integer)


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ORC Writer")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    import sqlContext.implicits._


    val records = (1 to 100).map { i =>;
      Person(s"name_$i", i, (0 to 1).map { m => Contact(s"contact_$m", s"phone_$m") })
    }


    val foobarRdd = sc.parallelize(("foo", 1) :: ("bar", 2) :: ("baz", -1) :: Nil).
      map { case (foo, bar) => Foobar(foo, bar) }

    val foobarDf = foobarRdd.toDF()
     foobarDf.limit(1).show

    sc.parallelize(records).toDF().write.format("orc").save("people")
    val people = sqlContext.read.format("orc").load("people")
    people.registerTempTable("people")
    sqlContext.sql("SELECT name FROM people WHERE age < 15").count()


  }
}