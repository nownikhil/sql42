package sql42

import org.scalatest._
import flatspec._
import matchers._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions.col

class SparkTest extends AnyFlatSpec with should.Matchers {
  Logger.getLogger("org").setLevel(Level.OFF)

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Dataframe test")
      .getOrCreate()
  }


  import spark.implicits._
  "Dataframe" should "just work" in {
    val emps = spark.sparkContext.parallelize(Employee.emps).toDF()
    val deps = spark.sparkContext.parallelize(Department.deps).toDF()
      .withColumnRenamed("name", "nameX")
      .withColumnRenamed("depid", "depidX")
    val noobs = spark.sparkContext.parallelize(NoobDep.noobs).toDF()

    val joined = emps.join(deps, emps("depid") ===  deps("depidX"))
    joined.printSchema()
    joined.show()

    import org.apache.spark.sql.functions.udf

    def someFunc(a: Int): Int = a + 1
    val someudf = udf(someFunc(_))

    val projected = joined.select(
      col("depid"),
      col("empid"),
      someudf(col("rating")).alias("tuntak")
    )

    projected.printSchema()
    projected.show()
    import org.apache.spark.sql.functions._
    val aggs = projected.groupBy(col("depid"))
      .agg(
        count(col("empid")).as("cnt"),
        max(col("tuntak")),
        count("*").as("cnt2")
      )
    aggs.printSchema()
   // aggs.show()

    aggs.filter(col("cnt") >= 2).show()
  }
}

// Input are copied because .toDF wasn't working with Java classes
case class Employee(empid: Int, name: String, depid: Int, good: Boolean, rating:Int)
case object Employee {
  val emps = Seq (
    Employee(1, "Bill", 1, false, 3),
    Employee(2, "Eric", 1, false, 2),
    Employee(3, "Sebastian", 2, true, 4),
    Employee(4, "GG", 3, true, 5),
    Employee(5, "GGWP", 3, true, 4),
  )
}

case class Department(depid: Int, name: String, useful: Boolean)
case object Department {
  val deps = Seq(
    Department(1, "dep1", true),
    Department(2, "dep2", true),
    Department(3, "dep3", false),
  )
}

case class NoobDep(noobid: Int, name: String, boolval: Boolean)
case object NoobDep {
  val noobs = Seq(
    NoobDep(1, "n7", true),
    NoobDep(2, "n2", true),
    NoobDep(3, "n3", false),
  )
}
