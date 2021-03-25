package dfs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StructType, StringType }
import org.apache.spark.sql.functions.lit

object DFRenameColumn {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("jewel").master("local[*]").getOrCreate()
    import spark.implicits._

    val data = Seq(
      Row(Row("James;", "", "Smith"), "36636", "M", "3000"),
      Row(Row("Michael", "Rose", ""), "40288", "M", "4000"),
      Row(Row("Robert", "", "Williams"), "42114", "M", "4000"),
      Row(Row("Maria", "Anne", "Jones"), "39192", "F", "4000"),
      Row(Row("Jen", "Mary", "Brown"), "", "F", "-1"))

    val schema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("middlename", StringType)
        .add("lastname", StringType))
      .add("dob", StringType)
      .add("gender", StringType)
      .add("salary", StringType)

    var rowRDD = spark.sparkContext.parallelize(data)

    val df = spark.createDataFrame(rowRDD, schema)
    //   df.printSchema()

    var df1 = df.withColumnRenamed("gender", "maleORFemale")
    df1.show
  }
}
