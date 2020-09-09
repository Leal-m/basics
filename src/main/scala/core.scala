import org.apache.spark.sql.{DataFrame, SparkSession}

object core {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ReadFile")
      .master("local[*]")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/report.csv")

    println("** Schema **")
    df.printSchema()

    println("** DataFrame **")
    df.show(false)
  }
}
