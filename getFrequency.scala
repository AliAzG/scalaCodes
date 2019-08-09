import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{SparkSession, SQLContext, DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.immutable.ListMap
import java.sql.Timestamp
import java.util.{Calendar, Date}
import org.joda.time.DateTime
import org.joda.time.Months
import org.joda.time.format.DateTimeFormat


var frequencyDF: DataFrame = ussdData.groupBy("mobile_no").pivot("new_topup_type").agg(count($"new_topup_type")).na.fill(0) 
        
        case class AppFormat(appInterestFirst: String, appInterestSecond: String, appInterestThird: String)

        val AppSchema = new StructType().add("APP_INTEREST_FIRST", StringType).add("APP_INTEREST_SECOND", StringType).add("APP_INTEREST_THIRD", StringType)

        val getInterests = udf((row: Row) => {

        val rowColumnsName: List[String] = row.schema.fieldNames.toList

        val rowValues: List[Int] = row.toSeq.toList.map(_.toString.toInt)

        val mapped: Map[Int, String] = (rowValues zip rowColumnsName).toMap

        val sorted = ListMap(mapped.toSeq.sortWith(_._1>_._1):_*)

        val thirdInterest = sorted.size match {
            case 1 => "NONE"
            case 2 => "NONE"
            case 3 => sorted.slice(2,3).head._1 match { 
                case 0 => "NONE"
                case _ => sorted.slice(2,3).head._2
            }
            case _ => sorted.slice(2,3).head._2

        }

        val secondInterest = sorted.size match {
            case 1 => "NONE"
            case 2 => sorted.slice(1,2).head._1 match {
                case 0 => "NONE"
                case _ => sorted.slice(1,2).head._2
            }
            case _ => sorted.slice(1,2).head._2
        }

        val firstInterest = sorted.slice(0,1).head._2

        AppFormat(firstInterest, secondInterest, thirdInterest)}, AppSchema)

        var interest_df: DataFrame = frequencyDF.withColumn("newCol", getInterests(struct(frequencyDF.columns.slice(1, frequencyDF.columns.length) map col: _*))).select($"mobile_no", $"newCol.*")
