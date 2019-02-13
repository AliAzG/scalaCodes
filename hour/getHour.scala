import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

//val format = "yyyyMMdd_HHmmss"

//val dtf = DateTimeFormatter.ofPattern(format)

//val ldt = LocalDateTime.of(2018, 1, 22, 10, 10, 43) // 20180122_101043
object Saat{
  def main(args: Array[String]) {
    val now = LocalDateTime.now();
    println(now.getHour());
  }
}
//ldt.format(dtf)