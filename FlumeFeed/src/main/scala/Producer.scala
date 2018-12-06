import java.io._
import java.net._

import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat

import scala.util.{Random, Try}

object Producer extends App {
  if (args.length != 1 || Try(args(0).toInt).isFailure) {
    println("Please provide exactly one int argument as server port")
    System.exit(1)
  }
  val random = new Random()
  val clientSocket = new Socket("127.0.0.1", args(0).toInt)

  val writer = new PrintWriter(clientSocket.getOutputStream)


  for (a <- 0 to 3000) {
    val cat = RandomGen.nextAlpha(random)
    val name = cat + RandomGen.nextProdNameId(random)
    writer.println(Row(pName = name, pPrice = a, pDate = RandomGen.nextDate(random), pCat = cat,
      clientIp = RandomGen.nextIp(random)).toCsv())
    writer.flush()
  }
  writer.close()
  clientSocket.close()
}

object RandomGen {

  val RefWeekStart = new LocalDateTime(2000, 1, 1, 0, 0)
  def nextDate(random: Random): LocalDateTime = {
    val datePart = random.nextInt(7)
    val timePart = random.nextInt(86400001)
    RefWeekStart.plusDays(datePart).plusMillis(timePart)
  }
  def nextAlpha(random: Random): String = {
    random.alphanumeric.head.toString
  }
  def nextProdNameId(random: Random): Int = random.nextInt(5)
  def nextIp(random: Random) : String = {
    def part = random.nextInt(256)
    s"$part.$part.$part.$part"
  }
}

case class Row(pName: String, pPrice: Int, pDate: LocalDateTime, pCat: String, clientIp: String) {
  val Format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  def toCsv() = {
    s"$pName,$pPrice,$pCat,$clientIp,${Format.print(pDate)}"
  }
}