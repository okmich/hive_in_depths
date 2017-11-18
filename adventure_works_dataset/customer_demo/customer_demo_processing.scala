import java.math.{BigDecimal => JavaDecimal}
import java.lang.{Byte => JavaByte}
import scala.xml.XML
import java.sql.Date



val customerRDD = sqlContext.read.parquet("/user/cloudera/bigretail/output/stores/sqoop/customers")
val projDF = customerRDD.select("CustomerID","Demographics")

case class CustomerDemo(CustomerID: Int, TotalPurchaseYTD: JavaDecimal, DateFirstPurchase : Date, BirthDate : Date, MaritalStatus : String, YearlyIncome : String, Gender : String, TotalChildren : JavaByte, NumberChildrenAtHome : JavaByte, Education : String, Occupation : String, HomeOwnerFlag : String, NumberCarsOwned : JavaByte, CommuteDistance : String) extends java.io.Serializable


val sdf = new java.text.SimpleDateFormat("yyy-MM-dd'Z'")

def createCustDemo(custId: Int, demo:String) : CustomerDemo = {
	def getDate(f: String) : Date = {
		if (f == null || f.isEmpty || f.length < 11) 
			null
		else {
			try{
				new Date(sdf.parse(f).getTime)
			} catch{
				case _ : Throwable => null
			}
		}
	}
	val node = XML.loadString(demo)
	val totalPurchase = (node \\ "IndividualSurvey" \\ "TotalPurchaseYTD").text
	val firstPurchaseDt = getDate((node \\ "IndividualSurvey" \\ "DateFirstPurchase").text)
	val birthDt = getDate((node \\ "IndividualSurvey" \\ "BirthDate").text)
	val childCount = (node \\ "IndividualSurvey" \\ "TotalChildren").text
	val childAtHomeCount = (node \\ "IndividualSurvey" \\ "NumberChildrenAtHome").text
	val ownedCarCount = (node \\ "IndividualSurvey" \\ "NumberCarsOwned").text
	CustomerDemo(
		custId,
		if (totalPurchase == null || totalPurchase.isEmpty) null else new JavaDecimal(totalPurchase),
		firstPurchaseDt,
		birthDt,
		(node \\ "IndividualSurvey" \\ "MaritalStatus").text,
		(node \\ "IndividualSurvey" \\ "YearlyIncome").text,
		(node \\ "IndividualSurvey" \\ "Gender").text,
		if (childCount == null || childCount.isEmpty ) null else childCount.toByte,
		if (childAtHomeCount == null || childAtHomeCount.isEmpty ) null else childAtHomeCount.toByte,
		(node \\ "IndividualSurvey" \\ "Education").text,
		(node \\ "IndividualSurvey" \\ "Occupation").text,
		(node \\ "IndividualSurvey" \\ "HomeOwnerFlag").text,
		if (ownedCarCount == null || ownedCarCount.isEmpty ) null else ownedCarCount.toByte,
		(node \\ "IndividualSurvey" \\ "CommuteDistance").text
	)
}
val projRDD = projDF.map(r => createCustDemo(r.get(0).toString.toInt, r.get(1).toString))

projRDD.toDF.write.format("orc").mode("append").save("/user/cloudera/bigretail/output/stores/spark/customer_demo")
