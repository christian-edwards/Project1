import org.apache.spark.{SparkConf, SparkContext}
import java.sql.{Connection, DriverManager, SQLException}
import java.io.File
import java.io.FileNotFoundException
import java.util.{InputMismatchException, Scanner}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

object Main {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    var currentUser = ""
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/project1"
    val username = "root"
    val password = "had00p_677"
    val scanner = new Scanner(System.in)
    var connection: Connection = DriverManager.getConnection(url, username, password)
    val spark = SparkSession
      .builder
      .appName("Get revenue per order")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    //varchar(255), state varchar(255), casesConfProbable INT, casesConfirmed INT, casesProbable INT, viralTestPos INT, viralTestNeg INT, viralTestTotal INT, antigenTestPos INT, antigenTestNeg INT, antigenTestTotal INT, viralPeoplePos INT, viralPeopleTotal INT, antigenPeoplePos INT, antigenPeopleTotal INT, viralEncountersTotal INT, testsCombinedTotal INT);
    /*
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("SELECT * FROM users;")
    while (resultSet.next()) {
      println(resultSet.getString(1) + ", " + resultSet.getString(2) + ", " + resultSet.getString(3), resultSet.getString(4))
    }
    */
    //spark.sql("USE covidDB")
    spark.sql("DROP TABLE IF EXISTS covidData")
    spark.sql("CREATE TABLE IF NOT EXISTS covidData(dateofrecord varchar(255),state varchar(255),casesconfprobable int, casesconfirmed int, casesprobable int, viraltestpos int, viraltestneg int, viraltesttotal int, antigentestpos int, antigentesttotal int, viralpeoplepos int, viralpeopletotal int, antigenpeoplepos int,antigenpeopletotal int,viralencounterstotal int, testscombinedtotal int) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'Inputs/time_series_covid19_US.txt' INTO TABLE covidData")

    def createAccount():String = {
      println("Desired username:")
      val user = scanner.nextLine()
      val statement = connection.createStatement()
      val resultSetNew = statement.executeQuery("SELECT * FROM users WHERE userName = '"+ user +"';")
      if(resultSetNew.next()){
        println("Name already exists!")
        return ""
      } else {
        println("Desired password:")
        val password = scanner.nextLine()
        var exStatement = connection.createStatement()
        exStatement.executeUpdate("INSERT INTO users(userName,isAdmin,pw) VALUES(\"" + user + "\", 0 , \"" + password +"\");" )
        println("Account created!")
        return user
      }
    }

    def login(): String = {
      println("Username:")
      val user = scanner.nextLine()
      println("Password:")
      val pass = scanner.nextLine()
      val statement = connection.createStatement()
      val resultSetNew = statement.executeQuery("SELECT * FROM users WHERE userName = '"+ user +"';")
      while (resultSetNew.next()) {
        if(resultSetNew.getString(4)==pass){
          println("Successfully logged in as "+user)
          return user
        } else {
          println("Incorrect login credentials.")
          println(resultSetNew.getString(4) + " " + pass)
          return ""
        }
      }
      println("No such user found.")
      return ""
    }

    def loginMenu(account:String):Int = {
      while(true){
        println("Currently logged in as " + account)
        if(account == "caedwar1"){println("ADMIN CONSOLE: Please select 1 through 6 to see queries. Select 0 to log out.")}
        else{println("USER CONSOLE: Please select 1 through 5 to see queries. Select 0 to log out.")}
        var j = scanner.nextLine()
        j match {
          case "1" =>
            println("Query 1: What is the current number of previously confirmed cases per state in the US?")
            spark.sql("SELECT state, MAX(casesconfirmed) FROM coviddata WHERE state!='state' GROUP BY state ORDER BY state").show(50)
          case "2" =>
            println("Query 2: What is the average number of positive antigen tests?")
            spark.sql("WITH maxTotal AS (SELECT state, MAX(antigentestpos) AS Confirmed FROM coviddata WHERE state!='state' GROUP BY state) SELECT ROUND(AVG(Confirmed),2) AS Average FROM maxTotal").show(50)
          case "3" =>
            println("Query 3: What is the number of cases since the start of November in Arkansas?")
            spark.sql("WITH dates AS (SELECT dateofrecord AS Date, casesconfirmed AS Cases FROM coviddata WHERE state =='AK' AND (dateofrecord =='11/3/2021' OR dateofrecord == '11/1/2021')) SELECT MAX(Cases) - MIN(Cases) AS November FROM dates").show()
          case "4" =>
            println("Query 4: What is the ratio of bacterial tests that were positive in South Carolina?")
            spark.sql("SELECT CONCAT(ROUND((MAX(viraltestpos)/MAX(viraltesttotal))*100,2), '%') AS Ratio  FROM coviddata WHERE state == 'SC'").show()
          case "5" =>
            println("Query 5: What are the total amount of previously confirmed cases in the United States?")
            spark.sql("WITH maxCase AS (SELECT state, MAX(casesconfirmed) AS Minimum FROM coviddata GROUP BY state) SELECT SUM(Minimum) AS Total from maxCase ").show(30)
          case "6" =>
            if(account =="caedwar1"){
              println("Future Query: Based on a linear model, how many total cases do we expect to be confirmed at the end of next month in Georgia?")
              spark.sql("WITH dates AS (SELECT dateofrecord AS Date, casesconfirmed AS Cases FROM coviddata WHERE state =='GA' AND (dateofrecord =='11/1/2021' OR dateofrecord == '10/1/2021')) SELECT ((MAX(Cases) - MIN(Cases))+MAX(Cases)) AS Prediction FROM dates").show()
            }
            else {
              println("ERROR: Please enter any number 1 - 5 to select a query or 0 to log out.")
            }
          case "0" =>println("Logging out...\n\n")
            return 0
          case default =>
            if(account =="caedwar1"){println("ERROR: Please enter any number 1 - 6 to select a query or 0 to log out.")}
            else{println("ERROR: Please enter any number 1 - 5 to select a query or 0 to log out.")}
        }

      }
      return 1
    }



    while (true) {
      println("Please select 1 to log in or 2 to create an account. 0 to exit.")
      var i = scanner.nextLine()
      i match {
        case "1" =>
          currentUser = login()
          if(currentUser != ""){
            loginMenu(currentUser)
            currentUser = ""
          }
        case "2" =>
          currentUser = createAccount()
          if(currentUser != ""){
            loginMenu(currentUser)
            currentUser = ""
          }
        case "0" => println("Exiting...")
          connection.close()
          return
        case default => println("ERROR: Please pick a correct input.")
      }
    }
  }
}