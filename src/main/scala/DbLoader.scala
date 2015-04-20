/**
 * Created by Gabriela & Grace on 3/26/2015.
 * The input for DbLoader is six files for the data (user, organization, distance, skill, interest, project).
 * All your input data use the same file names, headers, and formats.
 */

import org.anormcypher._
import scala.actors.Actor._
import scala.actors.Actor

object DbLoader {

  /**
   * This function returns what type of file it is based on the path to the file.
   * @param fileName A string that contains the the path to the file.
   * @return A string stating the type of file it is based on keyword from the path to file.
   */
  def whatFile(fileName: String ): String = {

    if (fileName.contains("user")){
      "user"
    }
    else if (fileName.contains("organization")){
      "organization"
    }
    else if (fileName.contains("distance")){
      "distance"
    }
    else if(fileName.contains("skill")){
      "skill"
    }
    else if (fileName.contains("interest")){
      "interest"
    }
    else if (fileName.contains("project")){
      "project"
    }
    else {
      "blank"
    }
  }

  /**
   * Ths function checks to see if the user wants to continue the last operation.
   * @param x A string, tailored to the last operation, asking if the user wants to continue.
   * @return A boolean indicating whether the user wants to continue.
   */
  def more(x: String): Boolean ={
    var resp = ""
    println(x + "(y/n) ")
    resp = Console.readLine()
    while (resp != "n" && resp != "N" && resp != "y" && resp != "Y"){
      println("Invalid response. " + x)
      resp = Console.readLine()
    }
    if (resp == "n" || resp == "N"){
      return false
    }
    return true
  }

  /**
   * This function checks if a input provided by the user is a number.
   * @param x A string that is the user's input.
   * @return A string containing the proper input, a number.
   */
  def validDisChecker(x: String): String ={
    var temp = x
    var bool = true;
    for (i <-0 until temp.length){
      if (!temp(i).isDigit && temp(i) != '.'){
        bool = false;
      }
    }
    while (bool == false){
      println("Invalid input. Please enter a digit for the distance to search between organizations: ")
      temp = Console.readLine()
      bool = true;
      for (i <-0 until x.length){
        if (!temp(i).isDigit && temp(i) != '.'){
          bool = false;
        }
      }
    }
    temp
  }

  def main(args: Array[String]):  Unit = {
    implicit val connection = Neo4jREST()


    Cypher("match n optional match n-[r]-() delete n,r").execute()

    val command = "using periodic commit 1000 load csv from 'file:"

    var userFilePath = ""
    var orgFilePath = ""
    var disFilepath = ""
    var interestFilePath = ""
    var skillFilePath = ""
    var projectFilePath = ""
    var fileType = " "

    if (args.length == 0){
      println("Usage for Windows: program C:/path/to/file.csv C:/path/to/file2.csv C:/path/to/file3.csv C:/path/to/file4.csv C:/path/to/file5.csv C:/path/to/file6.csv")
      println("Usage for OSX or Unix: program ///path/to/file.csv ///path/to/file2.csv ///path/to/file3.csv ///path/to/file4.csv ///path/to/file5.csv ///path/to/file6.csv")
      System.exit(1)

    }

    for (i<-0 until args.length){
      fileType = whatFile(args(i))
      fileType match {
        case "user" => userFilePath = args(i) + "'"
        case "organization" => orgFilePath =   args(i) + "'"
        case "distance" => disFilepath =   args(i) + "'"
        case "interest" => interestFilePath =   args(i) + "'"
        case "skill" => skillFilePath =  args(i) + "'"
        case "project" => projectFilePath = args(i) + "'"
        case _ => println("unknown file")
      }
    }

    var commandCont = " as line create (:User {id: line[0], fname: line[1], lname: line[2]})"
    var fullCommand = command.concat(userFilePath).concat(commandCont)

    //opening user file and putting the data into database
    Cypher(fullCommand).execute()



    val myOrgActor = actor {
      commandCont = " as line match (a: User{id: line[0]}) merge (or:Org {name: line[1]}) set or.type = line[2] create (a)-[r:belong_to]-> (or), (or)-[rr:has]->(a)"
      fullCommand = command.concat(orgFilePath).concat(commandCont)

      //opening organization file and putting data into database
      Cypher(fullCommand).execute()
    }

    val myInterestActor = actor {
      commandCont = " as line match (a: User{id: line[0]} ) merge (in:Interest {interest: lower(line[1])}) create (a)-[r:interested_in {level: toFloat(line[2])}]->(in) return r"
      fullCommand = command.concat(interestFilePath).concat(commandCont)

      //opening interest file and putting the data into database
      Cypher(fullCommand).execute()
    }

    val mySkillActor = actor {
      commandCont = " as line match (a: User {id: line[0]}) merge (sk:Skill {skill: line[1]}) create (a)-[r:skilled_at {level: toFloat(line[2])}]->(sk)  return r"
      fullCommand = command.concat(skillFilePath).concat(commandCont)

      //opening skill file and putting the data into database
      Cypher(fullCommand).execute()
    }

    val myProjectActor = actor {
      commandCont = " as line match (a: User {id: line[0]}) merge (pj:Project {project: line[1]}) create (a)-[r:worked_on]->(pj), (pj)-[rr:workers]->(a)"
      fullCommand = command.concat(projectFilePath).concat(commandCont)

      //opening project file and putting the data into database
      Cypher(fullCommand).execute()
    }


    while (myOrgActor.getState != Actor.State.Terminated){

    }

    //val myDisActor = actor {
      commandCont = " as line match (a: Org {name: line[0]}), (b:Org {name: line[1]}) create (a)-[r:distance{dis: toFloat(line[2])}]->(b), (b)-[rr:distance{dis: toFloat(line[2])}]->(a) return r,rr"
      fullCommand = command.concat(disFilepath).concat(commandCont)

      //opening distance file and putting the data into database
      Cypher(fullCommand).execute()
    //}

    while (mySkillActor.getState != Actor.State.Terminated && myInterestActor.getState != Actor.State.Terminated && myProjectActor.getState != Actor.State.Terminated){

    }
    val query = new QueryCollaborator()
    val secondQuery = new QueryColOfCol()
    do {
      println("Action? (Press qc for queryCollaborator and qcoc for queryColofCol): ")

      var resp = Console.readLine()
      var input = " "
      var dis:Float = 0;
      resp match {
        case "qc" => println("Input user id: ")
          input = Console.readLine()
          println("Input distance search between organizations: ")
          var temp = Console.readLine();

          dis = validDisChecker(temp).toFloat
          while (dis < 1){
            println("Invalid input. Please enter a digit greater than 0 for distance to search between organizations: ")
            temp = Console.readLine()
            dis = validDisChecker(temp).toFloat
          }
          query.queryCollaborator (input, dis)
        case "qcoc" => println("Input user id: ")
          input = Console.readLine()
          var interestList: List[String] = List()
          do {
            println("What interest would you like to search?")
            var interest = Console.readLine()
            interest = interest.toLowerCase
            interestList = interestList :+ interest
          }while (more("Would you like to search for another interest?"))

          secondQuery.colOfcol(input,interestList)
        case _ => println("Invalid action")
      }

    }while (more("Would you like to continue?"));

  }
}
