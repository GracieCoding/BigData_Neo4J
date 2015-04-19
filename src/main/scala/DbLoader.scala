/**
 * Created by Gabriela & Grace on 3/26/2015.
 * The input for DbLoader is six files for the data.
 * All you input data should use the same file names, headers, and formats.
 */

import org.anormcypher._

object DbLoader {

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

  def more(): Boolean ={
    var resp = ""
    println("Would you like to continue?(y/n) ")
    resp = Console.readLine()
    if (resp == "n" || resp == "N"){
      return false
    }
    return true
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
      println("Usage: program path_To_InputFile path_To_InputFile2 path_To_InputFile3 path_To_InputFile4 path_To_InputFile5")
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
        case _ => println("xxx")
      }
    }


    var commandCont = " as line create (:User {id: line[0], fname: line[1], lname: line[2]})"
    var fullCommand = command.concat(userFilePath).concat(commandCont)

    //opening user file and putting the data into database
    Cypher(fullCommand).execute()

    commandCont = " as line match (a: User{id: line[0]}) merge (or:Org {name: line[1]}) set or.type = line[2] create (a)-[r:belong_to]-> (or), (or)-[rr:has]->(a)"
    fullCommand = command.concat(orgFilePath).concat(commandCont)

    //opening organization file and putting data into database
    Cypher(fullCommand).execute()

    commandCont = " as line match (a: Org {name: line[0]}), (b:Org {name: line[1]}) create (a)-[r:distance{dis: toFloat(line[2])}]->(b), (b)-[rr:distance{dis: toFloat(line[2])}]->(a) return r,rr"
    fullCommand = command.concat(disFilepath).concat(commandCont)

    //opening distance file and putting the data into database
    Cypher(fullCommand).execute()

    commandCont = " as line match (a: User{id: line[0]} ) merge (in:Interest {interest: line[1]}) create (a)-[r:interested_in {level: toFloat(line[2])}]->(in) return r"
    fullCommand = command.concat(interestFilePath).concat(commandCont)

    //opening interest file and putting the data into database
    Cypher(fullCommand).execute()

    commandCont = " as line match (a: User {id: line[0]}) merge (sk:Skill {skill: line[1]}) create (a)-[r:skilled_at {level: toFloat(line[2])}]->(sk)  return r"
    fullCommand = command.concat(skillFilePath).concat(commandCont)

    //opening skill file and putting the data into database
    Cypher(fullCommand).execute()

    commandCont = " as line match (a: User {id: line[0]}) merge (pj:Project {project: line[1]}) create (a)-[r:worked_on]->(pj), (pj)-[rr:workers]->(a)"
    fullCommand = command.concat(projectFilePath).concat(commandCont)

    //opening project file and putting the data into database
    Cypher(fullCommand).execute()

    val query = new QueryCollaborator()
    val secondQuery = new QueryColOfCol()
    do {
      println("Action? (Press qc for queryCollaborator and qcoc for queryColofCol): ")

      var resp = Console.readLine()
      var input = " "
      var dis:Float = 10;
      resp match {
        case "qc" => println("Input user id: ")
          input = Console.readLine()
          println("Input distance search between organizations: ")
          dis = Console.readFloat()
          query.queryCollaborator (input, dis)
        case "qcoc" => println("Input user id: ")
          input = Console.readLine()
          println("What interest would you like to search?")
          var interest=Console.readLine()
          var interestList:List[String]=List()
          interestList = interestList :+ interest
          var more="y"
          while(more=="y") {
            println("Would you like to search another interest(y/n)?")
            more=Console.readLine()
            if(more=="y"){
              println("What interest:")
              interest=Console.readLine()
              interestList = interestList :+ interest
            }
          }
          secondQuery.colOfcol(input,interestList)
        case _ => println("Invalid action")
      }

    }while (more());

  }
}
