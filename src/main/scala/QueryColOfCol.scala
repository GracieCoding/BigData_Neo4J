/**
 * Created by Gabriela & Grace on 4/4/2015.
 * The input of QueryColOfCol is a user id and at least one interest. The output is a list of user names that are "trusted colleagues
 * -of-colleagues" and have the input interest(s).
 */


import org.anormcypher._

class QueryColOfCol {
  def colOfcol(x: String, interests:List[String]) : Unit ={
    implicit val connection = Neo4jREST()
    val comm = "start user = node(*) where user.id = {x} match(u: User), (p:Project) where user-->p and u-->p and user<>u return distinct u.id as id"
    val cyphie = Cypher(comm).on("x" -> x)
    val stream = cyphie()
    val Col = stream.map(row =>{row[String]("id")}).toList
    var ColofCol: List[String] = List()

    for( i <- 0 until Col.length) {
      val comm2 = "match (user: User {id: {x}}),(u: User), (p:Project) where user-->p and u-->p and user<>u and u.id<>{y} return distinct u.id as id"
      val cyphie2 = Cypher(comm2).on("x" -> Col(i), "y" -> x)
      val streams = cyphie2()
      ColofCol = ColofCol ::: streams.map(row =>{row[String]("id")}).toList
    }

    val comm3 = "unwind{z} as zz unwind {w} as ww  match (i: Interest{interest: zz}), (u: User {id: ww}) where u--> i  return distinct u.fname as name, u.lname as lname, i.interest as interest"
    val cyphie3 = Cypher(comm3).on("z" -> interests, "w"->ColofCol)
    val stream3 = cyphie3()
    var myFinalList = stream3.map(row=>{row[String]("name")->row[String]("lname")->row[String]("interest")}).toList

    println()

    println("Results: ")
    if (!myFinalList.isEmpty) {
      myFinalList = myFinalList.sortBy(x=> x._1)
      for (i <- 0 until myFinalList.length) {
        if (i == 0) {
          print(myFinalList(i)._1 + ": " + myFinalList(i)._2 + " ")
        }
        else {
          if (myFinalList(i)._1 == myFinalList(i - 1)._1) {
            print(myFinalList(i)._2 + " ")
          }
          else {
            println(myFinalList(i)._1 + ": " + myFinalList(i)._2 + " ")
          }
        }
      }
      println()
    }
    else {
      println("There is no match found.")
    }


  }
}
