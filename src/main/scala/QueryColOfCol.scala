/**
 * Created by Gaberila & Grace on 4/4/2015.
 * The input of QueryColOfCol is a user id and at least one interest. The output is a list of user names that are "trusted colleagues
 * -of-colleagues" and have one or more particular interest as the input user.
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

    val comm3 = "unwind{z} as zz unwind {w} as ww  match (i: Interest {interest: zz}), (u: User {id: ww}) where u--> i  return u.fname as name, i.interest as interest"
    val cyphie3 = Cypher(comm3).on("z" -> interests, "w"->ColofCol)
    val stream3 = cyphie3()
    val myFinalList = stream3.map(row=>{row[String]("name")->row[String]("interest")}).toList

    println(myFinalList)



  }
}
