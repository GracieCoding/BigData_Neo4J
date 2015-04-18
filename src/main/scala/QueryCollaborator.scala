/**
 * Created by Gaberila & Grace on 3/29/2015.
 */

import org.anormcypher._

class QueryCollaborator {

  def queryCollaborator (x: String, y: Float) : Unit = {
    implicit val connection = Neo4jREST()

    val comm4Interest = "start user = node(*) where user.id = {x} match (a: Org), (b: User), (c: Interest), (o:Org), (o)-[r]->(a), (b)-[rr]->(c), (user)-[rrr]->(c) where (user-->a and r.dis < {y} ) and (b--> a or b-->o) and (b<>user) return distinct b.fname as user, c.interest as interest, rr.level + rrr.level as total, case coalesce(b-->o and o<>a) when true then o.name else a.name end as result"
    val cyph4Interest = Cypher(comm4Interest).on("x" -> x, "y" -> y)
    val stream4Interest = cyph4Interest()
    var myList = stream4Interest.map(row =>{row[String]("user")->row[String]("interest")->row[BigDecimal]("total")->row[String]("result")}).toList

    val comm4Skills = "start user = node(*) where user.id = {x} match (a: Org), (b: User), (s:Skill), (o:Org), (o)-[r]->(a), (b)-[rr]->(s), (user)-[rrr]->(s) where (user-->a and user-->s and r.dis < {y} ) and (b--> a or b-->o) and ( b-->s) and (b<>user) return distinct b.fname as username, s.skill as skill, case rr.level > rrr.level when true then rr.level else rrr.level end as result, case coalesce(b-->o and o<>a) when true then o.name else a.name end as orgName"
    val cyph4Skills = Cypher(comm4Skills).on("x"->x, "y" -> y)
    val stream4Skills = cyph4Skills()
    myList = myList ::: stream4Skills.map(row =>{row[String]("username")->row[String]("skill")->row[BigDecimal]("result")->row[String]("orgName")}).toList

    var infoList: List[(BigDecimal, List[(((String, String),BigDecimal), String)])] = List()
    var sum: BigDecimal = 0

    val myMap = myList.groupBy(x => x._1._1._1)

    for (i<- myMap){
      sum = 0
      for (j <-0 until i._2.length) {
        sum += i._2(j)._1._2
      }
      infoList = infoList :+ (sum, i._2)
    }

    infoList = infoList.sortBy(x=> x._1)

    println("List with total weight, username, interest/skill, interest/skill level, organization: ")
    println(infoList.reverse)

  }
}
