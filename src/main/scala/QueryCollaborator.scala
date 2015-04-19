/**
 * Created by Gabriela & Grace on 3/29/2015.
 * The input of QueryCollaborator is a user id and a distance. It should be able to find all user that are in the same organization.
 * as the query user or in an organization that is within the input distance, and has the same interest/skill as the query user.
 * The outputs are a list of user names, their common interests (skills) shared with the query user, ranked interest/skill weights. The formula for ranked common interest is the sum
 * of the interest level between the two user. For example, if user j and k has interest i with the weight wji (wji>0) and wki (wki>0), respectively, the common interest weight between user j
 * and k will be wji+wki. The forumla for the ranked common skill is the max between the skill levels of two users.
 * The individuals should be ranked by the total weight of shared interests (or skills) with the user.
 */

import org.anormcypher._

class QueryCollaborator {

  def queryCollaborator (x: String, y: Float) : Unit = {
    implicit val connection = Neo4jREST()

    //querying for common interest
    val comm4Interest = "match (user: User{id: {x}}), (a: Org), (b: User), (c: Interest), (o:Org), (o)-[r]->(a), (b)-[rr]->(c), (user)-[rrr]->(c) where (user-->a and r.dis < {y} ) and (b--> a or b-->o) and (b<>user) return distinct b.fname as user, b.lname as lname, c.interest as interest, rr.level + rrr.level as total, case coalesce(b-->o and o<>a) when true then o.name else a.name end as result"
    val cyph4Interest = Cypher(comm4Interest).on("x" -> x, "y" -> y)
    val stream4Interest = cyph4Interest()
    var myList = stream4Interest.map(row =>{row[String]("user")->row[String]("lname")->row[String]("interest")->row[BigDecimal]("total")->row[String]("result")}).toList

    //querying for common interest
    val comm4Skills = "match (user: User{id: {x}}),(a: Org), (b: User), (s:Skill), (o:Org), (o)-[r]->(a), (b)-[rr]->(s), (user)-[rrr]->(s) where (user-->a and user-->s and r.dis < {y} ) and (b--> a or b-->o) and ( b-->s) and (b<>user) return distinct b.fname as username, b.lname as lname, s.skill as skill, case rr.level > rrr.level when true then rr.level else rrr.level end as result, case coalesce(b-->o and o<>a) when true then o.name else a.name end as orgName"
    val cyph4Skills = Cypher(comm4Skills).on("x"->x, "y" -> y)
    val stream4Skills = cyph4Skills()
    myList = myList ::: stream4Skills.map(row =>{row[String]("username")->row[String]("lname")->row[String]("skill")->row[BigDecimal]("result")->row[String]("orgName")}).toList

    var infoList: List[(BigDecimal, List[((((String, String),String), BigDecimal), String)])] = List()
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

    println("Ordered by Total Ranking (full name => interest/skill -> ranked level => Organization)")
    for (i <-infoList.length-1 to 0 by -1){
      println(infoList(i)._1)
      print("" + '\t' + infoList(i)._2(0)._1._1._1._1 + " " + infoList(i)._2(0)._1._1._1._2 + " => ")
      for (j <- 0 until infoList(i)._2.length){
        print(infoList(i)._2(j)._1._1._2 + "->" + infoList(i)._2(j)._1._2 + " ")
      }
      print("=> " + infoList(i)._2(0)._2)
      println()
    }

  }
}
