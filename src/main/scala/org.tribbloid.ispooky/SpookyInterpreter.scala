package org.tribbloid.ispooky

import org.apache.spark.sql.SQLContext
import org.tribbloid.ispark.{Results, SparkInterpreter}
import org.tribbloid.spookystuff.SpookyContext

import scala.collection.immutable
import scala.language.reflectiveCalls
import scala.tools.nsc.interpreter.NamedParam

/**
 * Created by peng on 22/07/14.
 */
class SpookyInterpreter(args: Seq[String], usejavacp: Boolean=true)
  extends SparkInterpreter(args, usejavacp) {

  override lazy val appName: String = "ISpooky"

  override def initializeSpark() {
    super.initializeSpark()

    val IR = scala.tools.nsc.interpreter.Results

    val sqlContext = new SQLContext(this.sc)
    intp.quietBind(NamedParam[SQLContext]("sqlContext", sqlContext), immutable.List("@transient")) match {
      case IR.Success =>
      case _ => throw new RuntimeException("SQLContext failed to initialize")
    }

    val spooky = new SpookyContext(sqlContext)
    intp.quietBind(NamedParam[SpookyContext]("spooky", spooky), immutable.List("@transient")) match {
      case IR.Success =>
      case _ => throw new RuntimeException("SpookyContext failed to initialize")
    }

    interpret("""
import scala.concurrent.duration._
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.dsl._
import spooky._
              """) match {
      case Results.Success(value) =>
      case Results.Failure(ee) => throw new RuntimeException("SQLContext failed to be imported", ee)
      case _ => throw new RuntimeException("SQLContext failed to be imported")
    }
  }

//      interpret("""
//  import scala.concurrent.duration._
//  import org.tribbloid.spookystuff.actions._
//  import org.tribbloid.spookystuff.dsl._
//                """) match {
//        case Results.Failure(ee) => throw new RuntimeException("SpookyContext failed to be imported", ee)
//        case Results.Success(value) =>
//        case _ => throw new RuntimeException("SpookyContext failed to be imported")
//      }
}
