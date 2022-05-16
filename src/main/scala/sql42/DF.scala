package sql42

import org.apache.calcite.adapter.java.ReflectiveSchema
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rel.logical._
import org.apache.calcite.schema.impl.{AggregateFunctionImpl, ScalarFunctionImpl}
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.{Frameworks, RelRunners}
import sql42.RexNodeHandler.compute

import java.io.PrintWriter
import java.util.logging.Logger
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

trait Op {
  var sourceName: Option[String] = None
  def updateSourceName(name: String): Op = {
    this.sourceName = Some(name)
    this
  }
}

case class StringOp(stringValue: String) extends Op
case class UnionOp(values: Seq[String]) extends Op
case class SortOp(fields: Seq[String], direction: Seq[Direction]) extends Op
case class ProjectOp(expressions: Seq[String]) extends Op // already parsed as col(a) or df.col(a)
case class SourceOp(tableName: String) extends Op
case class AggOp(groupByFields: Seq[String], aggs: Seq[String]) extends Op
case class JoinOp(leftDFName: String,
   rightDFName: String,
   condition: String,
   joinType: String,
   leftFieldNames: Seq[String],
   rightFieldNames: Seq[String],
) extends Op

// declares a new variable
class FrameOp(var dataframeName: String) extends Op {
  val ops = scala.collection.mutable.ArrayBuffer.empty[Op]
  def addOp(op: Op): FrameOp = {
    this.ops += op
    this
  }
}


object DF {
  val logger = Logger.getLogger(this.getClass.toString)

  def getQuery: String = {
    val caseWhen =
      s"case\n when n.boolval = false then 0\n when n.boolval = true then 1\n end as nc"

    val mathExpression = s"sub.cnt / 2, sub.cnt + 1 - 3 + 9 as addition, isNull(sub.cnt) as isnull"
    val innerQuery =
      s"select count(e.empid) as cnt, d.depid, customudaf(myudf(myudf(e.rating))) as name from hr.emps as e inner join hr.deps as d on (e.secondid = d.secondid and e.depid = d.depid) group by d.depid having count(*) >= 2"
    val query =
      s"Select sub.*, $caseWhen, $mathExpression, n.name, isNull(n.name) as nullString from ($innerQuery) sub left join hr.noobs as n on sub.depid = n.noobid order by n.name desc"

    val unionQuery = s"$innerQuery union all $innerQuery union all $innerQuery"
    unionQuery
  }

  def main(args: Array[String]): Unit = {
    val rootSchema = Frameworks.createRootSchema(true)
    val schema = new ReflectiveSchema(new HrMin.Hr())
    val hr = rootSchema.add("hr", schema)

    // Handle UDFs
    val implementation = ScalarFunctionImpl.create(classOf[HrMin.MyUdf], "eval")
    val arrayContains = ScalarFunctionImpl.create(classOf[ArrayContains], "f")
    val isNull = ScalarFunctionImpl.create(classOf[isNull], "f")
    val udaf = AggregateFunctionImpl.create(classOf[MySum])

    hr.add("myudf", implementation)
    hr.add("customudaf", udaf)
    hr.add("isNull", isNull)

    val insensitiveParser = SqlParser.configBuilder.setCaseSensitive(false).build
    val frameworkConfig = Frameworks.newConfigBuilder
      .parserConfig(insensitiveParser)
      .defaultSchema(hr)
      .build

    val planner = Frameworks.getPlanner(frameworkConfig)
    val sqlNode = planner.parse(getQuery)
    val sqlNodeValidated = planner.validate(sqlNode)
    val relRoot = planner.rel(sqlNodeValidated)
    val relNode = relRoot.project

    implicit val sourceList = getSourceList(relNode).groupBy(f => f).mapValues(_.size).toMap
    val m = scala.collection.mutable.Map.empty[String, Op]
    compute(relNode, m, forceNewVariable = false)

    val relWriter =
      new RelWriterImpl(new PrintWriter(System.out), SqlExplainLevel.ALL_ATTRIBUTES, false)
    relNode.explain(relWriter)

    println(PrettyPrinter.printFrames(m))

  }

  private def getSourceList(relNode: RelNode): Seq[String] = {
    relNode match {
      case node: LogicalTableScan => Seq(node.getTable.getQualifiedName.asScala.mkString(""))
      case node => node.getInputs.asScala.flatMap(n => getSourceList(n)).toSeq
    }
  }

  private def executeQuery(node: RelNode): Unit = {
    val run = RelRunners.run(node)
    val resultSet = run.executeQuery
    while ({
      resultSet.next
    }) {
      for (i <- 1 to resultSet.getMetaData.getColumnCount) {
        System.out.print(resultSet.getObject(i) + ",")
      }
      System.out.println()
    }
  }
}
