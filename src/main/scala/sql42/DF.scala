package sql42

import org.apache.calcite.adapter.java.ReflectiveSchema
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.externalize.RelWriterImpl
import org.apache.calcite.rel.logical._
import org.apache.calcite.rex.{RexCall, RexFieldAccess, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.schema.impl.{AggregateFunctionImpl, ScalarFunctionImpl}
import org.apache.calcite.sql.fun.{SqlCaseOperator, SqlCastFunction}
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.validate.SqlUserDefinedFunction
import org.apache.calcite.sql.{SqlBinaryOperator, SqlExplainLevel, SqlKind, SqlOperator, SqlPrefixOperator, SqlSelect}
import org.apache.calcite.tools.Frameworks

import java.io.{File, PrintWriter}
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

case class Frame(name: String, ops: Seq[Op] = Seq.empty[Op]) {
  def addOp(op: Op): Frame = {
    this.copy(ops = this.ops :+ op)
  }
}

trait Op {
  def value: String
}

case class RenameOp(fields: Seq[(String, String)]) extends Op {
  override def value: String = fields.map { case (oldName, newName) =>
    s".withColumnRenamed($oldName, $newName)"
  }.mkString("\n")
}

// funcCalls (name, args) => args are only column references because we have already done a project
case class AggOp(groupByFields: Seq[String], funcCalls: (String, String)) extends Op {
  override def value: String = ???
}

case class StringOp(value: String) extends Op

object DF {
  def main(args: Array[String]): Unit = {
    val query =
      "SELECT e.first_name AS FirstName, s.salary AS Salary from employee AS e join salary AS s on e.emp_id=s.emp_id where e.organization = 'Tesla' and s.organization = 'Tesla'"

    val config = SqlParser.configBuilder.setCaseSensitive(false).build
    val parser = SqlParser.create(query, config)
    val node = parser.parseQuery()
    println(node.getClass)
    node.asInstanceOf[SqlSelect].getFrom
  }
}

class MySum() {
  def init = 0
  def add(accumulator: Int, v: Int): Int = accumulator + v
  def merge(accumulator0: Int, accumulator1: Int): Int = accumulator0 + accumulator1
  def result(accumulator: Int): Int = accumulator
}

object WithSchema {
  val logger = Logger.getLogger(this.getClass.toString)
  val writer = new PrintWriter(new File("C:\\Users\\Nikhil\\df\\temp.txt"))
  val idx = new AtomicInteger(0)

  val implementation = ScalarFunctionImpl.create(classOf[HrMin.MyUdf], "eval")
  val arrayContains = ScalarFunctionImpl.create(classOf[ArrayContains], "f")
  val isNull = ScalarFunctionImpl.create(classOf[isNull], "f")

  val udaf = AggregateFunctionImpl.create(classOf[MySum])

  def main(args: Array[String]): Unit = {

    val rootSchema = Frameworks.createRootSchema(true)
    val schema = new ReflectiveSchema(new HrMin.Hr())
    val hr = rootSchema.add("hr", schema)

    hr.add("myudf", implementation)
    hr.add("customudaf", udaf)
    hr.add("isNull", isNull)

    val insensitiveParser = SqlParser.configBuilder.setCaseSensitive(false).build
    val frameworkConfig = Frameworks.newConfigBuilder
      .parserConfig(insensitiveParser)
      .defaultSchema(hr)
      .build

    val planner = Frameworks.getPlanner(frameworkConfig)

    val caseWhen =
      s"case\n when n.boolval = false then 0\n when n.boolval = true then 1\n end as nc"

    val mathExpression = s"sub.cnt / 2, sub.cnt + 1 - 3 + 9 as addition, isNull(sub.cnt) as isnull"
    val innerQuery =
      s"select count(e.empid) as cnt, d.depid, customudaf(myudf(myudf(e.rating))) as name from hr.emps as e inner join hr.deps as d on (e.secondid = d.secondid and e.depid = d.depid) group by d.depid having count(*) >= 2"
    val query =
      s"Select sub.*, $caseWhen, $mathExpression, n.name, isNull(n.name) as nullString from ($innerQuery) sub left join hr.noobs as n on sub.depid = n.noobid order by n.name desc"

    val sqlNode = planner.parse(query)
    val sqlNodeValidated = planner.validate(sqlNode)

    val relRoot = planner.rel(sqlNodeValidated)
    val relNode = relRoot.project
    compute(relNode, forceNewVariable = false)

    val relWriter =
      new RelWriterImpl(new PrintWriter(System.out), SqlExplainLevel.ALL_ATTRIBUTES, false)
    relNode.explain(relWriter)
    println("============================ GG ============================ GG ")
    import org.apache.calcite.tools.RelRunners
    val run = RelRunners.run(relNode)
    val resultSet = run.executeQuery

    //println("Result:")
    while ({
      resultSet.next
    }) {
      for (i <- 1 to resultSet.getMetaData.getColumnCount) {
        //System.out.print(resultSet.getObject(i) + ",")
      }
      //System.out.println()
    }
    writer.close()
  }


  def compute(relNode: RelNode, forceNewVariable: Boolean = true): Int = {
    relNode match {
      case node: LogicalSort =>

        val directions = node.getCollation.getFieldCollations.asScala.toSeq.map(collation => collation.direction)
        val expressions = node.getSortExps.asScala.toSeq.map(exp => handleExpression(exp, node.getInput.getRowType))
        assert(directions.size == expressions.size, "Each field should have a direction specified with it")

        val expressionWithDir = expressions.zip(directions).map {
          case (exp, dir) if dir == Direction.DESCENDING => s"$exp.desc()"
          case (exp, _) => exp
        }.mkString(", ")

        val frameId = compute(node.getInput, forceNewVariable)
        // MainFrame.add(frameId, SortExpression)
        writer.write(s"\n.orderBy($expressionWithDir)")
        frameId
      case node: LogicalProject =>
        println(node.getRowType)
        val projectExpressions = node.getNamedProjects.asScala.map { namedProject =>
          val fieldName = namedProject.right
          val rexNode = namedProject.left
          val rowType = node.getInput.getRowType
          val exp = handleExpression(rexNode, rowType)
          // Based on space or other logic determine if a bracket / renaming is needed
          rexNode match {
            case r: RexInputRef if rowType.getFieldNames.get(r.getIndex) == fieldName => exp
            case _ if !exp.contains(" ") => s"""$exp.as("$fieldName")""" // Do not add () around the expression
            case _ => s"""($exp).as("$fieldName")"""
          }
        }.mkString(", ")

        val frameId = compute(node.getInput, forceNewVariable)
        writer.write(s"\n.select($projectExpressions)")
        frameId
      case node: LogicalFilter =>
        //println("Condition: " + node.getCondition + " Kind: " + node.getCondition.getKind)
        val condition = handleExpression(node.getCondition, node.getRowType)
        val frameId = compute(node.getInput, forceNewVariable)
        writer.write(s"\n.filter($condition)")
        frameId
      case node: LogicalJoin =>
        //logger.warning("Join type: " + node.getJoinType + ", Condition " + node.getCondition)
        val joinCondition = node.analyzeCondition()
        //println(handleExpression(node.getCondition, node.getRowType))

        val leftNamesSize = node.getLeft.getRowType.getFieldNames.size()
        val leftKeySeq = joinCondition.leftKeys.asScala.toSeq
        val rightKeySeq = joinCondition.rightKeys.asScala.toSeq

        val fieldNames = node.getRowType.getFieldNames.asScala.toSeq

        val newRight: Seq[String] = node.getRowType.getFieldNames.asScala.toSeq.drop(leftNamesSize)
        val rightFields: Seq[String] = node.getRight.getRowType.getFieldNames.asScala.toSeq
        val names = rightFields.zip(newRight).filter(f => f._1 != f._2)

        val leftName = compute(node.getLeft, true)
        val r = compute(node.getRight, true)

        if (names.nonEmpty) {
          val ee = names.map{ case (o, n) => s".withColumnRenamed($o, $n)" }.mkString("\n")
          writer.write(s"\nval df${r}_renamed = df$r\n$ee")
        }
        val rightName = if (names.isEmpty) r else s"${r}_renamed"

        // Only Equi join is handled for now
        val conditionExpression = leftKeySeq.zip(rightKeySeq).map {
          case (l, r) =>
            val lCol = fieldNames(l)
            val rCol = fieldNames(r + leftNamesSize)
            s"df$leftName($lCol) === df$rightName($rCol)"
        }.mkString(" && ")

        // Maybe skip in case of inner join.. Ignore defaults
        val joinType = s""""${node.getJoinType.toString.toLowerCase}""""

        if (forceNewVariable) {
          val newIdx = idx.getAndIncrement()
          writer.write(s"\n\nval df$newIdx = df$leftName.join(df$rightName, $conditionExpression, $joinType)")
          newIdx
        } else {
          writer.write(s"\n\ndf$leftName.join(df$rightName, $conditionExpression, $joinType)")
          -1
        }
      case node: LogicalAggregate =>
        val fieldNames = node.getRowType.getFieldNames.asScala.toSet
        val fieldsNotInAggCall = node.getNamedAggCalls.asScala.toSeq.map(_.right).toSet
        val aggFields = fieldNames -- fieldsNotInAggCall
        val groupByExpression = aggFields.map(wrapInCol).mkString(", ")

        val aggCalls = node.getNamedAggCalls.asScala.map { pair =>
          val name = pair.right
          val aggCall = pair.left
          s"${handleAggregateCall(aggCall, node.getInput.getRowType)}.as($name)"
        }.mkString(", ")

        val frameId = compute(node.getInput)
        writer.write(s"\n.groupBy($groupByExpression)\n.agg($aggCalls)")
        frameId
      case node: LogicalTableScan =>
        val frameId = idx.getAndIncrement()
        writer.write(s"\n\nval df$frameId = spark.read.parquet(${node.getTable.getQualifiedName.asScala.mkString("")})")
        frameId
      case node =>
        println(s"========== $node ==========")
        println("<<<<=====WTF=====>>>>")
        println(node.getClass)
        0
    }
  }

  // Take frameId as input? col syntax arg, define list of enums

  def handleExpression(node: RexNode, dataType: RelDataType): String = {
    node match {
      case r: RexCall =>
       // println("Operands size: " + r.operands.size() + ", Kind: " + r.getKind + " Op: " + r.op + " Op class: " + r.op.getClass + " Type: " + r.getType)
        val operands = r.operands.asScala.toSeq
        handleSqlOp(r.op, operands, dataType)
      case r: RexInputRef =>
        val fieldName = dataType.getFieldNames.get(r.getIndex)
        wrapInCol(fieldName)
      case r: RexLiteral =>
        r.getValue match {
          case null => "null"
          case _ => r.getValue.toString
        }
      case r: RexFieldAccess =>
        //println("Rex field access: " + r)
        //println(dataType)
        //println(r.getField.getName)
        //println(r.getField.getIndex)
        //println(r.getReferenceExpr.asInstanceOf[RexInputRef].getIndex)
        s"${handleExpression(r.getReferenceExpr, dataType)}.${r.getField.getName}"
      case _ =>
        println("RexNode not handled: " + node)
        ""
    }
  }

  def handleSqlOp(op: SqlOperator, operands: Seq[RexNode], dataType: RelDataType): String = {
    op match {
      case o: SqlBinaryOperator =>
        val ops = operands.map { o =>
          val exp = handleExpression(o, dataType)
          if (o.isInstanceOf[RexLiteral] | o.isInstanceOf[RexInputRef]) exp else s"($exp)"
        }
        val sqlKindString = handleKind(o.getKind)
        s"${ops(0)} $sqlKindString ${ops(1)}"
      case o: SqlUserDefinedFunction =>
        val funcArgs = operands.map(op => handleExpression(op, dataType))
        val udfName = o.getName
        s"""$udfName(${funcArgs.mkString(", ")})"""
      case _: SqlCaseOperator =>
        assert(operands.size % 2 == 1, "Each when will be matched by then plus a final else")
        def _handle_case_when(opExprs: Seq[String]): String = {
          opExprs.size match {
            case 1 => s"otherwise(${opExprs.head})"
            case _ =>
              val whenCondition = opExprs(0)
              val thenStatement = opExprs(1)
              s"""when($whenCondition, $thenStatement).${_handle_case_when(opExprs.drop(2))}"""
          }
        }
        val opExprs = operands.toList.map(o => handleExpression(o, dataType))
        _handle_case_when(opExprs)
      case _: SqlCastFunction =>
        val inputRef = operands.head
        val innerExpression = handleExpression(inputRef, dataType)
        s"""CAST($innerExpression as ${inputRef.getType.getSqlTypeName})"""
      case o: SqlPrefixOperator =>
        val inputRef = operands.head
        val innerExpression = handleExpression(inputRef, dataType)
        s"${handleKind(o.getKind)}($innerExpression)"
      case _ =>
        println("Op not matched: " + op + " " + op.getClass)
        ""
    }
  }

  def handleKind(kind: SqlKind): String = {
    kind match {
      case SqlKind.GREATER_THAN_OR_EQUAL | SqlKind.GREATER_THAN | SqlKind.NOT => kind.sql
      case SqlKind.MINUS => "-"
      case SqlKind.PLUS => "+"
      case SqlKind.DIVIDE => "/"
      case SqlKind.AND => "&"
      case SqlKind.EQUALS => "=="
      case _ =>
        println("Kind not matched: " + kind)
        ""
    }
  }

  def wrapInCol(fieldName: String): String = s"""col("$fieldName")"""

  def handleAggregateCall(aggCall: AggregateCall, relDataType: RelDataType): String = {
    val callArgs = aggCall.getArgList.asScala.map { argIdx =>
      val fieldName = relDataType.getFieldNames.get(argIdx)
      wrapInCol(fieldName)
    }.mkString(", ")

    val arg = if (callArgs.isEmpty) "*" else callArgs
    s"${aggCall.getAggregation.getName}($arg)"
  }
}
