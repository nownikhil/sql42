package sql42

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexCall, RexFieldAccess, RexInputRef, RexLiteral, RexNode}
import org.apache.calcite.sql.{SqlBinaryOperator, SqlKind, SqlOperator, SqlPrefixOperator}
import org.apache.calcite.sql.fun.{SqlCaseOperator, SqlCastFunction}
import org.apache.calcite.sql.validate.SqlUserDefinedFunction
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

object ExpressionHandler {

  def handleExpression(node: RexNode, dataType: RelDataType): String = {
    node match {
      case r: RexCall =>
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
        "ERROR"
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
      case k =>
        println("Kind not matched: " + kind)
        k.toString
    }
  }

  def wrapInCol(fieldName: String): String = s"""col("$fieldName")"""

}
