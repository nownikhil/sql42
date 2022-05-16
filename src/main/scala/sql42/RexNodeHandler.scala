package sql42

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.logical._
import org.apache.calcite.rex.RexInputRef
import sql42.ExpressionHandler.{handleExpression, wrapInCol}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

object RexNodeHandler {

  val idx = new AtomicInteger(0)
  def getNewFrameId: String = s"df_${idx.getAndIncrement()}"

  def addOpForSingleRel(newOp: Op, frameMap: scala.collection.mutable.Map[String, Op], frameId: String)(implicit sourceCounts: Map[String, Int]): String = {
    frameMap(frameId) match {
      case op: SourceOp if sourceCounts(op.tableName) == 1 =>
        val newFrameId = getNewFrameId
        frameMap.remove(frameId)
        frameMap(newFrameId) = new FrameOp(newFrameId).addOp(op).addOp(newOp)
        newFrameId
      case _: SourceOp =>
        // The source is being referenced multiple times. We need to generate a new ID
        val newFrameId = getNewFrameId
        newOp.sourceName = Some(newFrameId)
        frameMap += (newFrameId ->  new FrameOp(newFrameId).addOp(newOp))
        newFrameId
      case op: FrameOp =>
        op.addOp(newOp)
        frameId
    }
  }

  def compute(relNode: RelNode, frameMap: scala.collection.mutable.Map[String, Op], forceNewVariable: Boolean = true)
             (implicit sourceCounts: Map[String, Int]): String = { // OldName => NewName

    relNode match {
      case node: LogicalSort =>
        val directions = node.getCollation.getFieldCollations.asScala.map(collation => collation.direction).toSeq
        // We only sort on FieldNames, complicated expressions will fail. However it is easier to handle later
        val sortColumns = node.getSortExps.asScala.map {
          case r: RexInputRef =>
            node.getInput.getRowType.getFieldNames.get(r.getIndex)
        }.toSeq

        assert(directions.size == sortColumns.size, "Each field should have a direction specified with it")
        val sortOp = SortOp(sortColumns, directions)
        val frameId = compute(node.getInput, frameMap, forceNewVariable)
        addOpForSingleRel(sortOp, frameMap, frameId)

      case node: LogicalProject =>
        val frameId = compute(node.getInput, frameMap, forceNewVariable)
        val expressions = node.getNamedProjects.asScala.map { namedProject =>
          val fieldName = namedProject.right
          val rexNode = namedProject.left
          val rowType = node.getInput.getRowType
          val exp = handleExpression(rexNode, rowType)

          rexNode match {
            case r: RexInputRef =>
              val prevName = rowType.getFieldNames.get(r.getIndex)
              frameMap(frameId) match {
                case JoinOp(leftName, rightName, _, _, leftNames, rightNames) =>
                  val mergedNames = (leftNames ++ rightNames)
                  val preName = mergedNames(r.getIndex)
                  val dfName = if (r.getIndex > leftNames.size) rightName else leftName
                  if (preName == fieldName) s"$dfName($preName)" else s"$dfName($preName).as($fieldName)"
                case _ if prevName == fieldName => exp // No rename required
                case _ => s"""$exp.as("$fieldName")"""
              }
            case _ if !exp.contains(" ") => s"""$exp.as("$fieldName")"""
            case _ => s"""($exp).as("$fieldName")"""
          }
        }

        val projectOp = ProjectOp(expressions.toSeq)
        addOpForSingleRel(projectOp, frameMap, frameId)

      case node: LogicalFilter =>
        val condition = handleExpression(node.getCondition, node.getRowType)
        val filterOp = StringOp(s".filter($condition)")
        val frameId = compute(node.getInput, frameMap, forceNewVariable)
        addOpForSingleRel(filterOp, frameMap, frameId)

      case node: LogicalJoin =>
        val leftName = compute(node.getLeft, frameMap, true)
        val rightName = compute(node.getRight, frameMap,  true)

        val joinCondition = node.analyzeCondition()
        val leftNamesSize = node.getLeft.getRowType.getFieldNames.size()
        val leftKeySeq = joinCondition.leftKeys.asScala.toSeq
        val rightKeySeq = joinCondition.rightKeys.asScala.toSeq
        val fieldNames = node.getRowType.getFieldNames.asScala.toSeq
        val conditionExpression = leftKeySeq.zip(rightKeySeq).map {
          case (l, r) =>
            val lCol = fieldNames(l)
            val rCol = fieldNames(r + leftNamesSize)
            s"$leftName($lCol) === $rightName($rCol)"
        }.mkString(" && ")

        val leftFields: Seq[String] = node.getLeft.getRowType.getFieldNames.asScala.toSeq
        val rightFields: Seq[String] = node.getRight.getRowType.getFieldNames.asScala.toSeq

        // Only Equi join is handled for now
        val joinType = s""""${node.getJoinType.toString.toLowerCase}""""

        val joinOp = JoinOp(leftName, rightName, conditionExpression, joinType, leftFields, rightFields)
        val newFrameId = getNewFrameId
        frameMap += (newFrameId -> new FrameOp(newFrameId).addOp(joinOp))
        newFrameId

      case node: LogicalAggregate =>
        val frameId = compute(node.getInput, frameMap, forceNewVariable)

        val fieldNames = node.getRowType.getFieldNames.asScala.toSet
        val fieldsNotInAggCall = node.getNamedAggCalls.asScala.toSeq.map(_.right).toSet
        val aggFields = fieldNames -- fieldsNotInAggCall

        val aggCalls = node.getNamedAggCalls.asScala.map { pair =>
          val name = pair.right
          val aggCall = pair.left
          s"${handleAggregateCall(aggCall, node.getInput.getRowType)}.as($name)"
        }
        val aggOp = AggOp(aggFields.toSeq, aggCalls.toSeq)
        addOpForSingleRel(aggOp, frameMap, frameId)

      case node: LogicalTableScan =>
        // This step is idempotent
        val tableName = node.getTable.getQualifiedName.asScala.mkString("")
        val frameId = s"${tableName}_df"
        frameMap += (frameId -> SourceOp(tableName))
        frameId

      case node: LogicalUnion =>
        val flattenedChildNodes = node.getInputs.asScala.flatMap {
          case n: LogicalUnion => n.getInputs.asScala.toSeq
          case n => Seq(n)
        }
        val childExprs = flattenedChildNodes.map(n => compute(n, frameMap, forceNewVariable))
        val frameId = getNewFrameId
        frameMap += (frameId -> new FrameOp(frameId).addOp(UnionOp(childExprs.toSeq)))
        frameId

      case node =>
        println(s"========== $node not handled ==========")
        getNewFrameId
    }
  }

  // Take frameId as input? col syntax arg, define list of enums
  def handleAggregateCall(aggCall: AggregateCall, relDataType: RelDataType): String = {
    val callArgs = aggCall.getArgList.asScala.map { argIdx =>
      val fieldName = relDataType.getFieldNames.get(argIdx)
      wrapInCol(fieldName)
    }.mkString(", ")

    val arg = if (callArgs.isEmpty) "*" else callArgs
    s"${aggCall.getAggregation.getName}($arg)"
  }

}
