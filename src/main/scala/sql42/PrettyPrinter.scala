package sql42

object PrettyPrinter {

  val tabLength = "  "
  val expressionLength = 70

  def printFrames(m: scala.collection.mutable.Map[String, Op]): String = {
    val sourceFrames = m.filter(_._2.isInstanceOf[SourceOp])
    val frameOp = m.values.filter(_.isInstanceOf[FrameOp]).map(_.asInstanceOf[FrameOp]).toSeq.sortBy(x => x.dataframeName.split("_").last.toInt)

    val sources = sourceFrames.map(s => s"""${s._1} = spark.read.parquet("${s._2.asInstanceOf[SourceOp].tableName}") """)

    val frameLines = frameOp.map { frame =>
      val referencedFrame = frame.sourceName.getOrElse("")
      val init = s"${frame.dataframeName} = $referencedFrame"
      init ++ frame.ops.map(op => handleSingleOp(op)).mkString("\n")
    }.mkString("\n\n")

    sources.mkString("\n") + "\n\n" + frameLines
  }

  def handleSingleOp(op: Op): String = {
    op match {
      case SourceOp(tableName) => s"spark.read.parquet($tableName)"
      case StringOp(stringValue) => stringValue
      case ProjectOp(expressions) =>
        generateExpressionString("select", expressions)
      case SortOp(fields, direction) =>
        val fieldNames = fields.mkString(",")
        s".orderBy($fieldNames)"
      case AggOp(groupByFields, aggs) =>
        generateExpressionString("groupBy", groupByFields.map(f => s""""$f"""")) + "\n" + generateExpressionString("agg", aggs)
      case JoinOp(leftDFName, rightDFName, condition, joinType, leftFieldNames, rightFieldNames) =>
        s"$leftDFName.join($rightDFName, $condition, $joinType)"
      case UnionOp(values) =>
        // Dont put the first one in bracket
        values.map(s => s"($s)").mkString(".unionAll")
    }
  }

  def generateExpressionString(baseName: String, fields: Seq[String]): String = {
    if (fields.map(_.length).sum > expressionLength) s".$baseName(${fields.mkString(s",\n$tabLength")}\n)"
    else s".$baseName(${fields.mkString(", ")})"
  }

}
