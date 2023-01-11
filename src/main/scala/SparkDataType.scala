object SparkDataType extends Enumeration {
  type SparkDataType = Value
  val INTEGER, FLOAT, STRING, BOOLEAN = Value

  def from(str: String): SparkDataType.Value = str match {
    case "INTEGER" =>
      INTEGER
    case "FLOAT" =>
      FLOAT
    case "STRING" =>
      STRING
    case "BOOLEAN" =>
      BOOLEAN
    case _ =>
      throw new IllegalArgumentException("Invalid DataType: " + str)
  }
}