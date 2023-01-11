object Operation extends Enumeration {
  type Operation = Value
  val MAP, FILTER = Value

  def from(str: String): Operation.Value = str match {
    case "MAP" =>
      MAP
    case "FILTER" =>
      FILTER
    case _ =>
      throw new IllegalArgumentException("Invalid Operation: " + str)
  }
}