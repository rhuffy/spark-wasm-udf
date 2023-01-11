//import org.apache.spark.sql.types.DataType
//import org.apache.spark.sql.functions.udf
//
//object UdfFactory {
//  def createMapFunction(inputColumnNames: Array[String], outputColumnType: String) = {
//    val outputDataType = DataType.fromDDL(outputColumnType)
//    inputColumnNames.length match {
//      case 0 =>
//        udf(() => InstanceWrapper.get.exports.getFunction().apply()(0))
//      case 1 =>
//        udf((a1: Any) => InstanceWrapper.get.apply(a1)(0))
//      case 2 =>
//        udf((a1: Any, a2: Any) => InstanceWrapper.get.apply(a1, a2)(0))
//      case 3 =>
//        udf((a1: Any, a2: Any, a3: Any) => InstanceWrapper.get.apply(a1, a2, a3)(0))
//      case 4 =>
//        udf((a1: Any, a2: Any, a3: Any, a4: Any) => InstanceWrapper.get.apply(a1, a2, a3, a4)(0))
//      case 5 =>
//        udf((a1: Any, a2: Any, a3: Any, a4: Any, a5: Any) => InstanceWrapper.get.apply(a1, a2, a3, a4, a5)(0))
//      case 6 =>
//        udf((a1: Any, a2: Any, a3: Any, a4: Any, a5: Any, a6: Any) => InstanceWrapper.get.apply(a1, a2, a3, a4, a5, a6)(0))
//      case 7 =>
//        udf((a1: Any, a2: Any, a3: Any, a4: Any, a5: Any, a6: Any, a7: Any) => InstanceWrapper.get.apply(a1, a2, a3, a4, a5, a6, a7)(0))
//      case 8 =>
//        udf((a1: Any, a2: Any, a3: Any, a4: Any, a5: Any, a6: Any, a7: Any, a8: Any) => InstanceWrapper.get.apply(a1, a2, a3, a4, a5, a6, a7, a8)(0))
//      case 9 =>
//        udf((a1: Any, a2: Any, a3: Any, a4: Any, a5: Any, a6: Any, a7: Any, a8: Any, a9: Any) =>  InstanceWrapper.get.apply(a1, a2, a3, a4, a5, a6, a7, a8, a9)(0))
//      case 10 =>
//        udf((a1: Any, a2: Any, a3: Any, a4: Any, a5: Any, a6: Any, a7: Any, a8: Any, a9: Any, a10: Any) => InstanceWrapper.get.apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)(0))
//      case _ =>
//        throw new RuntimeException("UDFs with " + inputColumnNames.length + " args are not supported.")
//    }
//  }
//
//  def createFilterFunction(inputColumnNames: Array[String]) = inputColumnNames.length match {
//    case 0 =>
//      udf(() => intToBool(InstanceWrapper.get.apply()(0)))
//    case 1 =>
//      udf((a1: Any) => intToBool(InstanceWrapper.get.apply(a1)(0)))
//    case 2 =>
//      udf((a1: Any, a2: Any) => intToBool(InstanceWrapper.get.apply(a1, a2)(0)))
//    case 3 =>
//      udf((a1: Any, a2: Any, a3: Any) => intToBool(InstanceWrapper.get.apply(a1, a2, a3)(0)))
//    case 4 =>
//      udf((a1: Any, a2: Any, a3: Any, a4: Any) => intToBool(InstanceWrapper.get.apply(a1, a2, a3, a4)(0)))
//    case 5 =>
//      udf((a1: Any, a2: Any, a3: Any, a4: Any, a5: Any) => intToBool(InstanceWrapper.get.apply(a1, a2, a3, a4, a5)(0)))
//    case 6 =>
//      udf((a1: Any, a2: Any, a3: Any, a4: Any, a5: Any, a6: Any) => intToBool(InstanceWrapper.get.apply(a1, a2, a3, a4, a5, a6)(0)))
//    case 7 =>
//      udf((a1: Any, a2: Any, a3: Any, a4: Any, a5: Any, a6: Any, a7: Any) => intToBool(InstanceWrapper.get.apply(a1, a2, a3, a4, a5, a6, a7)(0)))
//    case 8 =>
//      udf((a1: Any, a2: Any, a3: Any, a4: Any, a5: Any, a6: Any, a7: Any, a8: Any) => intToBool(InstanceWrapper.get.apply(a1, a2, a3, a4, a5, a6, a7, a8)(0)))
//    case 9 =>
//      udf((a1: Any, a2: Any, a3: Any, a4: Any, a5: Any, a6: Any, a7: Any, a8: Any, a9: Any) => intToBool(InstanceWrapper.get.apply(a1, a2, a3, a4, a5, a6, a7, a8, a9)(0)))
//    case 10 =>
//      udf((a1: Any, a2: Any, a3: Any, a4: Any, a5: Any, a6: Any, a7: Any, a8: Any, a9: Any, a10: Any) => intToBool(InstanceWrapper.get.apply(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10)(0)))
//    case _ =>
//      throw new RuntimeException("UDFs with " + inputColumnNames.length + " args are not supported.")
//  }
//
//  /**
//   * Used for performance testing.
//   */
//  def createAddFunction = udf((a1: Any, a2: Any) => a1.asInstanceOf[Int] + a2.asInstanceOf[Int])
//
//
//  private def intToBool(obj: Any) = obj.asInstanceOf[Integer] ne 0
//}