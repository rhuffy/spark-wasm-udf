import org.apache.commons.cli.Options
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.ParseException
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.wasmer.Module
import org.wasmer.Instance

import java.nio.{ByteBuffer, CharBuffer, DoubleBuffer, FloatBuffer, IntBuffer, LongBuffer, ShortBuffer}
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant

object Entrypoint {
  private val USER_DATA_PATH = Path.of("server/static/user_data")

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val cmd = parseArgs(args)
    val wasmPath = Path.of(cmd.getOptionValue("wasm"))
    val dataPath = USER_DATA_PATH.resolve(cmd.getOptionValue("data"))
    val schemaPath = USER_DATA_PATH.resolve(cmd.getOptionValue("schema"))
    val operation = Operation.from(cmd.getOptionValue("operation"))
    val functionName = cmd.getOptionValue("function")
    val inputColumnNames = cmd.getOptionValue("input").split(",")
    val maybeOutputColumnName = Option(cmd.getOptionValue("output"))
    val maybeOutputColumnType = Option(cmd.getOptionValue("outputType"))
    var schema = StructType.fromDDL(Files.readString(schemaPath))
    val spark = SparkSession.builder.master("local").appName("Java Spark SQL basic example").getOrCreate
    val start = Instant.now
    val df = spark.read.schema(schema).csv(dataPath.toString).repartition(1000)

    val wasmBytes = Files.readAllBytes(wasmPath)

    if (maybeOutputColumnName.nonEmpty) {
      schema = schema.add(maybeOutputColumnName.get, maybeOutputColumnType.get)
    }

    import spark.implicits._

    val size = df.mapPartitions(it => Iterator(it.size)).first

    val ds = df.mapPartitions(iterator => {
      val instance = new Instance(wasmBytes)
      val memory = instance.exports.getMemory("memory")
      val mallocFunction = instance.exports.getFunction("malloc")
      val execFunction = instance.exports.getFunction("exec")

      val input0 = ByteBuffer.allocate(size * 4)
      val input1 = ByteBuffer.allocate(size * 4)

      val input0Ints = input0.asIntBuffer
      val input1Ints = input1.asIntBuffer

      iterator.zipWithIndex.foreach{case (row, i) => {
        input0Ints.position(i)
        input0Ints.put(row.getInt(0))
        input1Ints.position(i)
        input1Ints.put(row.getInt(1))
      }}
      val input0Addr = mallocFunction.apply(size.asInstanceOf[Object])(0).asInstanceOf[Int]
      val input1Addr = mallocFunction.apply(size.asInstanceOf[Object])(0).asInstanceOf[Int]
      val outputAddr = mallocFunction.apply(size.asInstanceOf[Object])(0).asInstanceOf[Int]

      val memoryBuffer = memory.buffer
      memoryBuffer.position(input0Addr)
      memoryBuffer.put(input0.array)
      memoryBuffer.position(input1Addr)
      memoryBuffer.put(input1.array)

      execFunction.apply(
        input0Addr.asInstanceOf[Object],
        input1Addr.asInstanceOf[Object],
        outputAddr.asInstanceOf[Object],
        size.asInstanceOf[Object]
      )

      val outputBytes = new Array[Byte](size * 4)
      memoryBuffer.position(outputAddr)
      memoryBuffer.get(outputBytes, 0, size * 4)

      val outputIntBuffer = ByteBuffer.wrap(outputBytes).asIntBuffer

      (0 to size-1).map { i =>
        RowFactory.create(
          input0Ints.get(i).asInstanceOf[Object],
          input1Ints.get(i).asInstanceOf[Object],
          outputIntBuffer.get(i).asInstanceOf[Object]
        )
      }.toIterator
    })(RowEncoder(schema))

//    val output = dataPath.resolveSibling(FilenameUtils.getBaseName(wasmPath.toString))
//    ds.write.json(output.toString)
    ds.toDF().show()
    spark.stop()
    val finish = Instant.now
    println("Execution time: " + String.valueOf(Duration.between(start, finish).toMillis) + "ms")
  }

  @throws[ParseException]
  private def parseArgs(args: Array[String]): CommandLine = {
    val options = new Options
    options.addRequiredOption("w", "wasm", true, "path to wasm file")
    options.addRequiredOption("d", "data", true, "path to data file")
    options.addRequiredOption("s", "schema", true, "path to schema file")
    options.addRequiredOption("x", "operation", true, "operation (MAP|FILTER)")
    options.addRequiredOption("f", "function", true, "function name")
    options.addRequiredOption("i", "input", true, "comma-separated input column names")
    options.addOption("o", "output", true, "name of output column when using MAP")
    options.addOption("t", "outputType", true, "type of output column when using MAP")
    val parser = new DefaultParser
    parser.parse(options, args)
  }
}