import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.wasmer.Module;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

public class Entrypoint {
    public static void main(String[] args) throws Exception {

        System.out.println(StructType.fromDDL("name STRING, age INT"));


        Options options = new Options();

//        options.addRequiredOption("w", "wasm", true, "path to WASM module");
        options.addRequiredOption("c", "c", true, "path to C file");
        options.addRequiredOption("d", "data", true, "path to data file");
        options.addRequiredOption("s", "schema", true, "path to schema file");
        options.addRequiredOption("e", "emsdk", true, "path to EMSDK");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse( options, args);
//        Path wasmPath = Path.of(cmd.getOptionValue("wasm"));
        Path cPath = Path.of(cmd.getOptionValue("c"));
        Path dataPath = Path.of(cmd.getOptionValue("data"));
        Path schemaPath = Path.of(cmd.getOptionValue("schema"));
        Path emsdkPath = Path.of(cmd.getOptionValue("emsdk"));

        Path wasmPath = WasmCompiler.compileC(cPath, emsdkPath);

        StructType schema = StructType.fromDDL(Files.readString(schemaPath));

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        Dataset<Row> df = spark.read().schema(schema).csv(dataPath.toString());
        df.createOrReplaceTempView("PEOPLE");
        df.show();
        df.printSchema();

        byte[] wasmBytes = new Module(Files.readAllBytes(wasmPath)).serialize();

        UserDefinedFunction myUdf = udf(
                (UDF2<Integer, Integer, Integer>) (a, b) -> {
                    Module module = Module.deserialize(wasmBytes);
                    return (Integer)
                            module.instantiate().exports.getFunction("add").apply(a, b)[0];
                },
                DataTypes.IntegerType);

        df.select(col("name"), myUdf.apply(col("age"), col("height")).as("SUM")).show();

        spark.stop();
    }
}
