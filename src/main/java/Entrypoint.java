import org.apache.commons.cli.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.wasmer.Module;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

public class Entrypoint {
    private static final Path USER_DATA_PATH = Path.of("server/static/user_data");

    public static void main(String[] args) throws Exception {
        for(String a : args) {
            System.out.println(a);
        }
        CommandLine cmd = parseArgs(args);
        Path cPath = Path.of(cmd.getOptionValue("c"));
        Path dataPath = USER_DATA_PATH.resolve(cmd.getOptionValue("data"));
        Path schemaPath = USER_DATA_PATH.resolve(cmd.getOptionValue("schema"));
        Path emsdkPath = Path.of(cmd.getOptionValue("emsdk"));
        Operation operation = Operation.from(cmd.getOptionValue("operation"));
        String functionName = cmd.getOptionValue("function");
        String[] inputColumnNames = cmd.getOptionValue("input").split(",");
        Optional<String> maybeOutputColumnName = Optional.ofNullable(cmd.getOptionValue("output"));
        Optional<String> maybeOutputColumnType = Optional.ofNullable(cmd.getOptionValue("outputType"));

        Path wasmPath = WasmCompiler.compileC(cPath, emsdkPath, functionName);

        StructType schema = StructType.fromDDL(Files.readString(schemaPath));

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        Dataset<Row> df = spark.read().schema(schema).csv(dataPath.toString());
        df.createOrReplaceTempView("VIEW");
        df.show();
        df.printSchema();

        byte[] wasmBytes = new Module(Files.readAllBytes(wasmPath)).serialize();

        switch (operation) {
            case MAP: {
                UserDefinedFunction myUdf = UdfFactory.createMapFunction(wasmBytes, functionName, inputColumnNames, maybeOutputColumnType.orElseThrow());
                df = df.select(col("*"), myUdf.apply(columns(inputColumnNames)).as(maybeOutputColumnName.orElseThrow()));
            }
                break;
            case FILTER:
                 {
                UserDefinedFunction myUdf = UdfFactory.createFilterFunction(wasmBytes, functionName, inputColumnNames);
                 df = df.select(col("*")).where(myUdf.apply(columns(inputColumnNames)));
                 }
                break;

        }
        df.show();
        df.write().csv(FilenameUtils.removeExtension(dataPath.toString()) + "_" + UUID.randomUUID());

        spark.stop();
    }

    private static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();

        options.addRequiredOption("c", "c", true, "path to C file");
        options.addRequiredOption("d", "data", true, "path to data file");
        options.addRequiredOption("s", "schema", true, "path to schema file");
        options.addRequiredOption("x", "operation", true, "operation (MAP|FILTER)");
        options.addRequiredOption("e", "emsdk", true, "path to EMSDK");
        options.addRequiredOption("f", "function", true, "function name");
        options.addRequiredOption("i", "input", true, "comma-separated input column names");
        options.addOption("o", "output", true, "name of output column when using MAP");
        options.addOption("t", "outputType", true, "type of output column when using MAP");


        CommandLineParser parser = new DefaultParser();
        return parser.parse( options, args);
    }

    private static Column[] columns(String[] names) {
        return Arrays.stream(names).map(functions::col).toArray(Column[]::new);
    }
}
