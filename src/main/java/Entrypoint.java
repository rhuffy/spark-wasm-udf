import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.nio.file.Path;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

public class Entrypoint {
    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        StructType schema = new StructType()
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("height", DataTypes.IntegerType);

        Dataset<Row> df = spark.read().schema(schema).json("people.json");
        df.createOrReplaceTempView("PEOPLE");
        df.show();
        df.printSchema();

        WasmFunctionSupplier.init(Path.of("functions/c/udf_c.wasm"), "add");
        //        WasmFunctionSupplier.init(Path.of("udf.wat"), "add");

        UserDefinedFunction myUdf = udf(
                (UDF2<Integer, Integer, Integer>)
                        (a, b) -> (Integer) WasmFunctionSupplier.get().apply(a, b)[0],
                DataTypes.IntegerType);

        df.select(col("name"), myUdf.apply(col("age"), col("height")).as("SUM")).show();

        spark.stop();
    }
}
