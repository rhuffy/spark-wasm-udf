import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import java.nio.file.Path;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

public class Entrypoint {
    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("people.json");
        df.createOrReplaceTempView("PEOPLE");
        df.show();
        df.printSchema();

        WasmFunctionSupplier.init(Path.of("udf.wat"), "add");

        UserDefinedFunction myUdf = udf(
                (UDF2<Long, Long, Long>)
                        (a, b) -> (Long) WasmFunctionSupplier.get().apply(a, b)[0],
                DataTypes.LongType);

        df.select(col("name"), myUdf.apply(col("age"), col("height")).as("SUM")).show();

        spark.stop();
    }
}
