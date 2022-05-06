import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;
import org.wasmer.Module;

import static org.apache.spark.sql.functions.udf;

public class UdfFactory {
    public static UserDefinedFunction createMapFunction(byte[] wasmBytes, String functionName, String[] inputColumnNames, String outputColumnType){
        DataType outputDataType = DataType.fromDDL(outputColumnType);
        return createUdf(wasmBytes, functionName, inputColumnNames, outputDataType);
    }
    public static UserDefinedFunction createFilterFunction(byte[] wasmBytes, String functionName, String[] inputColumnNames){
        return createUdf(wasmBytes, functionName, inputColumnNames, DataTypes.BooleanType);
    }

    @NotNull
    private static UserDefinedFunction createUdf(byte[] wasmBytes, String functionName, String[] inputColumnNames, DataType outputDataType) {
        switch(inputColumnNames.length) {
            case 1:
                return udf(
                        (a1) -> {
                            Module module = Module.deserialize(wasmBytes);
                            return module.instantiate().exports.getFunction(functionName).apply(a1)[0];
                        },
                        outputDataType);
            case 2:
                return udf(
                        (a1, a2) -> {
                            Module module = Module.deserialize(wasmBytes);
                            return module.instantiate().exports.getFunction(functionName).apply(a1, a2)[0];
                        },
                        outputDataType);
            case 3:
                return udf(
                        (a1, a2, a3) -> {
                            Module module = Module.deserialize(wasmBytes);
                            return module.instantiate().exports.getFunction(functionName).apply(a1, a2, a3)[0];
                        },
                        outputDataType);
            default:
                throw new RuntimeException("UDFs with " + inputColumnNames.length + " args are not supported.");
        }
    }
}
