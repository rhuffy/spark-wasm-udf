import org.wasmer.Instance;
import org.wasmer.exports.Function;

import java.nio.file.Files;
import java.nio.file.Path;

public class WasmFunctionSupplier {
    private static Function function;

    public static void init(Path wasmPath, String functionName) throws Exception {
        byte[] wasmBytes = Files.readAllBytes(wasmPath);
        WasmFunctionSupplier.function = new Instance(wasmBytes).exports.getFunction(functionName);
    }

    public static Function get() {
        return function;
    }
}
