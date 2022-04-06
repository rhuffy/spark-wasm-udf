import io.github.kawamuray.wasmtime.Instance;
import io.github.kawamuray.wasmtime.Module;
import io.github.kawamuray.wasmtime.Store;
import io.github.kawamuray.wasmtime.WasmFunctions;

import static io.github.kawamuray.wasmtime.WasmValType.I64;
import static java.util.Collections.emptyList;

public class WasmFunctionSupplier {

    private static WasmFunctions.Function2<Long, Long, Long> func;

    public static void init(String fileName, String functionName) {
        Store<Void> store = Store.withoutData();
        Module module = Module.fromFile(store.engine(), fileName);
        Instance instance = new Instance(store, module, emptyList());
        func = WasmFunctions.func(
                store, instance.getFunc(store, functionName).get(), I64, I64, I64);
    }

    public static WasmFunctions.Function2<Long, Long, Long> get() {
        return func;
    }
}
