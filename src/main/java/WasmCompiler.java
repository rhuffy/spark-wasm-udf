import java.io.IOException;
import java.nio.file.Path;

public class WasmCompiler {
    public static Path compileC(Path srcFile, Path emsdkPath, String[] exports) throws InterruptedException, IOException {
        Path wasmPath = wasmPath(srcFile);
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(
                emsdkPath.resolve("emscripten/main/emcc").toString(),
                "--no-entry", srcFile.toString(),
                "-o", wasmPath.toString(),
                "-sEXPORTED_FUNCTIONS=" + String.join(",", exports));
        Process process = processBuilder.start();
        int exitCode = process.waitFor();
        assert exitCode == 0;
        return wasmPath;
    }

    private static Path wasmPath(Path srcFile) {
        return srcFile.resolveSibling(srcFile.getFileName().toString().split("\\.")[0] + ".wasm");
    }
}
