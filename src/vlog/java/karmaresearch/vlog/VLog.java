package karmaresearch.vlog;

public class VLog {

    // Required: set java.library.path to .../vlog/build:.../vlog/build/trident:.../vlog/build/trident/kognac
    static {
        // System.loadLibrary("kognac-log");
        // System.loadLibrary("trident-core");
        // System.loadLibrary("trident-sparql");
        System.loadLibrary("vlog_jni");
        // System.loadLibrary("kognac");
    }

    public native void start(String edbconfig);

    public static void main(String[] args) {
        VLog vlog = new VLog();
        vlog.start("Test string");
    }
}
