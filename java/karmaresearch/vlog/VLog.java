package karmaresearch.vlog;

public class VLog {

    static {
        System.loadLibrary("vlog");
    }

    public native void start(String edbconfig);

    public static void main(String[] args) {
        VLog vlog = new VLog();
        vlog.start("Test string");
    }
}
