package karmaresearch.vlog;

public class VLog {

    static {
        System.loadLibrary("kognac-log");
        System.loadLibrary("trident-core");
        System.loadLibrary("trident-sparql");
        System.loadLibrary("vlog_jni");
    }

    //This method starts the reasoner. As input, we pass the content (as a string)
    //of the edb configuration
    public native void start(String edbconfig);

    //This method is used to query all facts with a simple predicate
    public native QueryIterator query(int predID, Term[] terms);

    //This method stops and clear the reasoner. If this method is not called,
    //then there will be a memory leak since the reasoner will not be deallocated

    public native void stop();

    public static void main(String[] args) {
        VLog vlog = new VLog();
        vlog.start("Test string");
    }
}
