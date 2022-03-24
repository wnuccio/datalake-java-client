public class TimeUtils {
    static void waitSomeSeconds() {
        try {
            Thread.sleep(3000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
