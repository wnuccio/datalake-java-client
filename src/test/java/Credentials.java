import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Credentials {
    private Properties properties;

    public Credentials(Properties properties) {
        this.properties = properties;
    }

    public static Credentials readFrom(String path) {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new Credentials(props);
    }

    public String accountKey() {
        return properties.getProperty("accountKey");
    }

    public String accountName() {
        return properties.getProperty("accountName");
    }

}