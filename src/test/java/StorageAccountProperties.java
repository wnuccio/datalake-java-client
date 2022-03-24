import com.azure.storage.common.StorageSharedKeyCredential;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class StorageAccountProperties {
    private final Properties properties;

    public StorageAccountProperties(Properties properties) {
        this.properties = properties;
    }

    public String storageAccountKey() {
        return properties.getProperty("storageAccountKey");
    }

    public String storageAccountName() {
        return properties.getProperty("storageAccountName");
    }

    public String getEndpoint() {
        return "https://" + storageAccountName() + ".dfs.core.windows.net";
    }

    public StorageSharedKeyCredential sharedKeyCredentials() {
        return new StorageSharedKeyCredential(storageAccountName(), storageAccountKey());
    }

    public static StorageAccountProperties readFromLocalFile() {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("./storageAccountProperties.txt"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new StorageAccountProperties(props);
    }
}
