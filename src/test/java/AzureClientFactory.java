import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.FileSystemItem;

public class AzureClientFactory {

    private final StorageAccountProperties properties;
    private DataLakeServiceClient dataLakeServiceClient;

    public AzureClientFactory(StorageAccountProperties properties) {
        this.properties = properties;
    }

    public DataLakeServiceClient dataLakeClient() {
        if (dataLakeServiceClient != null) return dataLakeServiceClient;

        dataLakeServiceClient = new DataLakeServiceClientBuilder()
                .endpoint(properties.getEndpoint())
                .credential(properties.sharedKeyCredentials())
                .buildClient();

        return dataLakeServiceClient;
    }

    public void deleteAllContainers() {
        PagedIterable<FileSystemItem> containers = dataLakeClient().listFileSystems();
        if (containers.stream().findAny().isEmpty()) return;

        System.out.println("Delete all containers from Azure ... ");
        containers.stream().map(FileSystemItem::getName).forEach(dataLakeClient()::deleteFileSystem);
        waitSomeSeconds();
        System.out.println("Delete all containers from Azure - Done");
    }

    private static void waitSomeSeconds() {
        try {
            Thread.sleep(3000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
