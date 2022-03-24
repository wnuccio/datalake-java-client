import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.FileSystemItem;

public class AzureClientFactory {

    private final Credentials credentials;
    private DataLakeServiceClient dataLakeServiceClient;

    public AzureClientFactory(Credentials credentials) {
        this.credentials = credentials;
    }

    public DataLakeServiceClient dataLakeClient() {
        if (dataLakeServiceClient != null) return dataLakeServiceClient;

        dataLakeServiceClient = new DataLakeServiceClientBuilder()
                .endpoint(credentials.getEndpoint())
                .credential(new StorageSharedKeyCredential(credentials.storageAccountName(), credentials.storageAccountKey()))
                .buildClient();

        return dataLakeServiceClient;
    }

    public void deleteAllContainers() {
        PagedIterable<FileSystemItem> containers = dataLakeClient().listFileSystems();
        if (containers.stream().count() == 0) return;

        System.out.println("Delete all containers from Azure ... ");
        containers.stream().map(FileSystemItem::getName).forEach(dataLakeClient()::deleteFileSystem);
        waitSomeSeconds(3);
        System.out.println("Delete all containers from Azure - Done");
    }

    private static void waitSomeSeconds(int seconds) {
        try {
            Thread.sleep((long)seconds * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
