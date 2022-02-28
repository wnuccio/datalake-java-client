import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.FileSystemItem;

public class AzureClient {

    private DataLakeServiceClient dataLakeServiceClient;

    private String getEndpoint(String accountName) {
        return "https://" + accountName + ".dfs.core.windows.net";
    }

    private StorageSharedKeyCredential getCredentials() {
        String accountName = "walterstorageacc";
        String accountKey = "5mkhysRoJHefGplWbDcZrOwAZ5lFOxEThVQBTwyhZ4LREyLCR8IPDCipe/1CKWg67SDb7sLi3aCHrNMNk21vcQ==";
        return new StorageSharedKeyCredential(accountName, accountKey);
    }

    public DataLakeServiceClient dataLakeClient() {
        if (dataLakeServiceClient != null) return dataLakeServiceClient;

        StorageSharedKeyCredential credentials = getCredentials();
        String endpoint = getEndpoint(credentials.getAccountName());
        dataLakeServiceClient = new DataLakeServiceClientBuilder()
                .credential(credentials)
                .endpoint(endpoint)
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
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
