import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.FileSystemItem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DataLakeServiceClientTest {

    private AzureClientFactory azureClientFactory;
    private DataLakeServiceClient dataLakeClient;

    @BeforeEach
    void setUp() {
        StorageAccountProperties properties = StorageAccountProperties.readFromLocalFile();
        azureClientFactory = new AzureClientFactory(properties);
        dataLakeClient = azureClientFactory.dataLakeClient();

        azureClientFactory.deleteAllContainers();
    }

    @AfterEach
    void tearDown() {
        azureClientFactory.deleteAllContainers();
    }

    @Test
    void create_a_container() {
        PagedIterable<FileSystemItem> containers = dataLakeClient.listFileSystems();
        assertEquals(0, containers.stream().count());

        dataLakeClient.createFileSystem("a-test-container");

        assertEquals(1, containers.stream().count());
        FileSystemItem retrievedContainer = containers.stream().findFirst().orElseThrow();
        assertEquals("a-test-container", retrievedContainer.getName());
    }
}