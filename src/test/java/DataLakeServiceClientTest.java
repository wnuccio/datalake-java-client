import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.FileSystemItem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DataLakeServiceClientTest {

    private AzureClient azureClient;
    private DataLakeServiceClient dataLakeClient;

    @BeforeEach
    void setUp() {
        azureClient = new AzureClient();
        dataLakeClient = azureClient.dataLakeClient();

        azureClient.deleteAllContainers();
    }

    @AfterEach
    void tearDown() {
        azureClient.deleteAllContainers();
    }

    @Test
    void create_a_container() {
        PagedIterable<FileSystemItem> containers = dataLakeClient.listFileSystems();
        assertEquals(0, containers.stream().count());

        dataLakeClient.createFileSystem("a-test-container");

        assertEquals(1, containers.stream().count());
        FileSystemItem retrievedContainer = containers.stream().findFirst().get();
        assertEquals("a-test-container", retrievedContainer.getName());
    }
}