package com.dkaminski.fakestoragerpccontentrangeissue;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
class ApplicationTests {

    @Autowired
    private Storage storage;

    private static final String BUCKET_NAME = "my-staging-bucket";
    private static final int BUFFER_SIZE = 1024;
    private static final String FILE_NAME = "fake_file_name.zip";
    private static final int TOTAL_FILE_SIZE = 50 * 1024 * 1024;

    @Test
    void testCreateLargeBlob() throws Exception {

        try (final InputStream inPipe = createLargeDataStream(TOTAL_FILE_SIZE)) {

            final Blob blob = storage.createFrom(
                BlobInfo.newBuilder(BlobId.of(BUCKET_NAME, FILE_NAME)).build(),
                inPipe,
                BUFFER_SIZE
            );

            assertNotNull(blob);
            assertEquals(BUCKET_NAME, blob.getBucket());
            assertEquals(FILE_NAME, blob.getName());
        }
    }

    // creates an InputStream consisting of random bytes
    private InputStream createLargeDataStream(final int size) {

        byte[] largeData = new byte[size];
        for (int i = 0; i < size; i++) {
            largeData[i] = (byte) (i % 256);
        }
        return new ByteArrayInputStream(largeData);
    }

    @Configuration
    static class MockStorageConfiguration {

        private static final String testKey = """
            -----BEGIN PRIVATE KEY-----
            MIIBVQIBADANBgkqhkiG9w0BAQEFAASCAT8wggE7AgEAAkEA7gJdH+zKaNVHnyAK
            /4GNstCfzYh9fZe9RxiIaYFqaB8jH3UoZ1D5X2jSuVI+sXj5PuUQoHLlKqGgtu7c
            la9CTwIDAQABAkEAtdd6fMS2FIg20z1xCatarm60WRzZ+9Wt2B7HQgyNVoRSZ5Ei
            ABCqo6jFhVOkTMzy2GvOoa5hQfL6ikx78KKI2QIhAPj+91A/tWmQFXbx7RG5DljK
            uGhEEmTMKQSA0xFff5DVAiEA9LRIGpAii27XTAVbDenQMXMAEwU0ZmXny2FNBez+
            uJMCICPlXfILvTOXuhVzuyGa9B6I2xzs81nktOUZTVRr2BAhAiBPhPpdb3NaXkWm
            laL2TYHzX8ypYaqakAkYRWFTSKWp8wIhAJKjp0S0LutdIzI0gQSGWMH9V6/bzpjd
            oSAGGa6iDVXx
            -----END PRIVATE KEY-----
            """;

        @Bean
        Storage storage() throws IOException {

            Credentials credentials = ServiceAccountCredentials.newBuilder()
                .setPrivateKeyString(testKey)
                .setClientEmail("test@example.com")
                .build();

            return LocalStorageHelper.getOptions()
                .toBuilder()
                .setCredentials(credentials)
                .build()
                .getService();
        }
    }
}
