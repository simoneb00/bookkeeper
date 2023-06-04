package org.apache.bookkeeper.proto;

import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieSetUp;
import org.apache.bookkeeper.bookie.TestBKConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.apache.bookkeeper.proto.ProtoUtils.getMockedHandler;
import static org.apache.bookkeeper.proto.ProtoUtils.getMockedProcessor;
import static org.apache.bookkeeper.proto.ProtoUtils.getMockedRequest;
import static org.apache.bookkeeper.proto.ProtoUtils.VALID;
import static org.apache.bookkeeper.proto.ProtoUtils.INVALID;
import static org.apache.bookkeeper.proto.ProtoUtils.NULL;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.bookkeeper.proto.BookieProtocol.ParsedAddRequest;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This class tests the integration between the classes <i>BookieImpl</i> and <i>WriteEntryProcessor</i>, in particular between the methods:
 * <p> processPacket of WriteEntryProcessor </p>
 * <p> addEntry and recoveryAddEntry of BookieImpl </p>
 * This test assumes that the two classes have been unit tested.
 */
public class IntegrationTestBookieWEP {

    private static BookieImpl bookie;
    private static WriteEntryProcessor wep;

    @BeforeAll
    public static void setUp() {
        try {

            // Bookie configuration and startup
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setJournalWriteData(false);    // this to ensure that cb is used
            bookie = new BookieSetUp(conf);
            bookie.start();

            // WriteEntryProcessor configuration
            ParsedAddRequest request = getMockedRequest(VALID);
            BookieRequestHandler handler = getMockedHandler(VALID);
            BookieRequestProcessor processor = getMockedProcessor(VALID);

            when(processor.getBookie()).thenReturn(bookie);
            wep = WriteEntryProcessor.create(request, handler, processor);


        } catch (Exception e) {
            Assertions.fail("Configuration: no exception should be thrown here.");
            e.printStackTrace();
        }
    }

    /**
     * This method tests that the class BookieImpl is reachable from WriteEntryProcessor
     */
    @Test
    public void testReachability() {
    }
}
