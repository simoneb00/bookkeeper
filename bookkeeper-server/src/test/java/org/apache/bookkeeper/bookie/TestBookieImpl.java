package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.utils.TestBKConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class TestBookieImpl {

    private static BookieImpl bookie;

    public TestBookieImpl() {
        setUpBookie();
    }

    private void setUpBookie() {

        try {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            bookie = new BookieSetUp(conf);
        } catch (Exception e) {
            Assert.fail("Configuration: no exception should be thrown here.");
            e.printStackTrace();
        }


    }


    private static ByteBuf getValidByteBuf() {
        return Unpooled.copiedBuffer("test", StandardCharsets.UTF_8);
    }

    private static Stream<Arguments> addEntryInternalParams() {
        return Stream.of(
                Arguments.of(getValidByteBuf(), true, getValidCallback(), null, "key".getBytes(), false),
                Arguments.of(getValidByteBuf(), true, null,               null, null,             true )
        );
    }

    private static BookkeeperInternalCallbacks.WriteCallback getValidCallback() {
        return new BookkeeperInternalCallbacks.WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
                if (rc == BKException.Code.OK)
                    System.out.println("Success");
                else
                    System.out.println("Fail");
            }
        };
    }



    /**
     * This class tests the method public void addEntry(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb, Object ctx, byte[] masterKey)
     */
    @ParameterizedTest
    @MethodSource("addEntryInternalParams")
    public void testAddEntryInternal(ByteBuf entry, boolean ackBeforeSync, BookkeeperInternalCallbacks.WriteCallback cb, Object ctx, byte[] masterKey, boolean exceptionExpected) {
        try {

            this.bookie.addEntry(entry, ackBeforeSync, cb, ctx, masterKey);

            assert true;

        } catch (IOException | BookieException | InterruptedException e) {
            Assertions.assertTrue(exceptionExpected);
        }
    }

    @AfterAll
    public static void cleanUp() {
        bookie.shutdown();
    }
}
