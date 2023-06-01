package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.utils.TestBKConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

public class TestBookieImpl {

    enum Instance {
        VALID,
        INVALID
    }

    private BookieImpl bookie;
    private static long ledgerId;

    public TestBookieImpl() {
        setUpBookie();
    }

    private void setUpBookie() {

        try {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            this.bookie = new BookieSetUp(conf);
            //bookie.start();
        } catch (Exception e) {
            Assert.fail("Configuration: no exception should be thrown here.");
            e.printStackTrace();
        }


    }


    private static ByteBuf getByteBuf(Instance type) {
        switch (type) {
            case VALID:
                return Unpooled.copiedBuffer("test", StandardCharsets.UTF_8);
            case INVALID:
                ByteBuf invalidByteBuf = Mockito.mock(ByteBuf.class);
                Mockito.doThrow(new RuntimeException("invalid ByteBuf")).when(invalidByteBuf).getByte(Mockito.anyInt());
                return invalidByteBuf;
            default:
                Assertions.fail("getByteBuf: unexpected ByteBuf type.");
                return null;
        }

    }

    private static BookkeeperInternalCallbacks.WriteCallback getCallback(Instance type) {

        switch (type) {
            case VALID:
                return new BookkeeperInternalCallbacks.WriteCallback() {
                    @Override
                    public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
                        if (rc == BKException.Code.OK) {
                            System.out.println("Success");
                        }
                        else {
                            System.out.println("Fail");
                        }
                    }
                };
            case INVALID:
                return new BookkeeperInternalCallbacks.WriteCallback() {
                    @Override
                    public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
                        throw new RuntimeException("Invalid callback");
                    }
                };
            default:
                Assertions.fail("getCallback: Unexpected callBack type: " + type);
                return null;
        }
    }


    private static Stream<Arguments> addEntryParams() {
        return Stream.of(
                //           entry,                         ackBeforeSync,  cb,                                 ctx,           masterKey,          exceptionExpected
                Arguments.of(getByteBuf(Instance.VALID),    true,           null,                               "test",        "key".getBytes(),   true),   // expected: cb = null, so we expect an exception (if cb is actually used!)
                Arguments.of(getByteBuf(Instance.INVALID),  false,          getCallback(Instance.VALID),        "test",        "".getBytes(),      true ),  // expected: invalid entry, so we expect an exception
                Arguments.of(null,                          true,           getCallback(Instance.VALID),        new int[2],    null,               true ),  // expected: null entry and null masterKey, so we expect an exception
                Arguments.of(getByteBuf(Instance.VALID),    true,           getCallback(Instance.INVALID),      null,          "key".getBytes(),   true ),  // expected: invalid cb and null ctx, so we expect an exception
                Arguments.of(getByteBuf(Instance.VALID),    false,          getCallback(Instance.VALID),        "test",        "".getBytes(),      false)   // expected: entry successfully written
        );
    }


    /**
     * This method tests:
     * <p> public void addEntry(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb, Object ctx, byte[] masterKey).
     */
    @ParameterizedTest
    @MethodSource("addEntryParams")
    public void testAddEntry(ByteBuf entry, boolean ackBeforeSync, BookkeeperInternalCallbacks.WriteCallback cb, Object ctx, byte[] masterKey, boolean exceptionExpected) {
        try {

            this.bookie.addEntry(entry, ackBeforeSync, cb, ctx, masterKey);

            /* Here a method annotated with @VisibleForTesting is used */
            LedgerDescriptor descriptor = this.bookie.getLedgerForEntry(entry, masterKey);
            ledgerId = descriptor.getLedgerId();

            Assertions.assertNotNull(descriptor);   // if the descriptor is not null, then it exists a ledger in which the entry has been written, so the writing was successful

            /* TODO: the test is successful also if we expect an exception but no exception is thrown and the entry is correctly written
                (e.g. if the cb is never used, as in this case, the test will never fail also if it is equal to null, as long as the entry is written anyways) */


        } catch (Exception e) {
            Assertions.assertTrue(exceptionExpected);
            e.printStackTrace();
        }
    }

    private static Stream<Arguments> fenceParams() {
        return Stream.of(
                Arguments.of(-1,        "key".getBytes(),   true ),
                Arguments.of(0,         "".getBytes(),      true ),
                Arguments.of(ledgerId,  "key".getBytes(),   false)
        );
    }


    /*
     *  TODO: fix the method below, an actual LedgerHandle / LedgerDescriptor corresponding to an actual ledger would be useful (is it correct?)
     */

    /**
     * This method tests:
     * <p> public CompletableFuture<Boolean> fenceLedger(long ledgerId, byte[] masterKey)
     */
    @ParameterizedTest
    @MethodSource("fenceParams")
    public void testFenceLedger(long id, byte[] masterKey, boolean exceptionExpected) {
        try {
            CompletableFuture<Boolean> retValue = this.bookie.fenceLedger(id, masterKey);
            Assertions.assertTrue(retValue.get());
        } catch (IOException | BookieException e) {
            Assertions.assertTrue(exceptionExpected);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


}
