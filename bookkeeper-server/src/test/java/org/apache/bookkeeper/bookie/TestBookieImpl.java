package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.utils.TestBKConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class TestBookieImpl {

    enum Instance {
        VALID,
        INVALID
    }

    private BookieImpl bookie;
    private static final AtomicBoolean hasCompleted = new AtomicBoolean(false);

    public TestBookieImpl() {
        setUpBookie();
    }

    private void setUpBookie() {

        try {
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setJournalWriteData(false);    // this to ensure that cb is used
            this.bookie = new BookieSetUp(conf);
            bookie.start();
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

    private static WriteCallback getCallback(Instance type) {

        switch (type) {
            case VALID:
                return new WriteCallback() {
                    @Override
                    public void writeComplete(int rc, long ledgerId, long entryId, BookieId addr, Object ctx) {
                        hasCompleted.set(rc == BKException.Code.OK);
                    }
                };
            case INVALID:
                return new WriteCallback() {
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
                Arguments.of(getByteBuf(Instance.VALID),    true,           null,                               "test",        "key".getBytes(),   true),   // expected: cb = null, so we expect an exception
                Arguments.of(getByteBuf(Instance.INVALID),  false,          getCallback(Instance.VALID),        "test",        "".getBytes(),      true ),  // expected: invalid entry, so we expect an exception
                Arguments.of(null,                          true,           getCallback(Instance.VALID),        new int[2],    null,               true ),  // expected: null entry and null masterKey, so we expect an exception
                Arguments.of(getByteBuf(Instance.VALID),    true,           getCallback(Instance.INVALID),      null,          "key".getBytes(),   true ),  // expected: invalid cb and null ctx, so we expect an exception
                Arguments.of(getByteBuf(Instance.VALID),    false,          getCallback(Instance.VALID),        "test",        "key".getBytes(),   false)   // expected: entry successfully written
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

            /* hasCompleted will be updated to true, in the cb, in case of success (oth. its value will not change), so we wait until this condition is satisfied;
             * if it's not satisfied within a timeout period, an exception will be thrown. When it becomes true, the test may be considered successful. */
            Awaitility.await().untilAsserted(() -> Assertions.assertTrue(hasCompleted.get()));


        } catch (IOException | InterruptedException | BookieException | RuntimeException e) {
            Assertions.assertTrue(exceptionExpected);
            e.printStackTrace();
        }
    }

    private LedgerDescriptor mockLedgerDescr(long ledgerId) {
        LedgerDescriptor mockedDescr = Mockito.mock(LedgerDescriptor.class);
        Mockito.when(mockedDescr.getLedgerId()).thenReturn(ledgerId);
        return mockedDescr;
    }

    private static Stream<Arguments> fenceParams() {
        return Stream.of(
                Arguments.of(-1L, "key".getBytes(),   true ),
                Arguments.of(0L,  null,               true ),
                Arguments.of(0L,  "".getBytes(),      false),
                Arguments.of(1L,  "key".getBytes(),   false)
        );
    }


    /*
     *  TODO: fix the method below, an actual LedgerHandle / LedgerDescriptor corresponding to an actual ledger would be useful (is it correct?)
     */

    /**
     * This method tests:
     * <p> public CompletableFuture<Boolean> fenceLedger(long ledgerId, byte[] masterKey)
     * <p> fenceLedger description (javadoc): Fences a ledger. From this point on, clients will be unable to write to this ledger.
     *  Only recoveryAddEntry will be able to add entries to the ledger.
     *  This method is idempotent. Once a ledger is fenced, it can never be unfenced. Fencing a fenced ledger has no effect.
     */
    @ParameterizedTest
    @MethodSource("fenceParams")
    public void testFenceLedger(long id, byte[] masterKey, boolean exceptionExpected) {
        try {

            /* TODO: it seems that the ledgerId value is not checked to be non negative, is this a bug? */

            CompletableFuture<Boolean> retValue = this.bookie.fenceLedger(id, masterKey);
            Assertions.assertTrue(retValue.get(), "The ledger has not been fenced, but no exception has been thrown.");
            Assertions.assertFalse(exceptionExpected, "The ledger has been fenced, but with incorrect parameters.");

            /* At this point, the ledger has been fenced, so we shouldn't be able to write on them (except using recoveryAddEntry).
            *  We can test this trying to invoke the method addEntry, and expecting an exception  */

            /* using a method annotated with @VisibleForTesting */
            ByteBuf entry = bookie.createMasterKeyEntry(id, masterKey);

            System.out.println(entry);

            Assertions.assertThrows(BookieException.LedgerFencedException.class,
                    () -> bookie.addEntry(entry, false, getCallback(Instance.VALID), null, masterKey),
                    "An exception is not thrown when trying to write on a fenced ledger using addEntry.");


            /* TODO test that invoking recoveryAddEntry does not throw exceptions */



        } catch (IOException | BookieException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
            Assertions.assertTrue(exceptionExpected);
        }
    }

    private ByteBuf generateEntry(long ledger, long entry) {
        byte[] data = ("ledger-" + ledger + "-" + entry).getBytes();
        ByteBuf bb = Unpooled.buffer(8 + 8 + data.length);
        bb.writeLong(ledger);
        bb.writeLong(entry);
        bb.writeBytes(data);
        return bb;
    }


}
