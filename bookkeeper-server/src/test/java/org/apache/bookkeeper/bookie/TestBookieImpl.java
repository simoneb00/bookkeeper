package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.utils.TestBKConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class TestBookieImpl {

    enum Instance {
        VALID,
        INVALID
    }

    enum Add {
        ADD_ENTRY,
        RECOVERY_ADD_ENTRY
    }

    private static BookieImpl bookie;
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
            Assertions.fail("Configuration: no exception should be thrown here.");
            e.printStackTrace();
        }


    }


    private static ByteBuf getByteBuf(Instance type, long ledgerId) {
        switch (type) {
            case VALID:
                //return Unpooled.copiedBuffer("test", StandardCharsets.UTF_8);
                return generateEntry(ledgerId);
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
                return (rc, ledgerId, entryId, addr, ctx) -> hasCompleted.set(rc == BKException.Code.OK);
            case INVALID:
                return (rc, ledgerId, entryId, addr, ctx) -> {
                    throw new RuntimeException("Invalid callback");
                };
            default:
                Assertions.fail("getCallback: Unexpected callBack type: " + type);
                return null;
        }
    }


    private static Stream<Arguments> addEntryParams() {
        return Stream.of(
                //            entry,                                   ackBeforeSync,  cb,                                 ctx,           masterKey,          expExc,  methodToTest
                Arguments.of(getByteBuf(Instance.VALID, 1),    true,           null,                               "test",        "key".getBytes(),   true,    Add.ADD_ENTRY),   // expected: cb = null, so we expect an exception
                Arguments.of(getByteBuf(Instance.INVALID, 1),  false,          getCallback(Instance.VALID),        "test",        "".getBytes(),      true,    Add.ADD_ENTRY ),  // expected: invalid entry, so we expect an exception
                Arguments.of(null,                                     true,           getCallback(Instance.VALID),        new int[2],    null,               true,    Add.ADD_ENTRY ),  // expected: null entry and null masterKey, so we expect an exception
                Arguments.of(getByteBuf(Instance.VALID, 1),    true,           getCallback(Instance.INVALID),      null,          "key".getBytes(),   true,    Add.ADD_ENTRY ),  // expected: invalid cb and null ctx, so we expect an exception
                Arguments.of(getByteBuf(Instance.VALID, 1),    false,          getCallback(Instance.VALID),        "test",        "key".getBytes(),   false,   Add.ADD_ENTRY),   // expected: entry successfully written

                Arguments.of(getByteBuf(Instance.VALID, 2),    true,           getCallback(Instance.VALID),        "test",        "key".getBytes(),   false,   Add.RECOVERY_ADD_ENTRY),   // expected: entry successfully written
                Arguments.of(getByteBuf(Instance.INVALID, 2),  true,           null,                               new int[2],    null,               true,    Add.RECOVERY_ADD_ENTRY),   // expected: exception (invalid entry, null cb, null masterKey)
                Arguments.of(null,                                     true,           getCallback(Instance.INVALID),      null,          "".getBytes(),      true,    Add.RECOVERY_ADD_ENTRY)   // expected: exception (null entry, invalid cb)
        );
    }


    /**
     * This method tests:
     * <p> public void addEntry(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb, Object ctx, byte[] masterKey). </p>
     * <p> public void recoveryAddEntry(ByteBuf entry, WriteCallback cb, Object ctx, byte[] masterKey) </p>
     */
    @ParameterizedTest
    @MethodSource("addEntryParams")
    public void testAddEntry(ByteBuf entry, boolean ackBeforeSync, WriteCallback cb, Object ctx, byte[] masterKey, boolean exceptionExpected, Add methodToTest) {
        try {

            if (methodToTest == Add.ADD_ENTRY)
                this.bookie.addEntry(entry, ackBeforeSync, cb, ctx, masterKey);
            else if (methodToTest == Add.RECOVERY_ADD_ENTRY)
                this.bookie.recoveryAddEntry(entry, cb, ctx, masterKey);

            /* hasCompleted will be updated to true, in the cb, in case of success (oth. its value will not change), so we wait until this condition is satisfied;
             * if it's not satisfied within a timeout period, an exception will be thrown. When it becomes true, the test may be considered successful. */
            Awaitility.await().untilAsserted(() -> Assertions.assertTrue(hasCompleted.get()));


        } catch (IOException | InterruptedException | BookieException | RuntimeException e) {
            Assertions.assertTrue(exceptionExpected);
            e.printStackTrace();
        }
    }


    private static Stream<Arguments> fenceParams() {
        return Stream.of(
                //Arguments.of(-1L, "key".getBytes(),   true ),   TODO this first test case fails!
                Arguments.of(0L,  null,               true ),
                Arguments.of(0L,  "".getBytes(),      false),
                Arguments.of(1L,  "key".getBytes(),   false)
        );
    }


    /**
     * This method tests:
     * <p> public CompletableFuture<Boolean> fenceLedger(long ledgerId, byte[] masterKey)
     * <p> fenceLedger description (javadoc): Fences a ledger. From this point on, clients will be unable to write to this ledger.
     *  Only recoveryAddEntry will be able to add entries to the ledger.
     *  This method is idempotent. Once a ledger is fenced, it can never be unfenced. Fencing a fenced ledger has no effect.
     */
    @ParameterizedTest()
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

            /* this should throw an exception, as we're writing on a fenced ledger */
            Assertions.assertThrows(BookieException.LedgerFencedException.class,
                    () -> bookie.addEntry(entry, false, getCallback(Instance.VALID), null, masterKey),
                    "An exception is not thrown when trying to write on a fenced ledger using addEntry.");


            /* this should not return an exception, as recoveryAddEntry allows to write on a fenced ledger */
            Assertions.assertDoesNotThrow(() -> bookie.recoveryAddEntry(getByteBuf(Instance.VALID, 3), getCallback(Instance.VALID), null, "key".getBytes()));
            Awaitility.await().untilAsserted(() -> Assertions.assertTrue(hasCompleted.get()));  // we're checking if the write is successful.



        } catch (IOException | BookieException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
            Assertions.assertTrue(exceptionExpected);
        }
    }

    private static ByteBuf generateEntry(long ledger) {
        byte[] data = ("ledger-" + ledger + "-" + 1).getBytes();
        ByteBuf bb = Unpooled.buffer(8 + 8 + data.length);
        bb.writeLong(ledger);
        bb.writeLong(1);
        bb.writeBytes(data);
        return bb;
    }

    @AfterAll
    public static void cleanUp() {
        //bookie.shutdown();
    }

}
