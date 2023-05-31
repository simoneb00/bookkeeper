package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.LedgerEntries;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.apache.bookkeeper.client.utils.BookKeeperClusterTestCase;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;


/*
 *  In this class the following methods are tested:
 *  - public long addEntry(final long entryId, byte[] data)
 *  - public long addEntry(final long entryId, byte[] data, int offset, int length)
 *  - public void asyncAddEntry(long entryId, byte[] data, AddCallback cb, Object ctx)
 *  - public void asyncAddEntry(final long entryId, final byte[] data, final int offset, final int length, final AddCallback cb, final Object ctx)
 *  - public void asyncAddEntry(final long entryId, final byte[] data, final int offset, final int length, final AddCallbackWithLatency cb, final Object ctx)
 */
public class TestLedgerHandleAdv extends BookKeeperClusterTestCase {

    // BookKeeper client
    private BookKeeper bookKeeper;
    private LedgerHandleAdv ledgerHandleAdv;

    public TestLedgerHandleAdv() {
        super(5, 180);
    }


    @BeforeEach
    public void setUp() throws Exception{
        super.setUp();
        baseConf.setJournalWriteData(true);
        baseClientConf.setUseV2WireProtocol(true);
        this.bookKeeper = new BookKeeper(baseClientConf);

        this.ledgerHandleAdv = (LedgerHandleAdv) this.bookKeeper.createLedgerAdv(4, 3, 2, BookKeeper.DigestType.CRC32, "password".getBytes());
    }

    private static Stream<Arguments> provideParameters1() {
        return Stream.of(
                Arguments.of(-1, "data".getBytes(), true),
                Arguments.of(0, "data".getBytes(), false),
                Arguments.of(0, "".getBytes(), false),
                Arguments.of(0, null, true)
        );
    }

    @ParameterizedTest
    @MethodSource("provideParameters1")
    public void testAddEntry(long id, byte[] data, boolean exceptionExpected) {
        try {
            long resultId = this.ledgerHandleAdv.addEntry(id, data);
            Assert.assertEquals(id, resultId);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(exceptionExpected);
        }
    }

    @Disabled
    @ParameterizedTest
    @MethodSource("provideParameters1")
    public void testWriteAsync(long id, byte[] data, boolean exceptionExpected) {
        try {
            long result = this.ledgerHandleAdv.writeAsync(id, data).get();
            Assert.assertEquals(id, result);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(exceptionExpected);
        }
    }


    private static Stream<Arguments> provideParameters2() {
        return Stream.of(
                                      //id,   data,              offset, length,  cb,      ctx,    exception expected
                Arguments.of(-1,   "data".getBytes(),  0,      4,      false,   false,  true),      // expected: exception
                Arguments.of(0,    "data".getBytes(), -1,      6,      false,   false,  true),      // expected: exception
                Arguments.of(0,    "data".getBytes(),  0,      4,      true,    true,   false),     // expected: added an entry with id = 0 and data = "data"
                Arguments.of(0,    "".getBytes(),      1,     -1,      false,   false,  true),      // expected: exception
                Arguments.of(0,    null,               0,      0,      false,   false,  true),      // expected: exception
                Arguments.of(0,    "data".getBytes(),  4,      0,      false,   false,  false)      // expected: added entry with id = 0 and data = ""
        );
    }


    /*
     *  method under test: public void asyncAddEntry(final long entryId, final byte[] data, final int offset, final int length, final AddCallback cb, final Object ctx)
     */

    @Disabled
    @ParameterizedTest
    @MethodSource("provideParameters2")
    public void testAsyncAddEntry(long id, byte[] data, int offset, int length, boolean cb, boolean ctx, boolean exceptionExpected) {

        Object ctxObject = null;
        AsyncCallback.AddCallback addCallback = null;

        try {
            if (ctx)
                ctxObject = "test";
            if (cb) {
                addCallback = (rc, lh, entryId, ctx1) -> {
                    if (ctx1 != null)
                        Assertions.assertTrue(true);
                };
            }

            this.ledgerHandleAdv.asyncAddEntry(id, data, offset, length, addCallback, ctxObject);

            CompletableFuture<LedgerEntries> cf = this.ledgerHandleAdv.readAsync(id, id);
            LedgerEntries ledgerEntries = cf.get();
            org.apache.bookkeeper.client.api.LedgerEntry ledgerEntry = ledgerEntries.getEntry(id);

            Assertions.assertSame(ledgerEntry.getEntryBytes(), data);


        } catch (Exception e) {
            e.printStackTrace();
            Assertions.assertTrue(exceptionExpected);
        }

    }

}
