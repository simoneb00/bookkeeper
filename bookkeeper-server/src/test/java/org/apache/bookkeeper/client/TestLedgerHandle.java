package org.apache.bookkeeper.client;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.apache.bookkeeper.client.utils.BookKeeperClusterTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;


@RunWith(Parameterized.class)
public class TestLedgerHandle extends BookKeeperClusterTestCase {

    private byte[] data;
    private int offset;
    private int length;
    private boolean exceptionExpected;
    private BookKeeper bookKeeper;
    private LedgerHandle ledgerHandle;


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                // data,           offset, length, exception expected
                {"data".getBytes(), 4, 0, false},            // expected: no bytes written, but no exceptions
                {"data".getBytes(), 0, 4, false},            // expected: "data" is correctly written on the ledger
                {"data".getBytes(), 4, 1, true},             // expected: exception
                {"".getBytes(), 0, 0, true},             // expected: exception
                {null, 0, 0, true},             // expected: exception
                {null, -1, 0, true}              // expected: exception
        });
    }

    public TestLedgerHandle(byte[] data, int offset, int length, boolean exceptionExpected) {

        // calling BookKeeperClusterTestCase's constructor, in order to create a ZooKeeper server, mandatory for instantiating a BookKeeper client
        super(5, 180);

        this.data = data;
        this.offset = offset;
        this.length = length;
        this.exceptionExpected = exceptionExpected;
    }


    /* TODO improve setup, in this way a new BookKeeper client is instantiated before every @Test class */
    @Before
    public void setUp() throws Exception {
        super.setUp();
        baseConf.setJournalWriteData(true);
        baseClientConf.setUseV2WireProtocol(true);
        this.bookKeeper = new BookKeeper(baseClientConf);

        // the focus of this test case is not to check if the method createLedger has bugs, we only need a ledgerHandle instance.
        this.ledgerHandle = this.bookKeeper.createLedger(4, 3, 2, BookKeeper.DigestType.CRC32, "password".getBytes());
    }


    @Test
    public void testLedgerHandle() throws org.apache.bookkeeper.client.api.BKException {
        try {
            ledgerHandle.addEntry(this.data, this.offset, this.length);
            LedgerEntry le = ledgerHandle.readLastEntry();
            Assert.assertEquals(new String(le.getEntry(), StandardCharsets.UTF_8), new String(Arrays.copyOfRange(this.data, this.offset, this.offset + this.length), StandardCharsets.UTF_8));
        } catch(Exception e) {
            Assert.assertTrue(this.exceptionExpected);
        }
    }


    @Test
    public void testGetNumBookies() {
        long numBookies = this.ledgerHandle.getNumBookies();
        Assert.assertEquals(numBookies, 4);
    }
}
