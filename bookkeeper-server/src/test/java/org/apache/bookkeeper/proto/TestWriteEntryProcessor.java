package org.apache.bookkeeper.proto;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.LedgerDescriptor;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * This class aims to test the class WriteEntryProcessor.java,
 * and its integration with the class BookieImpl.java.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestWriteEntryProcessor {

    @Mock
    private BookieProtocol.ParsedAddRequest mockedRequest;
    @Mock
    private BookieRequestHandler mockedHandler;
    @Mock
    private BookieRequestProcessor mockedProcessor;
    @Mock
    private ChannelHandlerContext ctx;
    @Mock
    private BookieImpl bookie;
    @Mock
    private LedgerDescriptor mockedDescr;       // this is used to simulate the case in which the ledger is fenced

    @Before
    public void setUp() {
        /* BookieRequestProcessor mock configuration */
        when(mockedProcessor.getBookie()).thenReturn(bookie);

        /* BookieRequestHandler mock configuration */
        when(mockedHandler.ctx()).thenReturn(ctx);

        /* ParsedAddRequest mock configuration */
        when(mockedRequest.getEntryId()).thenReturn(1L);
        when(mockedRequest.getData()).thenReturn(Unpooled.copiedBuffer("test", StandardCharsets.UTF_8));
        when(mockedRequest.getLedgerId()).thenReturn(1L);
        when(mockedRequest.getMasterKey()).thenReturn("key".getBytes());
        when(mockedRequest.getProtocolVersion()).thenReturn(BookieProtocol.CURRENT_PROTOCOL_VERSION);
    }


    /**
     *  This method tests the method:
     *  <p> <i>protected void processPacket()</i> </p>
     *  In particular, it tests that:
     *  <p> if the bookie is read-only, we can't write on it </p>
     *  <p> if the ledger is fenced, we can't write on it, unless that the ParsedAddRequest is a recovery request (i.e. request.isRecoveryAdd() = true) </p>    //TODO: find a way to simulate a fenced ledger
     *  //TODO: add other testing scenarios
     *
     */
    @Test
    public void testProcessPacket() throws IOException, InterruptedException, BookieException {

        WriteEntryProcessor processor = WriteEntryProcessor.create(mockedRequest, mockedHandler, mockedProcessor);
        Assertions.assertNotNull(processor);

        processor.processPacket();


    }
}
