package org.apache.bookkeeper.proto;

import io.netty.channel.ChannelHandlerContext;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.junit.jupiter.api.Assertions;

import static org.apache.bookkeeper.proto.BookieProtocol.ADDENTRY;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doThrow;

public class ProtoUtils {

    public static final int VALID = 1;
    public static final int INVALID = 2;
    public static final int NULL = 3;

    public static BookieProtocol.ParsedAddRequest getMockedRequest(int type) {

        BookieProtocol.ParsedAddRequest mockedRequest = mock(BookieProtocol.ParsedAddRequest.class);

        switch (type) {
            case VALID:
                when(mockedRequest.getEntryId()).thenReturn(1L);
                when(mockedRequest.getLedgerId()).thenReturn(1L);
                when(mockedRequest.getOpCode()).thenReturn(ADDENTRY);

                return mockedRequest;
            case INVALID:
                doThrow(new RuntimeException("Invalid ParsedAddRequest")).when(mockedRequest).getData();
                return mockedRequest;
            case NULL:
                return null;
            default:
                Assertions.fail("getMockedRequest: unexpected instance type.");
                return null;
        }
    }

    public static BookieRequestHandler getMockedHandler(int type) {

        BookieRequestHandler mockedHandler = mock(BookieRequestHandler.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

        switch (type) {
            case VALID:
                when(mockedHandler.ctx()).thenReturn(ctx);
                return mockedHandler;
            case INVALID:
                doThrow(new RuntimeException("Invalid BookieRequestHandler")).when(mockedHandler).ctx();
                return mockedHandler;
            case NULL:
                return null;
            default:
                Assertions.fail("getMockedHandler: unexpected instance type.");
                return null;
        }
    }

    public static BookieRequestProcessor getMockedProcessor(int type) {

        BookieRequestProcessor mockedProcessor = mock(BookieRequestProcessor.class);
        BookieImpl bookie = mock(BookieImpl.class);

        switch (type) {
            case VALID:
                when(mockedProcessor.getBookie()).thenReturn(bookie);
                return mockedProcessor;
            case INVALID:
                doThrow(new RuntimeException("Invalid BookieRequestProcessor")).when(mockedProcessor).getBookie();
                return mockedProcessor;
            case NULL:
                return null;
            default:
                Assertions.fail("getMockedProcessor: unexpected instance type.");
                return null;
        }
    }
}
