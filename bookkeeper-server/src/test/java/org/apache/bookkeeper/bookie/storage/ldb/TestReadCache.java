package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.*;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class TestReadCache {

    private ReadCache readCache;

    public TestReadCache() {
        this.setUp();
    }

    private void setUp() {
        ByteBufAllocator byteBufAllocator = PooledByteBufAllocator.DEFAULT;
        long maxCacheSize = 100 * 1024 * 1024; // Maximum cache size of 100 MB
        int maxSegmentSize = 16 * 1024; // Maximum segment size of 16 KB

        this.readCache = new ReadCache(byteBufAllocator, maxCacheSize, maxSegmentSize);

    }

    @Test
    public void testPutGet() {
        ByteBuf buf = Unpooled.copiedBuffer("test", StandardCharsets.UTF_8);
        ByteBuf buf1 = Unpooled.copiedBuffer("hello", StandardCharsets.UTF_8);
        this.readCache.put(0, 0, buf);
        this.readCache.put(1, 1, buf1);

        ByteBuf byteBuf = this.readCache.get(0, 0);
        ByteBuf byteBuf1 = this.readCache.get(1, 1);

        Assert.assertEquals("test", byteBuf.toString(StandardCharsets.UTF_8));
        Assert.assertEquals("hello", byteBuf1.toString(StandardCharsets.UTF_8));
        Assert.assertTrue(this.readCache.hasEntry(0, 0));
        Assert.assertTrue(this.readCache.hasEntry(1, 1));
        Assert.assertEquals(this.readCache.count(), 2);
        Assert.assertEquals(buf.capacity() + buf1.capacity(), this.readCache.size());
    }
}
