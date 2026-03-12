/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.trino.fileio;

import org.apache.paimon.fs.SeekableInputStream;

import java.io.IOException;

/**
 * A buffered {@link SeekableInputStream} that reduces per-call I/O to the underlying stream.
 *
 * <p>Avro's {@code DataFileReader.sync()} scans for the sync marker one byte at a time via {@code
 * BinaryDecoder.InputStreamByteSource.read()}. On object stores (OSS/S3/WASB), each unbuffered
 * {@code read()} becomes an individual network request, causing extreme latency. For example, a
 * 1768-byte manifest file can require ~886 individual network calls taking 6+ seconds.
 *
 * <p>This wrapper buffers reads in bulk (default 64 KB). A {@code seek()} within the currently
 * buffered range only adjusts the buffer pointer without touching the underlying stream.
 */
public class BufferedSeekableInputStream extends SeekableInputStream {

    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024; // 64KB

    private final SeekableInputStream in;
    private final byte[] buffer;

    /** File offset corresponding to {@code buffer[0]}. */
    private long bufferStart;

    /** Index within {@code buffer} of the current logical read position. */
    private int bufferPos;

    /** Number of valid bytes in {@code buffer} starting at index 0. */
    private int bufferLen;

    public BufferedSeekableInputStream(SeekableInputStream in) {
        this(in, DEFAULT_BUFFER_SIZE);
    }

    public BufferedSeekableInputStream(SeekableInputStream in, int bufferSize) {
        this.in = in;
        this.buffer = new byte[bufferSize];
        this.bufferStart = 0;
        this.bufferPos = 0;
        this.bufferLen = 0;
    }

    @Override
    public long getPos() throws IOException {
        return bufferStart + bufferPos;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos >= bufferStart && pos < bufferStart + bufferLen) {
            // Within buffered range: just move the pointer, no network I/O needed.
            bufferPos = (int) (pos - bufferStart);
        } else {
            // Outside buffered range: invalidate buffer and seek underlying stream.
            in.seek(pos);
            bufferStart = pos;
            bufferPos = 0;
            bufferLen = 0;
        }
    }

    @Override
    public int read() throws IOException {
        if (bufferPos >= bufferLen) {
            fillBuffer();
            if (bufferLen == 0) {
                return -1;
            }
        }
        return buffer[bufferPos++] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        // First, drain any bytes already in the buffer.
        if (bufferPos < bufferLen) {
            int available = bufferLen - bufferPos;
            int toCopy = Math.min(len, available);
            System.arraycopy(buffer, bufferPos, b, off, toCopy);
            bufferPos += toCopy;
            return toCopy;
        }
        // Buffer is empty. For large requests (>= buffer capacity), bypass the buffer entirely to
        // avoid the overhead of multiple fill-and-copy cycles through a 64KB intermediate buffer.
        // This keeps bulk reads (ORC/Parquet stripe/row-group reads) at zero extra cost.
        if (len >= buffer.length) {
            bufferStart = bufferStart + bufferLen;
            bufferLen = 0;
            bufferPos = 0;
            int n = in.read(b, off, len);
            if (n > 0) {
                bufferStart += n;
            }
            return n;
        }
        // Small request: fill the buffer and serve from it.
        fillBuffer();
        if (bufferLen == 0) {
            return -1;
        }
        int toCopy = Math.min(len, bufferLen);
        System.arraycopy(buffer, 0, b, off, toCopy);
        bufferPos = toCopy;
        return toCopy;
    }

    /**
     * Refills the internal buffer from the underlying stream.
     *
     * <p>The underlying stream is always positioned at {@code bufferStart + bufferLen} after any
     * operation (seeks within the buffer do not move the underlying stream pointer). Therefore we
     * simply advance {@code bufferStart} and issue one bulk read.
     */
    private void fillBuffer() throws IOException {
        bufferStart = bufferStart + bufferLen;
        bufferPos = 0;
        bufferLen = 0;
        int n = in.read(buffer, 0, buffer.length);
        if (n > 0) {
            bufferLen = n;
        }
    }

    @Override
    public void close() throws IOException {
        in.close();
    }
}
