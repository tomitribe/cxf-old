/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cxf.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.Reader;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.IvParameterSpec;

import org.apache.cxf.Bus;
import org.apache.cxf.BusFactory;
import org.apache.cxf.common.util.SystemPropertyAction;
import org.apache.cxf.helpers.FileUtils;
import org.apache.cxf.helpers.IOUtils;
import org.apache.cxf.helpers.LoadingByteArrayOutputStream;

public class CachedOutputStream extends OutputStream {
    private static final File DEFAULT_TEMP_DIR;
    private static int defaultThreshold;
    private static long defaultMaxSize;
    private static String defaultCipherTransformation;
    static {
        
        String s = SystemPropertyAction.getPropertyOrNull("org.apache.cxf.io.CachedOutputStream.OutputDirectory");
        if (s != null) {
            File f = new File(s);
            if (f.exists() && f.isDirectory()) {
                DEFAULT_TEMP_DIR = f;
            } else {
                DEFAULT_TEMP_DIR = null;
            }
        } else {
            DEFAULT_TEMP_DIR = null;
        }

        setDefaultThreshold(-1);
        setDefaultMaxSize(-1);
        setDefaultCipherTransformation(null);
    }

    protected boolean outputLocked;
    protected OutputStream currentStream;

    private long threshold = defaultThreshold;
    private long maxSize = defaultMaxSize;

    private long totalLength;

    private boolean inmem;

    private boolean tempFileFailed;
    private File tempFile;
    private File outputDir = DEFAULT_TEMP_DIR;
    private boolean allowDeleteOfFile = true;
    private String cipherTransformation = defaultCipherTransformation;
    private Cipher enccipher;
    private Cipher deccipher;
    

    private List<CachedOutputStreamCallback> callbacks;
    
    private List<Object> streamList = new ArrayList<Object>();

    public CachedOutputStream(PipedInputStream stream) throws IOException {
        currentStream = new PipedOutputStream(stream);
        inmem = true;
        readBusProperties();
    }

    public CachedOutputStream() {
        this(defaultThreshold);
    }

    public CachedOutputStream(long threshold) {
        this.threshold = threshold; 
        currentStream = new LoadingByteArrayOutputStream(2048);
        inmem = true;
        readBusProperties();
    }

    private void readBusProperties() {
        Bus b = BusFactory.getThreadDefaultBus(false);
        if (b != null) {
            String v = getBusProperty(b, "bus.io.CachedOutputStream.Threshold", null);
            if (v != null && threshold == defaultThreshold) {
                threshold = Integer.parseInt(v);
            }
            v = getBusProperty(b, "bus.io.CachedOutputStream.MaxSize", null);
            if (v != null) {
                maxSize = Integer.parseInt(v);
            }
            v = getBusProperty(b, "bus.io.CachedOutputStream.CipherTransformation", null);
            if (v != null) {
                cipherTransformation = v;
            }
        }
    }

    private static String getBusProperty(Bus b, String key, String dflt) {
        String v = (String)b.getProperty(key);
        return v != null ? v : dflt;
    }

    public void holdTempFile() {
        allowDeleteOfFile = false;
    }
    public void releaseTempFileHold() {
        allowDeleteOfFile = true;
    }
    
    public void registerCallback(CachedOutputStreamCallback cb) {
        if (null == callbacks) {
            callbacks = new ArrayList<CachedOutputStreamCallback>();
        }
        callbacks.add(cb);
    }
    
    public void deregisterCallback(CachedOutputStreamCallback cb) {
        if (null != callbacks) {
            callbacks.remove(cb);
        }
    }

    public List<CachedOutputStreamCallback> getCallbacks() {
        return callbacks == null ? null : Collections.unmodifiableList(callbacks);
    }

    /**
     * Perform any actions required on stream flush (freeze headers, reset
     * output stream ... etc.)
     */
    protected void doFlush() throws IOException {
        
    }

    public void flush() throws IOException {
        currentStream.flush();
        if (null != callbacks) {
            for (CachedOutputStreamCallback cb : callbacks) {
                cb.onFlush(this);
            }
        }
        doFlush();
    }

    /**
     * Perform any actions required on stream closure (handle response etc.)
     */
    protected void doClose() throws IOException {
        
    }
    
    /**
     * Perform any actions required after stream closure (close the other related stream etc.)
     */
    protected void postClose() throws IOException {
        
    }

    /**
     * Locks the output stream to prevent additional writes, but maintains
     * a pointer to it so an InputStream can be obtained
     * @throws IOException
     */
    public void lockOutputStream() throws IOException {
        if (outputLocked) {
            return;
        }
        currentStream.flush();
        outputLocked = true;
        if (null != callbacks) {
            for (CachedOutputStreamCallback cb : callbacks) {
                cb.onClose(this);
            }
        }
        doClose();
        streamList.remove(currentStream);
    }
    
    public void close() throws IOException {
        currentStream.flush();
        outputLocked = true;
        if (null != callbacks) {
            for (CachedOutputStreamCallback cb : callbacks) {
                cb.onClose(this);
            }
        }
        doClose();
        currentStream.close();
        maybeDeleteTempFile(currentStream);
        postClose();
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof CachedOutputStream) {
            return currentStream.equals(((CachedOutputStream)obj).currentStream);
        }
        return currentStream.equals(obj);
    }

    /**
     * Replace the original stream with the new one, optionally copying the content of the old one
     * into the new one.
     * When with Attachment, needs to replace the xml writer stream with the stream used by
     * AttachmentSerializer or copy the cached output stream to the "real"
     * output stream, i.e. onto the wire.
     * 
     * @param out the new output stream
     * @param copyOldContent flag indicating if the old content should be copied
     * @throws IOException
     */
    public void resetOut(OutputStream out, boolean copyOldContent) throws IOException {
        if (out == null) {
            out = new ByteArrayOutputStream();
        }

        if (currentStream instanceof CachedOutputStream) {
            CachedOutputStream ac = (CachedOutputStream) currentStream;
            InputStream in = ac.getInputStream();
            IOUtils.copyAndCloseInput(in, out);
        } else {
            if (inmem) {
                if (currentStream instanceof ByteArrayOutputStream) {
                    ByteArrayOutputStream byteOut = (ByteArrayOutputStream) currentStream;
                    if (copyOldContent && byteOut.size() > 0) {
                        byteOut.writeTo(out);
                    }
                } else if (currentStream instanceof PipedOutputStream) {
                    PipedOutputStream pipeOut = (PipedOutputStream) currentStream;
                    IOUtils.copyAndCloseInput(new PipedInputStream(pipeOut), out);
                } else {
                    throw new IOException("Unknown format of currentStream");
                }
            } else {
                // read the file
                currentStream.close();
                if (copyOldContent) {
                    InputStream fin = createInputStream(tempFile);
                    IOUtils.copyAndCloseInput(fin, out);
                }
                streamList.remove(currentStream);
                deleteTempFile();
                inmem = true;
            }
        }
        currentStream = out;
        outputLocked = false;
    }

    public static void copyStream(InputStream in, OutputStream out, int bufferSize) throws IOException {
        IOUtils.copyAndCloseInput(in, out, bufferSize);
    }

    public long size() {
        return totalLength;
    }

    public byte[] getBytes() throws IOException {
        flush();
        if (inmem) {
            if (currentStream instanceof ByteArrayOutputStream) {
                return ((ByteArrayOutputStream)currentStream).toByteArray();
            } else {
                throw new IOException("Unknown format of currentStream");
            }
        } else {
            // read the file
            InputStream fin = createInputStream(tempFile);
            return IOUtils.readBytesFromStream(fin);
        }
    }

    public void writeCacheTo(OutputStream out) throws IOException {
        flush();
        if (inmem) {
            if (currentStream instanceof ByteArrayOutputStream) {
                ((ByteArrayOutputStream)currentStream).writeTo(out);
            } else {
                throw new IOException("Unknown format of currentStream");
            }
        } else {
            // read the file
            InputStream fin = createInputStream(tempFile);
            IOUtils.copyAndCloseInput(fin, out);
        }
    }
    
    public void writeCacheTo(StringBuilder out, long limit) throws IOException {
        writeCacheTo(out, "UTF-8", limit);
    }
    
    public void writeCacheTo(StringBuilder out, String charsetName, long limit) throws IOException {
        flush();
        if (totalLength < limit
            || limit == -1) {
            writeCacheTo(out, charsetName);
            return;
        }

        long count = 0;
        if (inmem) {
            if (currentStream instanceof ByteArrayOutputStream) {
                byte bytes[] = ((ByteArrayOutputStream)currentStream).toByteArray();
                out.append(IOUtils.newStringFromBytes(bytes, charsetName, 0, (int)limit));
            } else {
                throw new IOException("Unknown format of currentStream");
            }
        } else {
            // read the file
            InputStream fin = createInputStream(tempFile);
            Reader reader = new InputStreamReader(fin, charsetName);
            char bytes[] = new char[1024];
            long x = reader.read(bytes);
            while (x != -1) {
                if ((count + x) > limit) {
                    x = limit - count;
                }
                out.append(bytes, 0, (int)x);
                count += x;

                if (count >= limit) {
                    x = -1;
                } else {
                    x = reader.read(bytes);
                }
            }
            reader.close();
            fin.close();
        }
    }
    
    public void writeCacheTo(StringBuilder out) throws IOException {
        writeCacheTo(out, "UTF-8");
    }
    
    public void writeCacheTo(StringBuilder out, String charsetName) throws IOException {
        flush();
        if (inmem) {
            if (currentStream instanceof ByteArrayOutputStream) {
                byte[] bytes = ((ByteArrayOutputStream)currentStream).toByteArray();
                out.append(IOUtils.newStringFromBytes(bytes, charsetName));
            } else {
                throw new IOException("Unknown format of currentStream");
            }
        } else {
            // read the file
            InputStream fin = createInputStream(tempFile);
            Reader reader = new InputStreamReader(fin, charsetName);
            char bytes[] = new char[1024];
            int x = reader.read(bytes);
            while (x != -1) {
                out.append(bytes, 0, x);
                x = reader.read(bytes);
            }
            reader.close();
            fin.close();
        }
    }


    /**
     * @return the underlying output stream
     */
    public OutputStream getOut() {
        return currentStream;
    }

    public int hashCode() {
        return currentStream.hashCode();
    }

    public String toString() {
        StringBuilder builder = new StringBuilder().append("[")
            .append(CachedOutputStream.class.getName())
            .append(" Content: ");
        try {
            writeCacheTo(builder);
        } catch (IOException e) {
            //ignore
        }
        return builder.append("]").toString();
    }

    protected void onWrite() throws IOException {

    }

    private  void enforceLimits() throws IOException {
        if (maxSize > 0 && totalLength > maxSize) {
            throw new CacheSizeExceededException();
        }
        if (inmem && totalLength > threshold && currentStream instanceof ByteArrayOutputStream) {
            createFileOutputStream();
        }       
    }

    public void write(byte[] b, int off, int len) throws IOException {
        if (!outputLocked) {
            onWrite();
            this.totalLength += len;
            enforceLimits();
            currentStream.write(b, off, len);
        }
    }

    public void write(byte[] b) throws IOException {
        if (!outputLocked) {
            onWrite();
            this.totalLength += b.length;
            enforceLimits();
            currentStream.write(b);
        }
    }

    public void write(int b) throws IOException {
        if (!outputLocked) {
            onWrite();
            this.totalLength++;
            enforceLimits();
            currentStream.write(b);
        }
    }

    private void createFileOutputStream() throws IOException {
        if (tempFileFailed) {
            return;
        }
        ByteArrayOutputStream bout = (ByteArrayOutputStream)currentStream;
        try {
            if (outputDir == null) {
                tempFile = FileUtils.createTempFile("cos", "tmp");
            } else {
                tempFile = FileUtils.createTempFile("cos", "tmp", outputDir, false);
            }
            
            currentStream = createOutputStream(tempFile);
            bout.writeTo(currentStream);
            inmem = false;
            streamList.add(currentStream);
        } catch (Exception ex) {
            //Could be IOException or SecurityException or other issues.
            //Don't care what, just keep it in memory.
            tempFileFailed = true;
            if (currentStream != bout) {
                currentStream.close();
            }
            deleteTempFile();
            inmem = true;
            currentStream = bout;
        }
    }

    public File getTempFile() {
        return tempFile != null && tempFile.exists() ? tempFile : null;
    }

    public InputStream getInputStream() throws IOException {
        flush();
        if (inmem) {
            if (currentStream instanceof LoadingByteArrayOutputStream) {
                return ((LoadingByteArrayOutputStream) currentStream).createInputStream();
            } else if (currentStream instanceof ByteArrayOutputStream) {
                return new ByteArrayInputStream(((ByteArrayOutputStream) currentStream).toByteArray());
            } else if (currentStream instanceof PipedOutputStream) {
                return new PipedInputStream((PipedOutputStream) currentStream);
            } else {
                return null;
            }
        } else {
            try {
                InputStream fileInputStream = new FileInputStream(tempFile) {
                    boolean closed;
                    public void close() throws IOException {
                        if (!closed) {
                            super.close();
                            maybeDeleteTempFile(this);
                        }
                        closed = true;
                    }
                };
                streamList.add(fileInputStream);
                if (cipherTransformation != null) {
                    fileInputStream = new CipherInputStream(fileInputStream, deccipher) {
                        boolean closed;
                        public void close() throws IOException {
                            if (!closed) {
                                super.close();
                                closed = true;
                            }
                        }
                    };
                }
                
                return fileInputStream;
            } catch (FileNotFoundException e) {
                throw new IOException("Cached file was deleted, " + e.toString());
            }
        }
    }
    
    private synchronized void deleteTempFile() {
        if (tempFile != null) {
            File file = tempFile;
            tempFile = null;
            FileUtils.delete(file);
        }
    }
    private void maybeDeleteTempFile(Object stream) {
        streamList.remove(stream);
        if (!inmem && tempFile != null && streamList.isEmpty() && allowDeleteOfFile) {
            if (currentStream != null) {
                try {
                    currentStream.close();
                    postClose();
                } catch (Exception e) {
                    //ignore
                }
            }
            deleteTempFile();
            currentStream = new LoadingByteArrayOutputStream(1024);
            inmem = true;
        }
    }

    public void setOutputDir(File outputDir) throws IOException {
        this.outputDir = outputDir;
    }
    public void setThreshold(long threshold) {
        this.threshold = threshold;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    public void setCipherTransformation(String cipherTransformation) {
        this.cipherTransformation = cipherTransformation;
    }
    
    public static void setDefaultMaxSize(long l) {
        if (l == -1) {
            String s = System.getProperty("org.apache.cxf.io.CachedOutputStream.MaxSize",
                    "-1");
            l = Long.parseLong(s);
        }
        defaultMaxSize = l;
    }
    public static void setDefaultThreshold(int i) {
        if (i == -1) {
            String s = SystemPropertyAction.getProperty("org.apache.cxf.io.CachedOutputStream.Threshold",
                "-1");
            i = Integer.parseInt(s);
            if (i <= 0) {
                i = 64 * 1024;
            }
        }
        defaultThreshold = i;
        
    }
    public static void setDefaultCipherTransformation(String n) {
        if (n == null) {
            n = SystemPropertyAction.getPropertyOrNull("org.apache.cxf.io.CachedOutputStream.CipherTransformation");
        }
        defaultCipherTransformation = n;
    }

    private synchronized void initCiphers() throws GeneralSecurityException {
        if (enccipher == null) {
            int d = cipherTransformation.indexOf('/');
            String a;
            if (d > 0) {
                a = cipherTransformation.substring(0, d);
            } else {
                a = cipherTransformation;
            }
            try {
                Key key = KeyGenerator.getInstance(a).generateKey();
                enccipher = Cipher.getInstance(cipherTransformation);
                deccipher = Cipher.getInstance(cipherTransformation);
                enccipher.init(Cipher.ENCRYPT_MODE, key);
                final byte[] ivp = enccipher.getIV();
                deccipher.init(Cipher.DECRYPT_MODE, key, ivp == null ? null : new IvParameterSpec(ivp));
            } catch (GeneralSecurityException e) {
                enccipher = null;
                deccipher = null;
                throw e;
            }
        }
    }

    private OutputStream createOutputStream(File file) throws IOException {
        OutputStream out = new FileOutputStream(file);
        if (cipherTransformation != null) {
            try {
                initCiphers();
            } catch (GeneralSecurityException e) {
                throw new IOException(e.getMessage() + e.toString());
            }
            out = new CipherOutputStream(out, enccipher) {
                boolean closed;
                public void close() throws IOException {
                    if (!closed) {
                        super.close();
                        closed = true;
                    }
                }
            };
        }
        return out;
    }

    private InputStream createInputStream(File file) throws IOException {
        InputStream in = new FileInputStream(file);
        if (cipherTransformation != null) {
            in = new CipherInputStream(in, deccipher) {
                boolean closed;
                public void close() throws IOException {
                    if (!closed) {
                        super.close();
                        closed = true;
                    }
                }
            };
        }
        return in;
    }

}
