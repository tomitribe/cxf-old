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

package org.apache.cxf.transport.http.async;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import org.apache.cxf.Bus;
import org.apache.cxf.helpers.HttpHeaderHelper;
import org.apache.cxf.helpers.LoadingByteArrayOutputStream;
import org.apache.cxf.interceptor.Fault;
import org.apache.cxf.io.CachedOutputStream;
import org.apache.cxf.message.Exchange;
import org.apache.cxf.message.Message;
import org.apache.cxf.message.MessageImpl;
import org.apache.cxf.message.MessageUtils;
import org.apache.cxf.service.model.EndpointInfo;
import org.apache.cxf.transport.http.Cookie;
import org.apache.cxf.transport.http.DigestAuthSupplier;
import org.apache.cxf.transport.http.HTTPConduit;
import org.apache.cxf.transport.https.CertConstraints;
import org.apache.cxf.transport.https.CertConstraintsInterceptor;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.apache.cxf.version.Version;
import org.apache.cxf.ws.addressing.EndpointReferenceType;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.ContentEncoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.entity.ConsumingNHttpEntity;
import org.apache.http.nio.entity.ProducingNHttpEntity;

/**
 * 
 */
public class AsyncHTTPConduit extends HTTPConduit {
    
    public AsyncHTTPConduit(Bus b, EndpointInfo ei) throws IOException {
        super(b, ei);
    }
    public AsyncHTTPConduit(Bus b, EndpointInfo ei, 
                            EndpointReferenceType t) throws IOException {
        super(b, ei, t);
    }

    public void prepare(Message message) throws IOException {
        message.put(AsyncHTTPConduit.class, this);
        Map<String, List<String>> headers = getSetProtocolHeaders(message);
        
        URL currentURL = setupURL(message);
        if (MessageUtils.getContextualBoolean(message, "force.http.url.connection", false)
            || "https".equalsIgnoreCase(currentURL.getProtocol())) {
            //delegate to the parent for any https things for now
            super.prepare(message);
            return;
        }
        
        HTTPClientPolicy csPolicy = getClient(message);
        message.put(HTTPClientPolicy.class.getName() + ".complete", csPolicy);
        
        // If the HTTP_REQUEST_METHOD is not set, the default is "POST".
        String httpRequestMethod = 
            (String)message.get(Message.HTTP_REQUEST_METHOD);        

        boolean isChunking = false;
        int chunkThreshold = 0;
        boolean needToCacheRequest = false;
        // We must cache the request if we have basic auth supplier
        // without preemptive basic auth.
        if (authSupplier != null) {
            String auth = authSupplier.getPreemptiveAuthorization(
                    this, currentURL, message);
            if (auth == null || authSupplier.requiresRequestCaching()) {
                needToCacheRequest = true;
                isChunking = false;
                LOG.log(Level.FINE,
                        "Auth Supplier, but no Premeptive User Pass or Digest auth (nonce may be stale)"
                        + " We must cache request.");
            }
            message.put("AUTH_VALUE", auth);
        }
        if (csPolicy.isAutoRedirect()) {
            needToCacheRequest = true;
            LOG.log(Level.FINE, "AutoRedirect is turned on.");
        }
        if (csPolicy.getMaxRetransmits() > 0) {
            needToCacheRequest = true;
            LOG.log(Level.FINE, "MaxRetransmits is set > 0.");
        }
        // DELETE does not work and empty PUTs cause misleading exceptions
        // if chunking is enabled
        // TODO : ensure chunking can be enabled for non-empty PUTs - if requested
        if ((httpRequestMethod == null || "POST".equals(httpRequestMethod))
            && csPolicy.isAllowChunking()) {
            //TODO: The chunking mode be configured or at least some
            // documented client constant.
            //use -1 and allow the URL connection to pick a default value
            isChunking = true;
            chunkThreshold = csPolicy.getChunkingThreshold();
            if (chunkThreshold <= 0) {
                chunkThreshold = 64 * 1024;
            }
        }

        //Do we need to maintain a session?
        maintainSession = Boolean.TRUE.equals((Boolean)message.get(Message.MAINTAIN_SESSION));
        
        //If we have any cookies and we are maintaining sessions, then use them        
        if (maintainSession && sessionCookies.size() > 0) {
            List<String> cookies = null;
            for (String s : headers.keySet()) {
                if (HttpHeaderHelper.COOKIE.equalsIgnoreCase(s)) {
                    cookies = headers.remove(s);
                    break;
                }
            }
            if (cookies == null) {
                cookies = new ArrayList<String>();
            } else {
                cookies = new ArrayList<String>(cookies);
            }
            headers.put(HttpHeaderHelper.COOKIE, cookies);
            for (Cookie c : sessionCookies.values()) {
                cookies.add(c.requestCookieHeader());
            }
        }

        
        if (certConstraints != null) {
            message.put(CertConstraints.class.getName(), certConstraints);
            message.getInterceptorChain().add(CertConstraintsInterceptor.INSTANCE);
        }

        // Set the headers on the message according to configured 
        // client side policy.
        setHeadersByPolicy(message, currentURL, headers);
        
        WrappedOutputStream wout = new WrappedOutputStream(currentURL,
                                                           message,
                                                           isChunking,
                                                           chunkThreshold,
                                                           needToCacheRequest);
        message.setContent(OutputStream.class, wout);
        message.put(WrappedOutputStream.class, wout);
    }
    
    
    protected class WrappedOutputStream extends OutputStream 
        implements ProducingNHttpEntity, ConsumingNHttpEntity {
        
        protected Message outMessage;
        protected URL address;
        protected String contentType;
        protected LoadingByteArrayOutputStream outStream; 
        protected CachedOutputStream requestStream;
        protected boolean written;
        protected boolean connected;
        protected boolean closed;
        protected HttpResponse response;
        protected InputStream inStream;
        private int chunkThreshold;
        private boolean isChunking;
        private int contentLength = -1;
        private int responseCode;
        private String responseMessage;
        private int nretransmits;

        protected WrappedOutputStream(
                URL address,
                Message m, 
                boolean isChunking, 
                int chunkThreshold, 
                boolean needToCacheRequest
        ) {
            this.outMessage = m;
            this.address = address;
            this.chunkThreshold = chunkThreshold;
            this.isChunking = isChunking;
            if (needToCacheRequest || !isChunking) {
                requestStream = new CachedOutputStream();
            }
            if (isChunking) {
                outStream = new LoadingByteArrayOutputStream(64 * 1024);
            }
        }
        public void setResponse(HttpResponse r) {
            response = r;
        }
        
        public void doConnection() throws IOException {
            connected = true;
            if (closed) {
                contentLength = outStream == null ? -1 : outStream.size();
                isChunking = false;
            }
            final String httpRequestMethod = 
                (String)outMessage.get(Message.HTTP_REQUEST_METHOD);        

            HttpUriRequest request = null;
            URI uri2;
            try {
                uri2 = address.toURI();
                uri2 = new URI(null, null, uri2.getPath(), uri2.getQuery(), uri2.getFragment());
            } catch (URISyntaxException e) {
                throw new IOException(e.getMessage());
            }
            
            final URI uri = uri2;
            if (httpRequestMethod == null || "POST".equals(httpRequestMethod)) {
                HttpPost post = new HttpPost(uri);
                request = post;
                post.setEntity(this);
            } else if ("PUT".equals(httpRequestMethod)) {
                HttpPut post = new HttpPut(uri);
                request = post;
                post.setEntity(this);
            } else if ("GET".equals(httpRequestMethod)) {
                request = new HttpGet(uri);
            } else if ("DELETE".equals(httpRequestMethod)) {
                request = new HttpDelete(uri);
            } else if ("HEAD".equals(httpRequestMethod)) {
                request = new HttpHead(uri);
            } else if ("TRACE".equals(httpRequestMethod)) {
                request = new HttpTrace(uri);
            } else if ("OPTIONS".equals(httpRequestMethod)) {
                request = new HttpOptions(uri);
            } else {
                HttpRequestBase r = new HttpRequestBase() {
                    public String getMethod() {
                        return httpRequestMethod;
                    }
                };
                r.setURI(uri);
                request = r;
            }
            setURLRequestHeaders(request);
            
            HttpClientController.getHttpClientController(outMessage)
                .execute(AsyncHTTPConduit.this,
                         address,
                         request,
                         outMessage);
        }    
        public boolean handleRetransmits(HttpResponse r) {
            return false;
        }
        public synchronized boolean writeTo(ContentEncoder out) throws IOException {
            if (isChunking || requestStream == null) {
                if (outStream.size() > 0) {
                    ByteBuffer buf = ByteBuffer.wrap(outStream.getRawBytes(), 
                                                     0, outStream.size());
                    int size = outStream.size();
                    int i = out.write(buf);
                    while (i < size) {
                        int i2 =  out.write(buf);
                        i += i2;
                    }
                    
                    outStream.reset();
                    notifyAll();
                }
            } else if (requestStream != null) {
                InputStream in = requestStream.getInputStream();
                byte bytes[] = new byte[8192];
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                try {
                    int len = in.read(bytes);
                    while (len != -1) {
                        buffer.limit(len);
                        int i = out.write(buffer);
                        while (i < len) {
                            int i2 =  out.write(buffer);
                            i += i2;
                        }
                        buffer.clear();
                        len = in.read(bytes);
                    }
                } finally {
                    in.close();
                }
            }
            return closed;
        }

        private void setURLRequestHeaders(HttpUriRequest request) throws IOException {
            String ct  = (String)outMessage.get(Message.CONTENT_TYPE);
            String enc = (String)outMessage.get(Message.ENCODING);

            if (null != ct) {
                if (enc != null 
                    && ct.indexOf("charset=") == -1
                    && !ct.toLowerCase().contains("multipart/related")) {
                    ct = ct + "; charset=" + enc;
                }
            } else if (enc != null) {
                ct = "text/xml; charset=" + enc;
            } else {
                ct = "text/xml";
            }
            request.setHeader(HttpHeaderHelper.CONTENT_TYPE, ct);
            contentType = ct; 
            
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Sending "
                    + request.getMethod() 
                    + " Message with Headers to " 
                               + request.getURI()
                    + " Conduit :"
                    + getConduitName()
                    + "\nContent-Type: " + ct + "\n");
                logProtocolHeaders(Level.FINE, outMessage);
            }
            Map<String, List<String>> headers = getSetProtocolHeaders(outMessage);
            for (String header : headers.keySet()) {
                List<String> headerList = headers.get(header);
                if (HttpHeaderHelper.CONTENT_TYPE.equalsIgnoreCase(header)) {
                    continue;
                }
                if (HttpHeaderHelper.COOKIE.equalsIgnoreCase(header)) {
                    for (String s : headerList) {
                        request.addHeader(HttpHeaderHelper.COOKIE, s);
                    }
                } else {
                    StringBuilder b = new StringBuilder();
                    for (int i = 0; i < headerList.size(); i++) {
                        b.append(headerList.get(i));
                        if (i + 1 < headerList.size()) {
                            b.append(',');
                        }
                    }
                    request.addHeader(header, b.toString());
                }
            }
            if (request.getFirstHeader("User-Agent") == null) {
                request.addHeader("User-Agent", Version.getCompleteVersionString());
            }
        }

        private InputStream getNonEmptyContent(HttpResponse resp) {
            InputStream in = null;
            try {
                PushbackInputStream pin = 
                    new PushbackInputStream(resp.getEntity().getContent());
                int c = pin.read();
                if (c != -1) {
                    pin.unread((byte)c);
                    in = pin;
                }
            } catch (IOException ioe) {
                //ignore
            }    
            return in;
        }
        protected InputStream getPartialResponse(HttpResponse r,
                                                 Map<String, List<String>> headers)
            throws IOException {
            
            InputStream in = null;
            if (responseCode == HttpURLConnection.HTTP_ACCEPTED
                || responseCode == HttpURLConnection.HTTP_OK) {
                int cl = -1;
                List<String> cls = headers.get("content-length");
                if (cls != null) {
                    cl = Integer.parseInt(cls.get(0));
                }
                if (cl > 0) {
                    in = r.getEntity().getContent();
                } else if (hasChunkedResponse(headers) 
                           || hasEofTerminatedResponse(headers)) {
                    // ensure chunked or EOF-terminated response is non-empty
                    in = getNonEmptyContent(r);        
                }
            }
            return in;
        }
        private Map<String, List<String>> mapResponseHeaders() {
            Map<String, List<String>> headers = 
                new HashMap<String, List<String>>();
            for (Header header : response.getAllHeaders()) {
                String val = header.getValue();
                List<String> v = headers.get(HttpHeaderHelper.getHeaderKey(header.getName()));
                if (v == null) {
                    v = new ArrayList<String>(2);
                    headers.put(HttpHeaderHelper.getHeaderKey(header.getName()), v);
                }
                v.add(val);
            }
            return headers;
        }
        
        protected void handleResponseInternal() throws IOException {
            // Process retransmits until we fall out.
            responseCode = response.getStatusLine().getStatusCode();
            responseMessage = response.getStatusLine().getReasonPhrase();

            try {
                if (handleRetransmits()) {
                    return;
                }
            } catch (IOException ioex) {
                //need to propogate the exception
                Exception ex = new Fault(ioex);
                outMessage.getExchange().put(Exception.class, ex);
                Message inMessage = new MessageImpl();
                inMessage.setExchange(outMessage.getExchange());
                inMessage.setContent(Exception.class, ex);
                incomingObserver.onMessage(inMessage);
                response.getEntity().getContent().close();
                return;
            }
            if (requestStream != null) {
                //don't need to retransmit, make sure it gets cleaned up
                requestStream.close();
            }
            
            Exchange exchange = outMessage.getExchange();
            synchronized (this) {
                notifyAll();
            }
            if (outMessage != null && exchange != null) {
                exchange.put(Message.RESPONSE_CODE, responseCode);
            }
            
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Response Code: " 
                        + responseCode
                        + " Conduit: " + getConduitName());
                Header[] headerFields = response.getAllHeaders();
                if (null != headerFields) {
                    StringBuilder buf = new StringBuilder();
                    buf.append("Header fields: ");
                    buf.append(System.getProperty("line.separator"));
                    for (Header h : headerFields) {
                        buf.append("    ");
                        buf.append(h.getName());
                        buf.append(": ");
                        buf.append(h.getValue());
                        buf.append(System.getProperty("line.separator"));
                    }
                    LOG.fine(buf.toString());
                }
            }
            Message inMessage = new MessageImpl();
            inMessage.setExchange(exchange);

            if (responseCode == HttpURLConnection.HTTP_NOT_FOUND
                && !MessageUtils.isTrue(outMessage.getContextualProperty(
                    "org.apache.cxf.http.no_io_exceptions"))) {
                
                Exception ex = new Fault(new IOException("HTTP response '" + responseCode + ": " 
                                + response.getStatusLine().getReasonPhrase() + "'"));
                exchange.put(Exception.class, ex);
                inMessage.setContent(Exception.class, ex);
                incomingObserver.onMessage(inMessage);
                response.getEntity().getContent().close();
                return;
            }

            Map<String, List<String>> headers = mapResponseHeaders();
            InputStream in = null;
            if (isOneway(exchange)) {
                in = getPartialResponse(response, headers);
                if (in == null) {
                    // oneway operation or decoupled MEP without 
                    // partial response
                    response.getEntity().getContent().close();
                    return;
                }
            } else {
                //not going to be resending or anything, clear out the stuff in the out message
                //to free memory
                /*
                outMessage.removeContent(OutputStream.class);
                if (cachingForRetransmission && cachedStream != null) {
                    cachedStream.close();
                }
                cachedStream = null;
                */
            }
            

            
            inMessage.put(Message.PROTOCOL_HEADERS, headers);
            inMessage.put(Message.RESPONSE_CODE, responseCode);
            List<String> ctList = headers.get("content-type");
            if (ctList !=  null) {
                String ct = null;
                for (String s : ctList) {
                    if (ct == null) {
                        ct = s;
                    } else {
                        ct += "; " + s;
                    }
                }
                inMessage.put(Message.CONTENT_TYPE, ct);
                String charset = HttpHeaderHelper.findCharset(ct);
                String normalizedEncoding = HttpHeaderHelper.mapCharset(charset);
                if (normalizedEncoding == null) {
                    String m = new org.apache.cxf.common.i18n.Message("INVALID_ENCODING_MSG",
                                                                       LOG, charset).toString();
                    LOG.log(Level.WARNING, m);
                    throw new IOException(m);   
                } 
                inMessage.put(Message.ENCODING, normalizedEncoding);            
            }
                        
            if (maintainSession) {
                List<String> cookies = headers.get("Set-Cookie");
                Cookie.handleSetCookie(sessionCookies, cookies);
            }
            if (responseCode != HttpURLConnection.HTTP_NOT_FOUND
                && in == null) {
                in = response.getEntity().getContent();
            }
            // if (in == null) : it's perfectly ok for non-soap http services
            // have no response body : those interceptors which do need it will check anyway        
            inMessage.setContent(InputStream.class, in);
            incomingObserver.onMessage(inMessage);
        }

        private boolean handleRetransmits() throws IOException {
            if (requestStream != null) {
                HTTPClientPolicy policy = getClient(outMessage);
                // Default MaxRetransmits is -1 which means unlimited.
                int maxRetransmits = (policy == null)
                                     ? -1
                                     : policy.getMaxRetransmits();
                
                // MaxRetransmits of zero means zero.
                if (maxRetransmits == 0) {
                    return false;
                }
                if (maxRetransmits != -1 && nretransmits >= maxRetransmits) {
                    return false;
                }
                if (processRetransmit(outMessage)) {
                    ++nretransmits;
                    return true;
                }
            }
            return false;
        }
        private boolean processRetransmit(Message message) throws IOException {

            if ((message != null) && (message.getExchange() != null)) {
                message.getExchange().put(Message.RESPONSE_CODE, responseCode);
            }
            
            // Process Redirects first.
            switch(responseCode) {
            case HttpURLConnection.HTTP_MOVED_PERM:
            case HttpURLConnection.HTTP_MOVED_TEMP:
                return redirectRetransmit();
            case HttpURLConnection.HTTP_UNAUTHORIZED:
                return authorizationRetransmit();
            default:
                break;
            }
            return false;
        }
        private boolean authorizationRetransmit() throws IOException {
            // If we don't have a dynamic supply of user pass, then
            // we don't retransmit. We just die with a Http 401 response.
            if (authSupplier == null) {
                String auth = response.getFirstHeader("WWW-Authenticate").getValue();
                if (auth.startsWith("Digest ")) {
                    authSupplier = new DigestAuthSupplier();
                } else {
                    return false;
                }
            }
            
            URL currentURL = address;
            
            Header heads[] = response.getHeaders("WWW-Authenticate");
            List<String> auth = new ArrayList<String>();
            if (heads != null) {
                for (Header h : heads) {
                    auth.add(h.getValue());
                }
            }
            String realm = extractAuthorizationRealm(auth);
            
            Set<String> authURLs = getSetAuthoriationURLs(outMessage);
            
            // If we have been here (URL & Realm) before for this particular message
            // retransmit, it means we have already supplied information
            // which must have been wrong, or we wouldn't be here again.
            // Otherwise, the server may be 401 looping us around the realms.
            if (authURLs.contains(currentURL.toString() + realm)) {
                if (LOG.isLoggable(Level.INFO)) {
                    LOG.log(Level.INFO, "Authorization loop detected on Conduit \""
                        + getConduitName()
                        + "\" on URL \""
                        + "\" with realm \""
                        + realm
                        + "\"");
                }
                        
                throw new IOException("Authorization loop detected on Conduit \"" 
                                      + getConduitName() 
                                      + "\" on URL \""
                                      + "\" with realm \""
                                      + realm
                                      + "\"");
            }
            
            String up = 
                authSupplier.getAuthorizationForRealm(
                    AsyncHTTPConduit.this, currentURL, outMessage,
                    realm, response.getFirstHeader("WWW-Authenticate").getValue());
            
            // No user pass combination. We give up.
            if (up == null) {
                return false;
            }
            
            // Register that we have been here before we go.
            authURLs.add(currentURL.toString() + realm);
            
            Map<String, List<String>> headers = getSetProtocolHeaders(outMessage);
            headers.put("Authorization",
                        createMutableList(up));
            inStream = null;
            doConnection();
            return true;
        }
        private boolean redirectRetransmit() throws IOException {
            // If we are not redirecting by policy, then we don't.
            if (!getClient(outMessage).isAutoRedirect()) {
                return false;
            }
            // We keep track of the redirections for redirect loop protection.
            Set<String> visitedURLs = getSetVisitedURLs(outMessage);
            visitedURLs.add(address.toString());
            
            String newURL = response.getFirstHeader("Location").getValue();
            if (newURL != null) {
                if (visitedURLs.contains(newURL)) {
                    throw new IOException("Redirect loop detected on Conduit \"" 
                                          + getConduitName() 
                                          + "\" on '" 
                                          + newURL
                                          + "'");
                }
                inStream = null;
                
                address = new URL(newURL);
                // We are going to redirect.
                // Remove any Server Authentication Information for the previous
                // URL.
                Map<String, List<String>> headers = 
                                    getSetProtocolHeaders(outMessage);
                headers.remove("Authorization");
                headers.remove("Proxy-Authorization");
                
                // If user configured this Conduit with preemptive authorization
                // it is meant to make it to the end. (Too bad that information
                // went to every URL along the way, but that's what the user 
                // wants!
                setHeadersByAuthorizationPolicy(outMessage, address, headers);
                doConnection();
                return true;
            }
            return false;
        }
        
        public boolean isRepeatable() {
            return false;
        }

        public boolean isChunked() {
            return this.isChunking;
        }

        public long getContentLength() {
            return contentLength;
        }

        public Header getContentType() {
            return new BasicHeader(Message.CONTENT_TYPE, contentType);
        }

        public Header getContentEncoding() {
            return null;
        }

        public InputStream getContent() throws IOException, IllegalStateException {
            return inStream;
        }

        public boolean isStreaming() {
            return true;
        }

        public void consumeContent() throws IOException {
        }

        public void produceContent(final ContentEncoder encoder, 
                                   IOControl ioctrl) throws IOException {
            if (writeTo(encoder)) {
                encoder.complete();
            }
        }

        public void finish() throws IOException {
        }
        
        public synchronized void write(byte[] b, int off, int len) throws IOException {
            if (!connected
                && isChunking 
                && ((outStream.size() + len) > this.chunkThreshold)) {
                doConnection();
            }
            written = true;
            if (isChunking) {
                if (outStream.size() > 0 
                    && ((outStream.size() + len) > (64 * 1024))) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                outStream.write(b, off, len);
            }
            if (requestStream != null) {
                requestStream.write(b, off, len);
            }
        }
        public synchronized void write(int b) throws IOException {
            if (!connected
                && isChunking 
                && ((outStream.size() + 1) > this.chunkThreshold)) {
                doConnection();
            }
            written = true;
            if (isChunking) {
                outStream.write(b);
            }
            if (requestStream != null) {
                requestStream.write(b);
            }
        }
        public synchronized void close() throws IOException {
            closed = true;
            if (!connected) {
                doConnection();
            }
            if (isOneway(outMessage.getExchange())) {
                //for a one way, we at least need to wait for the response code
                while (responseCode == 0) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (responseCode != HttpURLConnection.HTTP_ACCEPTED
                    && responseCode != HttpURLConnection.HTTP_OK) {
                    throw new IOException(responseMessage);
                }
            }
        }

        public void consumeContent(final ContentDecoder decoder, IOControl ioctrl) throws IOException {
            if (inStream != null) {
                return;
            }
            inStream = new InputStream() {
                public int read(byte b[], int off, int len) throws IOException {
                    if (decoder.isCompleted()) {
                        return -1;
                    }
                    ByteBuffer buf = ByteBuffer.wrap(b, off, len);
                    int i = decoder.read(buf);
                    while (i == 0) {
                        i = decoder.read(buf);
                    }
                    return i;
                }

                public int read() throws IOException {
                    if (decoder.isCompleted()) {
                        return -1;
                    }
                    byte[] bytes = new byte[1];
                    int i = read(bytes, 0, 1);
                    if (i == -1) {
                        return -1;
                    }
                    i = bytes[0];
                    i &= 0xFF;
                    return i;
                }
                public void close() throws IOException {
                    if (!decoder.isCompleted()) {
                        ByteBuffer buf = ByteBuffer.allocate(4096);
                        int i = decoder.read(buf);
                        while (i != -1) {
                            buf.clear();
                            i = decoder.read(buf);
                        }
                    }
                }
            };
            this.handleResponseInternal();
        }
        public void writeTo(OutputStream outstream) throws IOException {
            throw new UnsupportedOperationException("Not used for NIO");
        }
    }


    protected void sendException(Message m, Exception exception) {
        Message m2 = new MessageImpl();
        m2.setExchange(m.getExchange());
        m2.setContent(Exception.class, exception);
        m.getExchange().put(Exception.class, exception);
        this.incomingObserver.onMessage(m2);
    }
    
}
