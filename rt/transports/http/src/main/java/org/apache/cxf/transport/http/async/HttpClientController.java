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
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cxf.Bus;
import org.apache.cxf.BusFactory;
import org.apache.cxf.buslifecycle.BusLifeCycleListener;
import org.apache.cxf.buslifecycle.BusLifeCycleManager;
import org.apache.cxf.message.Message;
import org.apache.cxf.transport.http.async.AsyncHTTPConduit.WrappedOutputStream;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.apache.cxf.version.Version;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.nio.DefaultClientIOEventDispatch;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.NHttpClientIOTarget;
import org.apache.http.nio.NHttpConnection;
import org.apache.http.nio.entity.ConsumingNHttpEntity;
import org.apache.http.nio.protocol.AsyncNHttpClientHandler;
import org.apache.http.nio.protocol.NHttpRequestExecutionHandler;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.nio.reactor.IOSession;
import org.apache.http.nio.reactor.SessionRequest;
import org.apache.http.nio.reactor.SessionRequestCallback;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.BasicHttpProcessor;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.RequestConnControl;
import org.apache.http.protocol.RequestContent;
import org.apache.http.protocol.RequestExpectContinue;
import org.apache.http.protocol.RequestTargetHost;
import org.apache.http.protocol.RequestUserAgent;

public class HttpClientController implements BusLifeCycleListener,
    NHttpRequestExecutionHandler {
    ConnectingIOReactor ioReactor;
    
    Map<String, Stack<SessionRequest>> sessions 
        = new ConcurrentHashMap<String, Stack<SessionRequest>>();
    
    
    HttpClientController() {
    }
    
    static final class MessageHolder {
        private Message msg;
        private String key;
        private SessionRequest session;
        
        public MessageHolder(Message m, String key) {
            msg = m;
            this.key = key;
        }
        public String getKey() {
            return key;
        }
        public Message get() {
            return msg;
        }
        public Message getAndRemove() {
            Message m = msg;
            msg = null;
            return m;
        }
        public void set(Message message) {
            msg = message;
        }
        public void setSession(SessionRequest r) {
            session = r;
        }
        public SessionRequest getSession() {
            return session;
        }
    }
    
    public void setUp() {
        try {
            HttpParams params = new BasicHttpParams();
            params.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 60000)
                .setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 30000)
                .setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 8 * 1024)
                .setBooleanParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false)
                .setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true)
                .setParameter(CoreProtocolPNames.USER_AGENT, Version.getCompleteVersionString());
            ioReactor = new DefaultConnectingIOReactor(4, params);
            
            
            BasicHttpProcessor httpproc = new BasicHttpProcessor();
            httpproc.addInterceptor(new RequestContent());
            httpproc.addInterceptor(new RequestTargetHost());
            httpproc.addInterceptor(new RequestConnControl());
            httpproc.addInterceptor(new RequestUserAgent());
            httpproc.addInterceptor(new RequestExpectContinue());
            
            AsyncNHttpClientHandler handler = new AsyncNHttpClientHandler(
                httpproc,
                this,
                new DefaultConnectionReuseStrategy(),
                params) {
                protected void handleTimeout(final NHttpConnection conn) {
                    super.handleTimeout(conn);
                    MessageHolder m = (MessageHolder)conn.getContext().getAttribute("MESSAGE");
                    if (m.get() != null) {
                        m.get().get(AsyncHTTPConduit.class).sendException(m.get(),
                                                                      new SocketTimeoutException());
                    }
                }
            };

            
            final IOEventDispatch ioEventDispatch 
                = new DefaultClientIOEventDispatch(handler, params) {
                    protected NHttpClientIOTarget createConnection(IOSession session) {
                        MessageHolder m = (MessageHolder)session.getAttribute(IOSession.ATTACHMENT_KEY);
                        HTTPClientPolicy client = (HTTPClientPolicy)m.get()
                            .get(HTTPClientPolicy.class.getName() + ".complete");
                        if (client != null) {
                            session.setSocketTimeout((int)client.getReceiveTimeout());
                        }
                        return super.createConnection(session);
                    }
                };
            
            Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        ioReactor.execute(ioEventDispatch);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            t.setDaemon(true);
            t.start();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    public SessionRequest findSession(String key) {
        Stack<SessionRequest> stack = sessions.get(key);
        if (stack != null && !stack.isEmpty()) {
            return stack.pop();
        }
        return null;
    }
    
    public void execute(AsyncHTTPConduit conduit, 
                        final URL address, 
                        HttpUriRequest request, 
                        final Message message) throws IOException {
        message.put(HttpUriRequest.class, request);
        int port = address.getPort();
        if (port == -1) {
            port = 80;
        }
        String key = address.getHost() + ":" + port;
        SessionRequest srequest = findSession(key);
        if (srequest != null && !srequest.getSession().isClosed()) {
            message.put(SessionRequest.class, srequest);
            Object o = srequest.getSession().getAttribute("http.connection");
            ((MessageHolder)srequest.getAttachment()).set(message);
            NHttpConnection ioc = (NHttpConnection)o;
            HTTPClientPolicy client = (HTTPClientPolicy)message
                .get(HTTPClientPolicy.class.getName() + ".complete");
            if (client != null) {
                ioc.setSocketTimeout((int)client.getReceiveTimeout());
            }

            ioc.requestOutput();
            return;
        }
        SocketAddress add = new InetSocketAddress(address.getHost(), port);
        synchronized (message) {
            SessionRequest req 
                = ioReactor.connect(add, null, new MessageHolder(message, key),
                    new SessionRequestCallback() {
                        public void completed(SessionRequest request) {
                            synchronized (message) {
                                message.notifyAll();
                            }
                        }

                        public void failed(SessionRequest request) {
                            message.put(IOException.class, 
                                        new ConnectException("Failed to connect to " + address));
                            synchronized (message) {
                                message.notifyAll();
                            }
                        }

                        public void timeout(SessionRequest request) {
                            message.put(IOException.class, 
                                        new ConnectException("Failed to connect to " + address));
                            synchronized (message) {
                                message.notifyAll();
                            }
                        }

                        public void cancelled(SessionRequest request) {
                            message.put(IOException.class, 
                                        new ConnectException("Failed to connect to " + address));
                            synchronized (message) {
                                message.notifyAll();
                            }
                        }
                    });
            HTTPClientPolicy client 
                = (HTTPClientPolicy)message.get(HTTPClientPolicy.class.getName() + ".complete");
            req.setConnectTimeout((int)client.getConnectionTimeout());
            
            try {
                message.wait();
                if (message.get(IOException.class) != null) {
                    throw message.get(IOException.class);
                }
            } catch (InterruptedException e) {
                //ignore
            }
            message.put(SessionRequest.class, req);
            srequest = req;
        }
    }
    
    public static HttpClientController getHttpClientController(Message message) {
        Bus bus = message.getExchange() == null ? null : message.getExchange().getBus();
        if (bus == null) {
            bus = BusFactory.getThreadDefaultBus();
        }
        HttpClientController stuff = bus.getExtension(HttpClientController.class);
        if (stuff == null) {
            stuff = registerHttpClient(bus);
        }
        return stuff;
    }
    static synchronized HttpClientController registerHttpClient(Bus bus) {
        HttpClientController stuff = bus.getExtension(HttpClientController.class);
        if (stuff == null) {
            stuff = new HttpClientController();
            stuff.setUp();
            bus.setExtension(stuff, HttpClientController.class);
            bus.getExtension(BusLifeCycleManager.class).registerLifeCycleListener(stuff);
        }
        return stuff;
    }

    public void initComplete() {
    }
    public void preShutdown() {
    }
    public void postShutdown() {
    }

    public void initalizeContext(HttpContext context, Object attachment) {
        context.setAttribute("MESSAGE", attachment);
    }

    public HttpRequest submitRequest(HttpContext context) {
        Message m = ((MessageHolder)context.getAttribute("MESSAGE")).get();
        if (m == null) {
            return null;
        }
        return (HttpRequest)m.remove(HttpUriRequest.class.getName());
    }

    public ConsumingNHttpEntity responseEntity(HttpResponse response, HttpContext context)
        throws IOException {
        MessageHolder holder = (MessageHolder)context.getAttribute("MESSAGE");
        Message m = holder.get();
        WrappedOutputStream out = m.get(WrappedOutputStream.class);
        out.setResponse(response);
        return out;
    }

    public void handleResponse(HttpResponse response, HttpContext context) throws IOException {
        MessageHolder holder = (MessageHolder)context.getAttribute("MESSAGE");
        Message m = holder.getAndRemove();
        SessionRequest r = m.get(SessionRequest.class);
        holder.setSession(r);
        String key = holder.getKey();
        Stack<SessionRequest> srs = sessions.get(key);
        if (srs == null) {
            srs = new Stack<SessionRequest>();
            sessions.put(key, srs);
        }
        srs.push(r);
    }

    public void finalizeContext(HttpContext context) {
        MessageHolder holder = (MessageHolder)context.getAttribute("MESSAGE");
        String key = holder.getKey();
        Stack<SessionRequest> srs = sessions.get(key);
        if (srs != null) {
            srs.removeElement(holder.getSession());
        }        
    }
    
}