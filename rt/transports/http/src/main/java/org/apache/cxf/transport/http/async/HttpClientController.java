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
import java.net.SocketTimeoutException;
import java.net.URL;

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
    
    HttpClientController() {
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
                    Message m = (Message)conn.getContext().getAttribute("MESSAGE");
                    m.get(AsyncHTTPConduit.class).sendException(m, new SocketTimeoutException());
                }
            };

            
            final IOEventDispatch ioEventDispatch 
                = new DefaultClientIOEventDispatch(handler, params) {
                    protected NHttpClientIOTarget createConnection(IOSession session) {
                        Message m = (Message)session.getAttribute(IOSession.ATTACHMENT_KEY);
                        HTTPClientPolicy client = (HTTPClientPolicy)m
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
    public void execute(AsyncHTTPConduit conduit, 
                        final URL address, 
                        HttpUriRequest request, 
                        final Message message) throws IOException {
        int port = address.getPort();
        if (port == -1) {
            port = 80;
        }
        InetSocketAddress add = new InetSocketAddress(address.getHost(), port);
        message.put(HttpUriRequest.class, request);
        synchronized (message) {
            SessionRequest req 
                = ioReactor.connect(add, null, message,
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
        Message m = (Message)context.getAttribute("MESSAGE");
        if (m == null) {
            return null;
        }
        return m.get(HttpUriRequest.class);
    }

    public ConsumingNHttpEntity responseEntity(HttpResponse response, HttpContext context)
        throws IOException {
        Message m = (Message)context.getAttribute("MESSAGE");
        WrappedOutputStream out = m.get(WrappedOutputStream.class);
        out.setResponse(response);
        return out;
    }

    public void handleResponse(HttpResponse response, HttpContext context) throws IOException {
    }

    public void finalizeContext(HttpContext context) {
        context.removeAttribute("MESSAGE");
    }
    
}