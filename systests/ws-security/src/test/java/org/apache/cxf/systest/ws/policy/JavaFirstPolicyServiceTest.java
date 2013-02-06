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

package org.apache.cxf.systest.ws.policy;

import java.io.InputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.ws.soap.SOAPFaultException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import org.apache.cxf.endpoint.Client;
import org.apache.cxf.frontend.ClientProxy;
import org.apache.cxf.helpers.DOMUtils;
import org.apache.cxf.helpers.IOUtils;
import org.apache.cxf.service.model.MessageInfo.Type;
import org.apache.cxf.systest.ws.common.SecurityTestUtil;
import org.apache.cxf.systest.ws.policy.javafirst.BindingSimpleServiceClient;
import org.apache.cxf.systest.ws.policy.javafirst.NoAlternativesOperationSimpleServiceClient;
import org.apache.cxf.systest.ws.policy.javafirst.OperationSimpleServiceClient;
import org.apache.cxf.systest.ws.policy.server.JavaFirstPolicyServer;
import org.apache.cxf.systest.ws.wssec11.client.UTPasswordCallback;
import org.apache.cxf.testutil.common.AbstractBusClientServerTestBase;
import org.apache.cxf.ws.policy.PolicyConstants;
import org.apache.cxf.ws.security.wss4j.WSS4JOutInterceptor;
import org.apache.neethi.Constants;
import org.apache.ws.security.WSConstants;
import org.apache.ws.security.handler.WSHandlerConstants;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;


import org.springframework.context.support.ClassPathXmlApplicationContext;

public class JavaFirstPolicyServiceTest extends AbstractBusClientServerTestBase {
    static final String PORT = allocatePort(JavaFirstPolicyServer.class);
    static final String PORT2 = allocatePort(JavaFirstPolicyServer.class, 2);
    static final String PORT3 = allocatePort(JavaFirstPolicyServer.class, 3);

    private static final String WSDL_NAMESPACE = "http://schemas.xmlsoap.org/wsdl/";

    @BeforeClass
    public static void startServers() throws Exception {
        assertTrue("Server failed to launch",
        // run the server in the same process
        // set this to false to fork
                   launchServer(JavaFirstPolicyServer.class, true));
    }

    @org.junit.AfterClass
    public static void cleanup() throws Exception {
        SecurityTestUtil.cleanup();
        stopAllServers();
    }

    @org.junit.Test
    public void testUsernameTokenInterceptorNoPasswordValidation() {
        System.setProperty("testutil.ports.JavaFirstPolicyServer", PORT);

        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(
            "org/apache/cxf/systest/ws/policy/client/javafirstclient.xml");

        JavaFirstAttachmentPolicyService svc = ctx.getBean("JavaFirstAttachmentPolicyServiceClient",
                                                           JavaFirstAttachmentPolicyService.class);

        WSS4JOutInterceptor wssOut = addToClient(svc);

        // just some basic sanity tests first to make sure that auth is working where password is provided.
        wssOut.setProperties(getPasswordProperties("alice", "password"));
        svc.doInputMessagePolicy();

        wssOut.setProperties(getPasswordProperties("alice", "passwordX"));
        try {
            svc.doInputMessagePolicy();
            fail("Expected authentication failure");
        } catch (Exception e) {
            assertTrue(true);
        }

        wssOut.setProperties(getNoPasswordProperties("alice"));

        try {
            svc.doInputMessagePolicy();
            fail("Expected authentication failure");
        } catch (Exception e) {
            assertTrue(true);
        }
    }

    @org.junit.Test
    public void testUsernameTokenPolicyValidatorNoPasswordValidation() {
        System.setProperty("testutil.ports.JavaFirstPolicyServer.2", PORT2);

        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(
            "org/apache/cxf/systest/ws/policy/client/javafirstclient.xml");

        SslUsernamePasswordAttachmentService svc = ctx.getBean("SslUsernamePasswordAttachmentServiceClient",
                                                               SslUsernamePasswordAttachmentService.class);

        WSS4JOutInterceptor wssOut = addToClient(svc);

        // just some basic sanity tests first to make sure that auth is working where password is provided.
        wssOut.setProperties(getPasswordProperties("alice", "password"));
        svc.doSslAndUsernamePasswordPolicy();

        wssOut.setProperties(getPasswordProperties("alice", "passwordX"));
        try {
            svc.doSslAndUsernamePasswordPolicy();
            fail("Expected authentication failure");
        } catch (Exception e) {
            assertTrue(true);
        }

        wssOut.setProperties(getNoPasswordProperties("alice"));

        try {
            svc.doSslAndUsernamePasswordPolicy();
            fail("Expected authentication failure");
        } catch (Exception e) {
            assertTrue(true);
        }
    }

    @Test
    public void testBindingNoClientCertAlternativePolicy() {
        System.setProperty("testutil.ports.JavaFirstPolicyServer", PORT);

        ClassPathXmlApplicationContext clientContext = new ClassPathXmlApplicationContext(new String[] {
            "org/apache/cxf/systest/ws/policy/client/sslnocertclient.xml"
        });

        BindingSimpleServiceClient simpleService = clientContext.getBean("BindingSimpleServiceClient",
                                                                         BindingSimpleServiceClient.class);

        try {
            simpleService.doStuff();
            fail("Expected exception as no credentials");
        } catch (SOAPFaultException e) {
            assertTrue(true);
        }

        WSS4JOutInterceptor wssOut = addToClient(simpleService);

        wssOut.setProperties(getNoPasswordProperties("alice"));
        try {
            simpleService.doStuff();
            fail("Expected exception as no password and no client cert");
        } catch (SOAPFaultException e) {
            assertTrue(true);
        }

        wssOut.setProperties(getPasswordProperties("alice", "password"));
        simpleService.doStuff();
    }

    @Test
    public void testBindingClientCertAlternativePolicy() {
        System.setProperty("testutil.ports.JavaFirstPolicyServer.3", PORT3);

        ClassPathXmlApplicationContext clientContext = new ClassPathXmlApplicationContext(new String[] {
            "org/apache/cxf/systest/ws/policy/client/sslcertclient.xml"
        });

        BindingSimpleServiceClient simpleService = clientContext.getBean("BindingSimpleServiceClient",
                                                                         BindingSimpleServiceClient.class);

        try {
            simpleService.doStuff();
            fail("Expected exception as no credentials");
        } catch (SOAPFaultException e) {
            assertTrue(true);
        }

        WSS4JOutInterceptor wssOut = addToClient(simpleService);

        wssOut.setProperties(getNoPasswordProperties("alice"));
        simpleService.doStuff();

        wssOut.setProperties(getPasswordProperties("alice", "password"));

        // this is successful because the alternative policy allows a password to be specified.
        simpleService.doStuff();
    }

    @Test
    public void testNoAltOperationNoClientCertAlternativePolicy() {
        System.setProperty("testutil.ports.JavaFirstPolicyServer.3", PORT3);

        ClassPathXmlApplicationContext clientContext = new ClassPathXmlApplicationContext(new String[] {
            "org/apache/cxf/systest/ws/policy/client/sslnocertclient.xml"
        });

        NoAlternativesOperationSimpleServiceClient simpleService = clientContext
            .getBean("NoAlternativesOperationSimpleServiceClient",
                     NoAlternativesOperationSimpleServiceClient.class);

        try {
            simpleService.doStuff();
            fail("Expected exception as no credentials");
        } catch (SOAPFaultException e) {
            assertTrue(true);
        }

        WSS4JOutInterceptor wssOut = addToClient(simpleService);

        wssOut.setProperties(getNoPasswordProperties("alice"));
        try {
            simpleService.doStuff();
            fail("Expected exception as no password and no client cert");
        } catch (SOAPFaultException e) {
            assertTrue(true);
        }

        wssOut.setProperties(getPasswordProperties("alice", "password"));
        try {
            simpleService.doStuff();
            fail("Expected exception as no client cert and password not allowed");
        } catch (SOAPFaultException e) {
            assertTrue(true);
        }

        wssOut.setProperties(getNoPasswordProperties("alice"));
        try {
            simpleService.ping();
            fail("Expected exception as no password");
        } catch (SOAPFaultException e) {
            assertTrue(true);
        }

        wssOut.setProperties(getPasswordProperties("alice", "password"));
        simpleService.ping();
    }

    @Test
    public void testNoAltOperationClientCertAlternativePolicy() {
        System.setProperty("testutil.ports.JavaFirstPolicyServer.3", PORT3);

        ClassPathXmlApplicationContext clientContext = new ClassPathXmlApplicationContext(new String[] {
            "org/apache/cxf/systest/ws/policy/client/sslcertclient.xml"
        });

        NoAlternativesOperationSimpleServiceClient simpleService = clientContext
            .getBean("NoAlternativesOperationSimpleServiceClient",
                     NoAlternativesOperationSimpleServiceClient.class);

        try {
            simpleService.doStuff();
            fail("Expected exception as no credentials");
        } catch (SOAPFaultException e) {
            assertTrue(true);
        }

        WSS4JOutInterceptor wssOut = addToClient(simpleService);

        wssOut.setProperties(getNoPasswordProperties("alice"));
        simpleService.doStuff();

        wssOut.setProperties(getPasswordProperties("alice", "password"));
        try {
            simpleService.doStuff();
            fail("Expected exception as password not allowed");
        } catch (SOAPFaultException e) {
            assertTrue(true);
        }

        wssOut.setProperties(getNoPasswordProperties("alice"));
        try {
            simpleService.ping();
            fail("Expected exception as no password");
        } catch (SOAPFaultException e) {
            assertTrue(true);
        }

        wssOut.setProperties(getPasswordProperties("alice", "password"));
        simpleService.ping();
    }

    @Test
    @Ignore
    public void testOperationNoClientCertAlternativePolicy() {
        System.setProperty("testutil.ports.JavaFirstPolicyServer.3", PORT3);

        ClassPathXmlApplicationContext clientContext = new ClassPathXmlApplicationContext(new String[] {
            "org/apache/cxf/systest/ws/policy/client/sslnocertclient.xml"
        });

        OperationSimpleServiceClient simpleService = clientContext
            .getBean("OperationSimpleServiceClient", OperationSimpleServiceClient.class);

        try {
            simpleService.doStuff();
            fail("Expected exception as no credentials");
        } catch (SOAPFaultException e) {
            assertTrue(true);
        }

        WSS4JOutInterceptor wssOut = addToClient(simpleService);

        wssOut.setProperties(getNoPasswordProperties("alice"));
        try {
            simpleService.doStuff();
            fail("Expected exception as no password and no client cert");
        } catch (SOAPFaultException e) {
            assertTrue(true);
        }

        wssOut.setProperties(getPasswordProperties("alice", "password"));
        simpleService.doStuff();
    }

    @Test
    @Ignore
    public void testOperationClientCertAlternativePolicy() {
        System.setProperty("testutil.ports.JavaFirstPolicyServer.3", PORT3);

        ClassPathXmlApplicationContext clientContext = new ClassPathXmlApplicationContext(new String[] {
            "org/apache/cxf/systest/ws/policy/client/sslcertclient.xml"
        });

        OperationSimpleServiceClient simpleService = clientContext
            .getBean("OperationSimpleServiceClient", OperationSimpleServiceClient.class);

        try {
            simpleService.doStuff();
            fail("Expected exception as no credentials");
        } catch (SOAPFaultException e) {
            assertTrue(true);
        }

        WSS4JOutInterceptor wssOut = addToClient(simpleService);

        wssOut.setProperties(getNoPasswordProperties("alice"));
        simpleService.doStuff();

        wssOut.setProperties(getPasswordProperties("alice", "password"));

        try {
            simpleService.doStuff();
            fail("Expected exception password is not supported");
        } catch (SOAPFaultException e) {
            assertTrue(true);
        }
    }

    private WSS4JOutInterceptor addToClient(Object svc) {
        Client client = ClientProxy.getClient(svc);
        WSS4JOutInterceptor wssOut = new WSS4JOutInterceptor();
        client.getEndpoint().getOutInterceptors().add(wssOut);
        return wssOut;
    }

    private Map<String, Object> getPasswordProperties(String username, String password) {
        UTPasswordCallback callback = new UTPasswordCallback();
        callback.setAliasPassword(username, password);

        Map<String, Object> outProps = new HashMap<String, Object>();
        outProps.put(WSHandlerConstants.ACTION, WSHandlerConstants.USERNAME_TOKEN);
        outProps.put(WSHandlerConstants.PASSWORD_TYPE, WSConstants.PW_TEXT);
        outProps.put(WSHandlerConstants.PW_CALLBACK_REF, callback);
        outProps.put(WSHandlerConstants.USER, username);
        return outProps;
    }

    private Map<String, Object> getNoPasswordProperties(String username) {
        Map<String, Object> outProps = new HashMap<String, Object>();
        outProps.put(WSHandlerConstants.ACTION, WSHandlerConstants.USERNAME_TOKEN);
        outProps.put(WSHandlerConstants.PASSWORD_TYPE, WSConstants.PW_NONE);
        outProps.put(WSHandlerConstants.USER, username);
        return outProps;
    }

    @org.junit.Test
    public void testJavaFirstAttachmentWsdl() throws Exception {
        Document doc = loadWsdl("JavaFirstAttachmentPolicyService");
        testJavaFirstAttachmentWsdl(doc);

        // verify that the policy attachment not being defensively copied is working ok still!
        Document doc2 = loadWsdl("JavaFirstAttachmentPolicyService2");
        testJavaFirstAttachmentWsdl(doc2);
    }

    private void testJavaFirstAttachmentWsdl(Document doc) throws Exception {
        Element binding = DOMUtils.getFirstChildWithName(doc.getDocumentElement(), WSDL_NAMESPACE, "binding");
        assertNotNull(binding);

        List<Element> operationMessages = DOMUtils.getChildrenWithName(binding, WSDL_NAMESPACE, "operation");
        assertEquals(4, operationMessages.size());

        Element doOperationLevelPolicy = getOperationElement("doOperationLevelPolicy", operationMessages);
        assertEquals("#UsernameToken",
                     getOperationPolicyReferenceId(doOperationLevelPolicy, Constants.URI_POLICY_13_NS));

        Element doInputMessagePolicy = getOperationElement("doInputMessagePolicy", operationMessages);
        assertEquals("#UsernameToken",
                     getMessagePolicyReferenceId(doInputMessagePolicy, Type.INPUT, Constants.URI_POLICY_13_NS));
        assertNull(getMessagePolicyReferenceId(doInputMessagePolicy, Type.OUTPUT, Constants.URI_POLICY_13_NS));

        Element doOutputMessagePolicy = getOperationElement("doOutputMessagePolicy", operationMessages);
        assertEquals("#UsernameToken",
                     getMessagePolicyReferenceId(doOutputMessagePolicy, Type.OUTPUT,
                                                 Constants.URI_POLICY_13_NS));
        assertNull(getMessagePolicyReferenceId(doOutputMessagePolicy, Type.INPUT, Constants.URI_POLICY_13_NS));

        Element doNoPolicy = getOperationElement("doNoPolicy", operationMessages);
        assertNull(getMessagePolicyReferenceId(doNoPolicy, Type.INPUT, Constants.URI_POLICY_13_NS));
        assertNull(getMessagePolicyReferenceId(doNoPolicy, Type.OUTPUT, Constants.URI_POLICY_13_NS));

        // ensure that the policies are attached to the wsdl:definitions
        List<Element> policyMessages = DOMUtils.getChildrenWithName(doc.getDocumentElement(),
                                                                    Constants.URI_POLICY_13_NS, "Policy");
        assertEquals(1, policyMessages.size());

        assertEquals("UsernameToken", getPolicyId(policyMessages.get(0)));

        Element exactlyOne = DOMUtils.getFirstChildWithName(policyMessages.get(0), "", "ExactlyOne");
        assertNull(exactlyOne);

        exactlyOne = DOMUtils.getFirstChildWithName(policyMessages.get(0), Constants.URI_POLICY_13_NS,
                                                    "ExactlyOne");
        assertNotNull(exactlyOne);
    }

    @org.junit.Test
    public void testJavaFirstWsdl() throws Exception {
        Document doc = loadWsdl("JavaFirstPolicyService");

        Element portType = DOMUtils.getFirstChildWithName(doc.getDocumentElement(), WSDL_NAMESPACE,
                                                          "portType");
        assertNotNull(portType);

        List<Element> operationMessages = DOMUtils.getChildrenWithName(portType, WSDL_NAMESPACE, "operation");
        assertEquals(5, operationMessages.size());

        Element operationOne = getOperationElement("doOperationOne", operationMessages);
        assertEquals("#InternalTransportAndUsernamePolicy",
                     getMessagePolicyReferenceId(operationOne, Type.INPUT, Constants.URI_POLICY_NS));
        Element operationTwo = getOperationElement("doOperationTwo", operationMessages);
        assertEquals("#TransportAndUsernamePolicy",
                     getMessagePolicyReferenceId(operationTwo, Type.INPUT, Constants.URI_POLICY_NS));
        Element operationThree = getOperationElement("doOperationThree", operationMessages);
        assertEquals("#InternalTransportAndUsernamePolicy",
                     getMessagePolicyReferenceId(operationThree, Type.INPUT, Constants.URI_POLICY_NS));
        Element operationFour = getOperationElement("doOperationFour", operationMessages);
        assertEquals("#TransportAndUsernamePolicy",
                     getMessagePolicyReferenceId(operationFour, Type.INPUT, Constants.URI_POLICY_NS));
        Element operationPing = getOperationElement("doPing", operationMessages);
        assertNull(getMessagePolicyReferenceId(operationPing, Type.INPUT, Constants.URI_POLICY_NS));

        List<Element> policyMessages = DOMUtils.getChildrenWithName(doc.getDocumentElement(),
                                                                    Constants.URI_POLICY_NS, "Policy");

        assertEquals(2, policyMessages.size());

        // validate that both the internal and external policies are included
        assertEquals("TransportAndUsernamePolicy", getPolicyId(policyMessages.get(0)));
        assertEquals("InternalTransportAndUsernamePolicy", getPolicyId(policyMessages.get(1)));
    }

    private Document loadWsdl(String serviceName) throws Exception {
        HttpURLConnection connection = getHttpConnection("http://localhost:" + PORT + "/" + serviceName
                                                         + "?wsdl");
        InputStream is = connection.getInputStream();
        String wsdlContents = IOUtils.toString(is);

        // System.out.println(wsdlContents);
        return DOMUtils.readXml(new StringReader(wsdlContents));
    }

    private String getPolicyId(Element element) {
        return element.getAttributeNS(PolicyConstants.WSU_NAMESPACE_URI, PolicyConstants.WSU_ID_ATTR_NAME);
    }

    private Element getOperationElement(String operationName, List<Element> operationMessages) {
        Element operationElement = null;
        for (Element operation : operationMessages) {
            if (operationName.equals(operation.getAttribute("name"))) {
                operationElement = operation;
                break;
            }
        }
        assertNotNull(operationElement);
        return operationElement;
    }

    private String getMessagePolicyReferenceId(Element operationElement, Type type, String policyNamespace) {
        Element messageElement = DOMUtils.getFirstChildWithName(operationElement, WSDL_NAMESPACE, type.name()
            .toLowerCase());
        assertNotNull(messageElement);
        Element policyReference = DOMUtils.getFirstChildWithName(messageElement, policyNamespace,
                                                                 "PolicyReference");
        if (policyReference != null) {
            return policyReference.getAttribute("URI");
        } else {
            return null;
        }
    }

    private String getOperationPolicyReferenceId(Element operationElement, String policyNamespace) {
        Element policyReference = DOMUtils.getFirstChildWithName(operationElement, policyNamespace,
                                                                 "PolicyReference");
        if (policyReference != null) {
            return policyReference.getAttribute("URI");
        } else {
            return null;
        }
    }
}
