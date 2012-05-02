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
package org.apache.cxf.rs.security.saml.sso.filter;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.UUID;
import java.util.logging.Logger;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.UriBuilder;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import org.apache.cxf.common.i18n.BundleUtils;
import org.apache.cxf.common.logging.LogUtils;
import org.apache.cxf.common.util.Base64Utility;
import org.apache.cxf.helpers.DOMUtils;
import org.apache.cxf.jaxrs.ext.RequestHandler;
import org.apache.cxf.jaxrs.impl.HttpHeadersImpl;
import org.apache.cxf.message.Message;
import org.apache.cxf.rs.security.saml.DeflateEncoderDecoder;
import org.apache.cxf.rs.security.saml.sso.SSOConstants;
import org.apache.cxf.rs.security.saml.sso.state.RequestState;
import org.apache.cxf.rs.security.saml.sso.state.ResponseState;
import org.apache.cxf.rs.security.saml.sso.state.SPStateManager;
import org.apache.ws.security.saml.ext.OpenSAMLUtil;
import org.apache.ws.security.util.DOM2Writer;
import org.opensaml.common.SAMLVersion;
import org.opensaml.saml2.core.AuthnContextClassRef;
import org.opensaml.saml2.core.AuthnContextComparisonTypeEnumeration;
import org.opensaml.saml2.core.AuthnRequest;
import org.opensaml.saml2.core.Issuer;
import org.opensaml.saml2.core.NameIDPolicy;
import org.opensaml.saml2.core.RequestedAuthnContext;
import org.opensaml.xml.io.MarshallingException;

public abstract class AbstractServiceProviderFilter implements RequestHandler {
    
    protected static final Logger LOG = 
        LogUtils.getL7dLogger(AbstractServiceProviderFilter.class);
    protected static final ResourceBundle BUNDLE = 
        BundleUtils.getBundle(AbstractServiceProviderFilter.class);
    
    private String idpServiceAddress;
    private String issuerId;
    private String assertionConsumerServiceAddress;
    private long stateTimeToLive = SSOConstants.DEFAULT_STATE_TIME;
    
    private SPStateManager stateProvider;
    
    public void setAssertionConsumerServiceAddress(
            String assertionConsumerServiceAddress) {
        this.assertionConsumerServiceAddress = assertionConsumerServiceAddress;
    }

    public void setIssuerId(String issuerId) {
        this.issuerId = issuerId;
    }
    
    public void setIdpServiceAddress(String idpServiceAddress) {
        this.idpServiceAddress = idpServiceAddress;
    }

    public String getIdpServiceAddress() {
        return idpServiceAddress;
    }

    protected boolean checkSecurityContext(Message m) {
        HttpHeaders headers = new HttpHeadersImpl(m);
        Map<String, Cookie> cookies = headers.getCookies();
        
        Cookie securityContextCookie = cookies.get(SSOConstants.SECURITY_CONTEXT_TOKEN);
        if (securityContextCookie == null) {
            reportError("MISSING_RESPONSE_STATE");
            return false;
        }
        String contextKey = securityContextCookie.getValue();
        ResponseState responseState = stateProvider.getResponseState(contextKey);
        if (responseState == null) {
            reportError("MISSING_RESPONSE_STATE");
            return false;
        }
        long stateCreatedAt = responseState.getCreatedAt();
        if (new Date().after(new Date(stateCreatedAt + stateTimeToLive))) {
            reportError("EXPIRED_RESPONSE_STATE");
            stateProvider.removeResponseState(contextKey);
            return false;
        }
        //TODO: use ResponseState to set up a proper SecurityContext 
        //      on the current message
        return true;
    }
    
    protected AuthnRequest createAuthnRequest(Message m, Document doc) throws Exception {
        Issuer issuer =
            SamlpRequestComponentBuilder.createIssuer(issuerId);
        NameIDPolicy nameIDPolicy =
            SamlpRequestComponentBuilder.createNameIDPolicy(
                true, "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent", "Issuer"
            );
        
        AuthnContextClassRef authnCtxClassRef =
            SamlpRequestComponentBuilder.createAuthnCtxClassRef(
                "urn:oasis:names:tc:SAML:2.0:ac:classes:PasswordProtectedTransport"
            );
        RequestedAuthnContext authnCtx =
            SamlpRequestComponentBuilder.createRequestedAuthnCtxPolicy(
                AuthnContextComparisonTypeEnumeration.EXACT,
                Collections.singletonList(authnCtxClassRef), null
            );
        
        //CHECKSTYLE:OFF
        return SamlpRequestComponentBuilder.createAuthnRequest(
                getAbsoluteAssertionServiceAddress(m), 
                false, 
                false,
                "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST", 
                SAMLVersion.VERSION_20,
                issuer, 
                nameIDPolicy, 
                authnCtx
        );
      //CHECKSTYLE:ON
    }
    
    protected String encodeAuthnRequest(Element authnRequestElement)
        throws MarshallingException, IOException {
        String requestMessage = DOM2Writer.nodeToString(authnRequestElement);
        
        DeflateEncoderDecoder encoder = new DeflateEncoderDecoder();
        byte[] deflatedBytes = encoder.deflateToken(requestMessage.getBytes("UTF-8"));
        
        String encodedRequestMessage = Base64Utility.encode(deflatedBytes);
        return URLEncoder.encode(encodedRequestMessage, "UTF-8");
    }

    protected SamlRequestInfo createSamlRequestInfo(Message m) throws Exception {
        Document doc = DOMUtils.createDocument();
        doc.appendChild(doc.createElement("root"));
 
        AuthnRequest authnRequest = createAuthnRequest(m, doc);
        Element authnRequestElement = OpenSAMLUtil.toDom(authnRequest, doc);
        String authnRequestEncoded = encodeAuthnRequest(authnRequestElement);
        
        SamlRequestInfo info = new SamlRequestInfo();
        info.setEncodedSamlRequest(authnRequestEncoded);
        
        String originalRequestURI = (String)m.get(Message.REQUEST_URI);
        RequestState requestState = new RequestState(originalRequestURI,
                                                     idpServiceAddress,
                                                     authnRequest.getID(),
                                                     issuerId,
                                                     System.currentTimeMillis());
        
        String relayState = UUID.randomUUID().toString();
        stateProvider.setRequestState(relayState, requestState);
        info.setRelayState(relayState);
        
        return info;
    }
    
    private String getAbsoluteAssertionServiceAddress(Message m) {
        if (assertionConsumerServiceAddress == null) {    
            //TODO: Review the possibility of using this filter
            //for validating SAMLResponse too
            reportError("MISSING_ASSERTION_SERVICE_URL");
            throw new WebApplicationException(500);
        }
        if (!assertionConsumerServiceAddress.startsWith("http")) {
            String httpBasePath = (String)m.get("http.base.path");
            return UriBuilder.fromUri(httpBasePath)
                             .path(assertionConsumerServiceAddress)
                             .build()
                             .toString();
        } else {
            return assertionConsumerServiceAddress;
        }
    }
    
    protected void reportError(String code) {
        org.apache.cxf.common.i18n.Message errorMsg = 
            new org.apache.cxf.common.i18n.Message(code, BUNDLE);
        LOG.warning(errorMsg.toString());
    }
    
    public void setStateTimeToLive(long stateTime) {
        this.stateTimeToLive = stateTime;
    }

    public void setStateProvider(SPStateManager provider) {
        this.stateProvider = provider;
    }
    
}
