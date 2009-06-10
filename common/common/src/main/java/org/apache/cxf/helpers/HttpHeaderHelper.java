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

package org.apache.cxf.helpers;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class HttpHeaderHelper {
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String CONTENT_ID = "Content-ID";
    public static final String CONTENT_TRANSFER_ENCODING = "Content-Transfer-Encoding";
    public static final String COOKIE = "Cookie";
    public static final String TRANSFER_ENCODING = "Transfer-Encoding";
    public static final String CHUNKED = "chunked";
    public static final String CONNECTION = "Connection";
    public static final String CLOSE = "close";
    public static final String AUTHORIZATION = "Authorization";
    private static final String ISO88591 = Charset.forName("ISO-8859-1").name();
    
    private static Map<String, String> internalHeaders = new HashMap<String, String>();
    private static Map<String, String> encodings = new ConcurrentHashMap<String, String>();
    
    static {
        internalHeaders.put("Content-Type", "content-type");
        internalHeaders.put("Content-ID", "content-id");
        internalHeaders.put("Content-Transfer-Encoding", "content-transfer-encoding"); 
        internalHeaders.put("Transfer-Encoding", "transfer-encoding");
        internalHeaders.put("Connection", "connection");
        internalHeaders.put("authorization", "Authorization");
        internalHeaders.put("soapaction", "SOAPAction");
        internalHeaders.put("accept", "Accept");
    }
    
    private HttpHeaderHelper() {
        
    }
    
    public static List getHeader(Map<String, List<String>> headerMap, String key) {
        return headerMap.get(getHeaderKey(key));
    }
    
    public static String getHeaderKey(String key) {
        if (internalHeaders.containsKey(key)) {
            return internalHeaders.get(key);
        } else {
            return key;
        }
    }
    
    public static String findCharset(String contentType) {
        if (contentType == null) {
            return null;
        }
        int idx = contentType.indexOf("charset=");
        if (idx != -1) {
            String charset = contentType.substring(idx + 8);
            if (charset.indexOf(";") != -1) {
                charset = charset.substring(0, charset.indexOf(";")).trim();
            }
            if (charset.charAt(0) == '\"') {
                charset = charset.substring(1, charset.length() - 1);
            }
            return charset;
        }
        return null;
    }
    public static String mapCharset(String enc) {
        return mapCharset(enc, ISO88591);
    }    
    
    //helper to map the charsets that various things send in the http Content-Type header 
    //into something that is actually supported by Java and the Stax parsers and such.
    public static String mapCharset(String enc, String deflt) {
        if (enc == null) {
            return deflt;
        }
        //older versions of tomcat don't properly parse ContentType headers with stuff
        //after charset="UTF-8"
        int idx = enc.indexOf(";");
        if (idx != -1) {
            enc = enc.substring(0, idx);
        }
        // Charsets can be quoted. But it's quite certain that they can't have escaped quoted or
        // anything like that.
        enc = enc.replace("\"", "").trim();
        enc = enc.replace("'", "");
        if ("".equals(enc)) {
            return deflt;
        }
        String newenc = encodings.get(enc);
        if (newenc == null) {
            try {
                newenc = Charset.forName(enc).name();
            } catch (IllegalCharsetNameException icne) {
                return null;
            } catch (UnsupportedCharsetException uce) {
                return null;
            }
            encodings.put(enc, newenc);
        }
        return newenc;
    }
}
