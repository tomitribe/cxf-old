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

package org.apache.cxf.anonymous_complex_type;

import javax.jws.WebService;

import org.apache.cxf.anonymous_complex_type.SplitNameResponse.Names;

@WebService(serviceName = "anonymous_complex_type_service", 
        portName = "anonymous_complex_typeSOAP", 
        endpointInterface = "org.apache.cxf.anonymous_complex_type.AnonymousComplexType", 
        targetNamespace = "http://cxf.apache.org/anonymous_complex_type/")
        
public class AnonymousComplexTypeImpl implements AnonymousComplexType {

    public Names splitName(String name) {
        // TODO Auto-generated method stub
        if (name != null) {
            Names names = new Names();
            int pos = name.indexOf(" ");
            if (pos > 0) {
                names.setFirst(name.substring(0, pos));
                names.setSecond(name.substring(pos + 1));
            } else {
                names.setFirst(name);
            }
            return names;
        }
        return null;
    }
}
