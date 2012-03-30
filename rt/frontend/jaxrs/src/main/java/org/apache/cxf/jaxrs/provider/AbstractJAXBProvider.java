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

package org.apache.cxf.jaxrs.provider;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.logging.Logger;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.ValidationEventHandler;
import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.validation.Schema;

import org.xml.sax.helpers.DefaultHandler;

import org.apache.cxf.common.i18n.BundleUtils;
import org.apache.cxf.common.logging.LogUtils;
import org.apache.cxf.common.util.PackageUtils;
import org.apache.cxf.jaxb.JAXBUtils;
import org.apache.cxf.jaxrs.ext.MessageContext;
import org.apache.cxf.jaxrs.model.ClassResourceInfo;
import org.apache.cxf.jaxrs.utils.InjectionUtils;
import org.apache.cxf.jaxrs.utils.JAXRSUtils;
import org.apache.cxf.jaxrs.utils.ResourceUtils;
import org.apache.cxf.jaxrs.utils.schemas.SchemaHandler;
import org.apache.cxf.message.Message;
import org.apache.cxf.phase.PhaseInterceptorChain;
import org.apache.cxf.staxutils.DelegatingXMLStreamWriter;
import org.apache.cxf.staxutils.DepthRestrictingStreamReader;
import org.apache.cxf.staxutils.DepthXMLStreamReader;
import org.apache.cxf.staxutils.DocumentDepthProperties;
import org.apache.cxf.staxutils.StaxStreamFilter;
import org.apache.cxf.staxutils.StaxUtils;

public abstract class AbstractJAXBProvider extends AbstractConfigurableProvider
    implements MessageBodyReader<Object>, MessageBodyWriter<Object> {
    
    protected static final ResourceBundle BUNDLE = BundleUtils.getBundle(AbstractJAXBProvider.class);

    protected static final Logger LOG = LogUtils.getL7dLogger(AbstractJAXBProvider.class);
    private static final String JAXB_DEFAULT_NAMESPACE = "##default";
    private static final String JAXB_DEFAULT_NAME = "##default";
    
    private static Map<String, JAXBContext> packageContexts = new HashMap<String, JAXBContext>();
    private static Map<Class<?>, JAXBContext> classContexts = new HashMap<Class<?>, JAXBContext>();

   
    protected Set<Class<?>> collectionContextClasses = new HashSet<Class<?>>();
    protected JAXBContext collectionContext; 
    
    protected Map<String, String> jaxbElementClassMap = Collections.emptyMap();
    protected boolean unmarshalAsJaxbElement;
    protected boolean marshalAsJaxbElement;
    
    protected Map<String, String> outElementsMap;
    protected Map<String, String> outAppendMap;
    protected List<String> outDropElements;
    protected List<String> inDropElements;
    protected Map<String, String> inElementsMap;
    protected Map<String, String> inAppendMap;
    private boolean attributesToElements;
    
    private MessageContext mc;
    private Schema schema;
    private String collectionWrapperName;
    private Map<String, String> collectionWrapperMap;
    private List<String> jaxbElementClassNames = Collections.emptyList();
    private Map<String, Object> cProperties;
    private Map<String, Object> uProperties;
    
    private boolean skipJaxbChecks;
    private boolean singleJaxbContext;
    private Class[] extraClass;
    
    private boolean validateOutput;
    private boolean validateBeforeWrite;
    private ValidationEventHandler eventHandler;
    private DocumentDepthProperties depthProperties;
    
    public void setValidationHandler(ValidationEventHandler handler) {
        eventHandler = handler;
    }
    
    public void setSingleJaxbContext(boolean useSingleContext) {
        singleJaxbContext = useSingleContext;
    }
    
    public void setExtraClass(Class[] userExtraClass) {
        extraClass = userExtraClass;
    }
    
    @Override
    public void init(List<ClassResourceInfo> cris) {
        if (singleJaxbContext) {
            Set<Class<?>> allTypes = 
                new HashSet<Class<?>>(ResourceUtils.getAllRequestResponseTypes(cris, true).keySet());
            JAXBContext context = 
                ResourceUtils.createJaxbContext(allTypes, extraClass, cProperties);
            if (context != null) {
                for (Class<?> cls : allTypes) {
                    classContexts.put(cls, context);
                }
            }
        }
    }
    
    public void setContextProperties(Map<String, Object> contextProperties) {
        cProperties = contextProperties;
    }
    
    public void setUnmarshallerProperties(Map<String, Object> unmarshalProperties) {
        uProperties = unmarshalProperties;
    }
    
    public void setUnmarshallAsJaxbElement(boolean value) {
        unmarshalAsJaxbElement = value;
    }
    
    public void setMarshallAsJaxbElement(boolean value) {
        marshalAsJaxbElement = value;
    }
    
    public void setJaxbElementClassNames(List<String> names) {
        jaxbElementClassNames = names;
    }
    
    public void setJaxbElementClassMap(Map<String, String> map) {
        jaxbElementClassMap = map;
    }
    
    protected boolean isPayloadEmpty() {
        return mc != null ? isPayloadEmpty(mc.getHttpHeaders()) : false;     
    }
    
    protected void reportEmptyContentLength() {
        String message = new org.apache.cxf.common.i18n.Message("EMPTY_BODY", BUNDLE).toString();
        LOG.warning(message);
        throw new WebApplicationException(400);
    }

    protected <T> T getStaxHandlerFromCurrentMessage(Class<T> staxCls) {
        Message m = PhaseInterceptorChain.getCurrentMessage();
        if (m != null) {
            return staxCls.cast(m.getContent(staxCls));
        }
        return null;
    }
    
    protected static boolean isXmlRoot(Class<?> cls) {
        return cls.getAnnotation(XmlRootElement.class) != null;
    }
    
    @SuppressWarnings("unchecked")
    protected Object convertToJaxbElementIfNeeded(Object obj, Class<?> cls, Type genericType) 
        throws Exception {
        
        boolean asJaxbElement = jaxbElementClassNames.contains(cls.getName());
        if (!asJaxbElement && isXmlRoot(cls)) {
            return obj;
        }
        
        QName name = null;
        String expandedName = jaxbElementClassMap.get(cls.getName());
        if (expandedName != null) {
            name = JAXRSUtils.convertStringToQName(expandedName);
        } else if (marshalAsJaxbElement || asJaxbElement) {
            name = getJaxbQName(cls, genericType, obj, false);
        }
        return name != null ? new JAXBElement(name, cls, null, obj) : obj;
    }
    
    public void setCollectionWrapperName(String wName) {
        collectionWrapperName = wName;
    }
    
    public void setCollectionWrapperMap(Map<String, String> map) {
        collectionWrapperMap = map;
    }
    
    protected void setContext(MessageContext context) {
        mc = context;
    }
    
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] anns, MediaType mt) {
        
        if (InjectionUtils.isSupportedCollectionOrArray(type)) {
            type = InjectionUtils.getActualType(genericType);
            if (type == null) {
                return false;
            }
        }
        
        return marshalAsJaxbElement || isSupported(type, genericType, anns);
    }
    
    protected JAXBContext getCollectionContext(Class<?> type) throws JAXBException {
        synchronized (collectionContextClasses) {
            if (!collectionContextClasses.contains(type)) {
                collectionContextClasses.add(CollectionWrapper.class);
                collectionContextClasses.add(type);
            }
            collectionContext = JAXBContext.newInstance(collectionContextClasses.toArray(new Class[]{}), 
                                                        cProperties);
            return collectionContext;
        }
    }
    
    protected QName getCollectionWrapperQName(Class<?> cls, Type type, Object object, boolean pluralName)
        throws Exception {
        String name = getCollectionWrapperName(cls);
        if (name == null) {
            return getJaxbQName(cls, type, object, pluralName);
        }
            
        return JAXRSUtils.convertStringToQName(name);
    }
    
    private String getCollectionWrapperName(Class<?> cls) {
        if (collectionWrapperName != null) { 
            return collectionWrapperName;
        }
        if (collectionWrapperMap != null) {
            return collectionWrapperMap.get(cls.getName());
        }
        
        return null;
    }
    
    protected QName getJaxbQName(Class<?> cls, Type type, Object object, boolean pluralName) 
        throws Exception {
        
        if (cls == JAXBElement.class) {
            return object != null ? ((JAXBElement)object).getName() : null;
        }
        
        XmlRootElement root = cls.getAnnotation(XmlRootElement.class);
        if (root != null) {
            return getQNameFromNamespaceAndName(root.namespace(), root.name(), cls, pluralName);
        } else if (cls.getAnnotation(XmlType.class) != null) {
            XmlType xmlType = cls.getAnnotation(XmlType.class);
            return getQNameFromNamespaceAndName(xmlType.namespace(), xmlType.name(), cls, pluralName);
        } else {
            return new QName(getPackageNamespace(cls), cls.getSimpleName());
        }
    }
    
    private QName getQNameFromNamespaceAndName(String ns, String localName, Class<?> cls, boolean plural) {
        String name = getLocalName(localName, cls.getSimpleName() , plural);
        String namespace = getNamespace(ns);
        if ("".equals(namespace)) {
            namespace = getPackageNamespace(cls);
        }
        return new QName(namespace, name);
    }
    
    private String getLocalName(String name, String clsName, boolean pluralName) {
        if (JAXB_DEFAULT_NAME.equals(name)) {
            name = clsName;
            if (name.length() > 1) {
                name = name.substring(0, 1).toLowerCase() + name.substring(1); 
            } else {
                name = name.toLowerCase();
            }
        }
        if (pluralName) {
            name += 's';
        }
        return name;
    }
    
    private String getPackageNamespace(Class<?> cls) {
        String packageNs = JAXBUtils.getPackageNamespace(cls);
        return packageNs != null ? getNamespace(packageNs) : "";
    }
    
    private String getNamespace(String namespace) {
        if (JAXB_DEFAULT_NAMESPACE.equals(namespace)) {
            return "";
        }
        return namespace;
    }
    
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] anns, MediaType mt) {
        if (InjectionUtils.isSupportedCollectionOrArray(type)) {
            type = InjectionUtils.getActualType(genericType);
            if (type == null) {
                return false;
            }
        }    
        return canBeReadAsJaxbElement(type) || isSupported(type, genericType, anns);
    }

    protected boolean canBeReadAsJaxbElement(Class<?> type) {
        return unmarshalAsJaxbElement && type != Response.class;
    }
    
    public void setSchemaLocations(List<String> locations) {
        schema = SchemaHandler.createSchema(locations, getBus());    
    }
    
    public void setSchema(Schema s) {
        schema = s;    
    }
    
    public long getSize(Object o, Class<?> type, Type genericType, Annotation[] annotations, MediaType mt) {
        return -1;
    }

    protected MessageContext getContext() {
        return mc;
    }
    
    @SuppressWarnings("unchecked")
    protected JAXBContext getJAXBContext(Class<?> type, Type genericType) throws JAXBException {
        if (mc != null) {
            ContextResolver<JAXBContext> resolver = 
                mc.getResolver(ContextResolver.class, JAXBContext.class);
            if (resolver != null) {
                JAXBContext customContext = resolver.getContext(type);
                if (customContext != null) {
                    return customContext;
                }
            }
        }
        
        synchronized (classContexts) {
            JAXBContext context = classContexts.get(type);
            if (context != null) {
                return context;
            }
        }
        
        JAXBContext context = getPackageContext(type);
                
        return context != null ? context : getClassContext(type);
    }
    
    public JAXBContext getClassContext(Class<?> type) throws JAXBException {
        synchronized (classContexts) {
            JAXBContext context = classContexts.get(type);
            if (context == null) {
                Class[] classes = null;
                if (extraClass != null) {
                    classes = new Class[extraClass.length + 1];
                    classes[0] = type;
                    System.arraycopy(extraClass, 0, classes, 1, extraClass.length);
                } else {
                    classes = new Class[] {type};    
                }
                
                context = JAXBContext.newInstance(classes, cProperties);
                classContexts.put(type, context);
            }
            return context;
        }
    }
    
    public JAXBContext getPackageContext(Class<?> type) {
        if (type == null || type == JAXBElement.class) {
            return null;
        }
        synchronized (packageContexts) {
            String packageName = PackageUtils.getPackageName(type);
            JAXBContext context = packageContexts.get(packageName);
            if (context == null) {
                try {
                    if (type.getClassLoader() != null && objectFactoryOrIndexAvailable(type)) { 
                        context = JAXBContext.newInstance(packageName, type.getClassLoader(), cProperties);
                        packageContexts.put(packageName, context);
                    }
                } catch (JAXBException ex) {
                    LOG.fine("Error creating a JAXBContext using ObjectFactory : " 
                                + ex.getMessage());
                    return null;
                }
            }
            return context;
        }
    }
    
    protected boolean isSupported(Class<?> type, Type genericType, Annotation[] anns) {
        if (jaxbElementClassMap != null && jaxbElementClassMap.containsKey(type.getName())
            || isSkipJaxbChecks()) {
            return true;
        }
        return isXmlRoot(type)
            || JAXBElement.class.isAssignableFrom(type)
            || objectFactoryOrIndexAvailable(type)
            || (type != genericType && objectFactoryForType(genericType))
            || org.apache.cxf.jaxrs.utils.JAXBUtils.getAdapter(type, anns) != null;
    
    }
    
    protected boolean objectFactoryOrIndexAvailable(Class<?> type) {
        return type.getResource("ObjectFactory.class") != null
               || type.getResource("jaxb.index") != null; 
    }
    
    private boolean objectFactoryForType(Type genericType) {
        return objectFactoryOrIndexAvailable(InjectionUtils.getActualType(genericType));
    }
    
    protected Unmarshaller createUnmarshaller(Class<?> cls, Type genericType) 
        throws JAXBException {
        return createUnmarshaller(cls, genericType, false);        
    }
    
    protected Unmarshaller createUnmarshaller(Class<?> cls, Type genericType, boolean isCollection) 
        throws JAXBException {
        JAXBContext context = isCollection ? getCollectionContext(cls) 
                                           : getJAXBContext(cls, genericType);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        if (schema != null) {
            unmarshaller.setSchema(schema);
        }
        if (eventHandler != null) {
            unmarshaller.setEventHandler(eventHandler);
        }
        if (uProperties != null) {
            for (Map.Entry<String, Object> entry : uProperties.entrySet()) {
                unmarshaller.setProperty(entry.getKey(), entry.getValue());
            }
        }
        return unmarshaller;        
    }
    
    protected Marshaller createMarshaller(Object obj, Class<?> cls, Type genericType, String enc)
        throws JAXBException {
        
        Class<?> objClazz = JAXBElement.class.isAssignableFrom(cls) 
                            ? ((JAXBElement)obj).getDeclaredType() : cls;
                            
        JAXBContext context = getJAXBContext(objClazz, genericType);
        Marshaller marshaller = context.createMarshaller();
        if (enc != null) {
            marshaller.setProperty(Marshaller.JAXB_ENCODING, enc);
        }
        validateObjectIfNeeded(marshaller, obj);
        return marshaller;
    }
    
    protected void validateObjectIfNeeded(Marshaller marshaller, Object obj) 
        throws JAXBException {
        if (validateOutput && schema != null) {
            marshaller.setEventHandler(eventHandler);
            marshaller.setSchema(schema);
            if (validateBeforeWrite) {
                marshaller.marshal(obj, new DefaultHandler());
                marshaller.setSchema(null);
            }
        }
    }
        
    protected Class<?> getActualType(Class<?> type, Type genericType, Annotation[] anns) {
        Class<?> theType = null;
        if (JAXBElement.class.isAssignableFrom(type)) {
            theType = InjectionUtils.getActualType(genericType);
        } else {
            theType = type;
        }
        XmlJavaTypeAdapter adapter = org.apache.cxf.jaxrs.utils.JAXBUtils.getAdapter(theType, anns);
        theType = org.apache.cxf.jaxrs.utils.JAXBUtils.getTypeFromAdapter(adapter, theType, false);
        
        return theType;
    }
    
    protected static Object checkAdapter(Object obj, Class<?> cls, Annotation[] anns, boolean marshal) {
        XmlJavaTypeAdapter adapter = org.apache.cxf.jaxrs.utils.JAXBUtils.getAdapter(cls, anns);
        return org.apache.cxf.jaxrs.utils.JAXBUtils.useAdapter(obj, adapter, marshal); 
    }
    
    protected Schema getSchema() {
        return schema;
    }

    
    public static void clearContexts() {
        classContexts.clear();
        packageContexts.clear();
    }
    
    protected static String getStackTrace(Exception ex) { 
        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
    
    protected static void handleJAXBException(JAXBException e, boolean read) {
        LOG.warning(getStackTrace(e));
        StringBuilder sb = new StringBuilder();
        if (e.getMessage() != null) {
            sb.append(e.getMessage()).append(". ");
        }
        if (e.getCause() != null && e.getCause().getMessage() != null) {
            sb.append(e.getCause().getMessage()).append(". ");
        }
        if (e.getLinkedException() != null && e.getLinkedException().getMessage() != null) {
            sb.append(e.getLinkedException().getMessage()).append(". ");
        }
        Throwable t = e.getLinkedException() != null 
            ? e.getLinkedException() : e.getCause() != null ? e.getCause() : e;
        String message = new org.apache.cxf.common.i18n.Message("JAXB_EXCEPTION", 
                             BUNDLE, sb.toString()).toString();
        Response.Status status = read 
            ? Response.Status.BAD_REQUEST : Response.Status.INTERNAL_SERVER_ERROR; 
        Response r = Response.status(status)
            .type(MediaType.TEXT_PLAIN).entity(message).build();
        throw new WebApplicationException(t, r);
    }
    
    public void setOutTransformElements(Map<String, String> outElements) {
        this.outElementsMap = outElements;
    }
    
    public void setInAppendElements(Map<String, String> inElements) {
        this.inAppendMap = inElements;
    }
    
    public void setInTransformElements(Map<String, String> inElements) {
        this.inElementsMap = inElements;
    }
    
    public void setOutAppendElements(Map<String, String> map) {
        this.outAppendMap = map;
    }

    public void setOutDropElements(List<String> dropElementsSet) {
        this.outDropElements = dropElementsSet;
    }

    public void setInDropElements(List<String> dropElementsSet) {
        this.inDropElements = dropElementsSet;
    }
    
    protected static Set<QName> convertToSetOfQNames(List<String> dropEls) {
        Set<QName> dropElements = Collections.emptySet();
        if (dropEls != null) {
            dropElements = new LinkedHashSet<QName>(dropEls.size());
            for (String val : dropEls) {
                dropElements.add(JAXRSUtils.convertStringToQName(val));
            }
        }
        return dropElements;
    }
    
    protected XMLStreamReader createTransformReaderIfNeeded(XMLStreamReader reader, InputStream is) {
        if (inDropElements != null) {
            Set<QName> dropElements = convertToSetOfQNames(inDropElements);
            reader = StaxUtils.createFilteredReader(createNewReaderIfNeeded(reader, is),
                                               new StaxStreamFilter(dropElements.toArray(new QName[]{})));    
        }
        if (inElementsMap != null || inAppendMap != null) {
            reader = new InTransformReader(createNewReaderIfNeeded(reader, is),
                                           inElementsMap, inAppendMap);
        }
        return reader;
    }

    protected XMLStreamWriter createTransformWriterIfNeeded(XMLStreamWriter writer,
                                                            OutputStream os) {
        if (outElementsMap != null || outDropElements != null 
            || outAppendMap != null || attributesToElements) {
            writer = createNewWriterIfNeeded(writer, os);
            writer = new OutTransformWriter(writer, outElementsMap, outAppendMap,
                                            outDropElements, attributesToElements);
        }
        return writer;
    }
    
    protected XMLStreamReader createNewReaderIfNeeded(XMLStreamReader reader, InputStream is) {
        return reader == null ? StaxUtils.createXMLStreamReader(is) : reader;
    }
    
    protected XMLStreamWriter createNewWriterIfNeeded(XMLStreamWriter writer, OutputStream os) {
        return writer == null ? StaxUtils.createXMLStreamWriter(os) : writer;
    }
    
    protected static void convertToQNamesMap(Map<String, String> map,
                                             QNamesMap elementsMap,
                                             Map<String, String> nsMap) {
        if (map != null) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                QName lname = JAXRSUtils.convertStringToQName(entry.getKey());
                QName rname = JAXRSUtils.convertStringToQName(entry.getValue());
                elementsMap.put(lname, rname);
                if (nsMap != null) {
                    nsMap.put(lname.getNamespaceURI(), rname.getNamespaceURI());
                }
            }
        }
    }
    
    protected static void convertToMapOfQNames(Map<String, String> map,
                                               Map<QName, QName> elementsMap) {
        if (map != null) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                QName lname = JAXRSUtils.convertStringToQName(entry.getKey());
                QName rname = JAXRSUtils.convertStringToQName(entry.getValue());
                elementsMap.put(lname, rname);
            }
        }
    }
    
    
    public void setAttributesToElements(boolean value) {
        this.attributesToElements = value;
    }

    public void setSkipJaxbChecks(boolean skipJaxbChecks) {
        this.skipJaxbChecks = skipJaxbChecks;
    }

    public boolean isSkipJaxbChecks() {
        return skipJaxbChecks;
    }

    protected XMLStreamReader createDepthReaderIfNeeded(XMLStreamReader reader, InputStream is) {
        DocumentDepthProperties props = getDepthProperties();
        if (props != null && props.isEffective()) {
            reader = createNewReaderIfNeeded(reader, is);
            return new DepthRestrictingStreamReader(reader, props);
        }
        return reader;
    }
    
    protected DocumentDepthProperties getDepthProperties() {
        if (depthProperties != null) {
            return depthProperties;
        }
        if (getContext() != null) {
            String totalElementCountStr = (String)getContext().getContextualProperty(
                DocumentDepthProperties.TOTAL_ELEMENT_COUNT);
            String innerElementCountStr = (String)getContext().getContextualProperty(
                DocumentDepthProperties.INNER_ELEMENT_COUNT);
            String elementLevelStr = (String)getContext().getContextualProperty(
                DocumentDepthProperties.INNER_ELEMENT_LEVEL);
            if (totalElementCountStr != null || innerElementCountStr != null || elementLevelStr != null) {
                try {
                    int totalElementCount = totalElementCountStr != null 
                        ? Integer.valueOf(totalElementCountStr) : -1;
                    int elementLevel = elementLevelStr != null ? Integer.valueOf(elementLevelStr) : -1;
                    int innerElementCount = innerElementCountStr != null 
                        ? Integer.valueOf(innerElementCountStr) : -1;
                    return new DocumentDepthProperties(totalElementCount, elementLevel, innerElementCount);
                } catch (Exception ex) {
                    throw new WebApplicationException(ex);
                }
            }
        }
        return null;
    }
    
    public void setValidateBeforeWrite(boolean validateBeforeWrite) {
        this.validateBeforeWrite = validateBeforeWrite;
    }

    public void setValidateOutput(boolean validateOutput) {
        this.validateOutput = validateOutput;
    }

    public void setDepthProperties(DocumentDepthProperties depthProperties) {
        this.depthProperties = depthProperties;
    }

    @XmlRootElement
    protected static class CollectionWrapper {
        
        @XmlAnyElement(lax = true)
        private List<?> l;
        
        public void setList(List<?> list) {
            l = list;
        }
        
        public List<?> getList() {
            if (l == null) {
                l = new ArrayList<Object>();
            }
            return l;
        }
        
        @SuppressWarnings("unchecked")
        public <T> Object getCollectionOrArray(Class<T> type, Class<?> origType,
                                               XmlJavaTypeAdapter adapter) {
            List<?> theList = getList();
            boolean adapterChecked = false;
            if (theList.size() > 0) {
                Object first = theList.get(0);
                if (first instanceof JAXBElement && !JAXBElement.class.isAssignableFrom(type)) {
                    adapterChecked = true;
                    List<Object> newList = new ArrayList<Object>(theList.size());
                    for (Object o : theList) {
                        newList.add(org.apache.cxf.jaxrs.utils.JAXBUtils.useAdapter(
                                        ((JAXBElement)o).getValue(), adapter, false));
                    }
                    theList = newList;
                }
            }
            if (origType.isArray()) {
                T[] values = (T[])Array.newInstance(type, theList.size());
                for (int i = 0; i < theList.size(); i++) {
                    values[i] = (T)org.apache.cxf.jaxrs.utils.JAXBUtils.useAdapter(
                                       theList.get(i), adapter, false);
                }
                return values;
            } else {
                if (!adapterChecked && adapter != null) {
                    List<Object> newList = new ArrayList<Object>(theList.size());
                    for (Object o : theList) {
                        newList.add(org.apache.cxf.jaxrs.utils.JAXBUtils.useAdapter(o, adapter, false));
                    }
                    theList = newList;
                }
                if (origType == Set.class) {
                    return new HashSet(theList);
                } else {
                    return theList;
                }
            }
        }
        
    }
    
    protected static class OutTransformWriter extends DelegatingXMLStreamWriter {
        private QNamesMap elementsMap;
        private Map<QName, QName> appendMap = new HashMap<QName, QName>(5);
        private Map<String, String> nsMap = new HashMap<String, String>(5);
        private Set<String> prefixes = new HashSet<String>(2);
        private Set<String> writtenUris = new HashSet<String>(2);
        
        private Set<QName> dropElements;
        private List<Integer> droppingIndexes = new LinkedList<Integer>();
        private List<QName> appendedElements = new LinkedList<QName>();
        private List<Integer> appendedIndexes = new LinkedList<Integer>();
        private int currentDepth;
        private boolean attributesToElements;
        
        public OutTransformWriter(XMLStreamWriter writer, 
                                  Map<String, String> outMap,
                                  Map<String, String> append,
                                  List<String> dropEls,
                                  boolean attributesToElements) {
            super(writer);
            elementsMap = new QNamesMap(outMap == null ? 0 : outMap.size());
            convertToQNamesMap(outMap, elementsMap, nsMap);
            convertToMapOfQNames(append, appendMap);
            dropElements = convertToSetOfQNames(dropEls);
            this.attributesToElements = attributesToElements;
        }

        @Override
        public void writeNamespace(String prefix, String uri) throws XMLStreamException {
            if (matchesDropped()) {
                return;
            }
            if (writtenUris.contains(uri)) {
                return;
            }
            String value = nsMap.get(uri);
            if (value != null && value.length() == 0) {
                return;
            }
            super.writeNamespace(prefix, value != null ? value : uri);
        }
        
        @Override
        public void writeStartElement(String prefix, String local, String uri) throws XMLStreamException {
            currentDepth++;
            QName currentQName = new QName(uri, local);
            
            QName appendQName = appendMap.get(currentQName);
            if (appendQName != null && !appendedElements.contains(appendQName)) {
                currentDepth++;
                String theprefix = uri.equals(appendQName.getNamespaceURI()) ? prefix : "";
                write(new QName(appendQName.getNamespaceURI(), appendQName.getLocalPart(), theprefix));
                if (theprefix.length() > 0) {
                    super.writeNamespace(theprefix, uri);
                    writtenUris.add(uri);
                }
                appendedElements.add(appendQName);
                appendedIndexes.add(currentDepth - 1);
            }
            
            if (dropElements.contains(currentQName)) {
                droppingIndexes.add(currentDepth - 1);
                return;
            }
            write(new QName(uri, local, prefix));
        }
        
        @Override
        public void writeEndElement() throws XMLStreamException {
            --currentDepth;
            if (indexRemoved(droppingIndexes)) {
                return;
            }
            super.writeEndElement();
            if (indexRemoved(appendedIndexes)) {
                super.writeEndElement();
            }
        }
        
        @Override
        public void writeCharacters(String text) throws XMLStreamException {
            if (matchesDropped()) {
                return;
            }
            super.writeCharacters(text);
        }
        
        private void write(QName qname) throws XMLStreamException {
            QName name = elementsMap.get(qname);
            if (name == null) {
                name = qname;
            }
            boolean writeNs = false;
            String prefix = "";
            if (name.getNamespaceURI().length() > 0) {
                if (qname.getPrefix().length() == 0) {
                    prefix = findUniquePrefix();
                    writeNs = true;
                } else {
                    prefix = qname.getPrefix();
                    prefixes.add(prefix);
                }
                prefixes.add(prefix);
            }
            super.writeStartElement(prefix, name.getLocalPart(), name.getNamespaceURI());
            if (writeNs) {
                this.writeNamespace(prefix, name.getNamespaceURI());
            }
        }
        
        private String findUniquePrefix() {
            
            int i = 0;
            while (true) {
                if (!prefixes.contains("ps" + ++i)) {
                    return "ps" + i;
                }
            }
        }
        
        private boolean matchesDropped() {
            int size = droppingIndexes.size();
            if (size > 0 && droppingIndexes.get(size - 1) == currentDepth - 1) {
                return true;
            }
            return false;
        }
        
        private boolean indexRemoved(List<Integer> indexes) {
            int size = indexes.size();
            if (size > 0 && indexes.get(size - 1) == currentDepth) {
                indexes.remove(size - 1);
                return true;
            }
            return false;
        }
        
        @Override
        public NamespaceContext getNamespaceContext() {
            return new DelegatingNamespaceContext(super.getNamespaceContext(), nsMap);
        }
        
        @Override
        public void writeAttribute(String uri, String local, String value) throws XMLStreamException {
            if (!attributesToElements) {
                super.writeAttribute(uri, local, value);
            } else {
                writeAttributeAsElement(uri, local, value);
            }
        }

        @Override
        public void writeAttribute(String local, String value) throws XMLStreamException {
            if (!attributesToElements) {
                super.writeAttribute(local, value);
            } else {
                writeAttributeAsElement("", local, value);
            }
        }
        
        private void writeAttributeAsElement(String uri, String local, String value)
            throws XMLStreamException {
            this.writeStartElement(uri, local);
            this.writeCharacters(value);
            this.writeEndElement();
        }
    } 
    protected static class JAXBCollectionWrapperReader extends DepthXMLStreamReader {
        
        private boolean firstName;
        private boolean firstNs;
        
        public JAXBCollectionWrapperReader(XMLStreamReader reader) {
            super(reader);
        }
        
        @Override
        public String getNamespaceURI() {
            if (!firstNs) {
                firstNs = true;
                return "";
            }
            return super.getNamespaceURI();
        }
        
        @Override
        public String getLocalName() {
            if (!firstName) {
                firstName = true;
                return "collectionWrapper";
            }
            
            return super.getLocalName();
        }
    }
    
    private static class QNamesMap {
        private QName[] keys;
        private QName[] values;
        private int index;
        
        public QNamesMap(int size) {
            keys = new QName[size];
            values = new QName[size];
        }
        
        public void put(QName key, QName value) {
            keys[index] = key;
            values[index] = value;
            index++;
        }
        
        public QName get(QName key) {
            for (int i = 0; i < keys.length; i++) {
                if (keys[i].getNamespaceURI().equals(key.getNamespaceURI())) {
                    if (keys[i].getLocalPart().equals(key.getLocalPart())) {
                        return values[i];
                    } else if ("*".equals(keys[i].getLocalPart())) {
                        // assume it is something like {somens}* : *
                        return "*".equals(values[i]) ? new QName(key.getLocalPart()) 
                            : new QName(values[i].getNamespaceURI(), key.getLocalPart());
                    }
                }
            }
            return null;    
        }
    }
    
    protected static class InTransformReader extends DepthXMLStreamReader {
        
        private static final String INTERN_NAMES = "org.codehaus.stax2.internNames";
        private static final String INTERN_NS = "org.codehaus.stax2.internNsUris";
        
        private QNamesMap inElementsMap;
        private Map<QName, QName> inAppendMap = new HashMap<QName, QName>(5);
        private Map<String, String> nsMap = new HashMap<String, String>(5);
        private QName currentQName;
        private QName previousQName;
        private int previousDepth = -1;
        
        public InTransformReader(XMLStreamReader reader, 
                                 Map<String, String> inMap,
                                 Map<String, String> appendMap) {
            super(reader);
            inElementsMap = new QNamesMap(inMap == null ? 0 : inMap.size());
            convertToQNamesMap(inMap, inElementsMap, nsMap);
            convertToMapOfQNames(appendMap, inAppendMap);
        }
        
        public int next() throws XMLStreamException {
            if (currentQName != null) {
                return XMLStreamConstants.START_ELEMENT;
            } else if (previousDepth != -1 && previousDepth == getDepth() + 1) {
                previousDepth = -1;
                return XMLStreamConstants.END_ELEMENT;
            } else {
                return super.next();
            }
        }
        
        public Object getProperty(String name) throws IllegalArgumentException {

            if (INTERN_NAMES.equals(name) || INTERN_NS.equals(name)) {
                return Boolean.FALSE;
            }
            return super.getProperty(name);
        }

        public String getLocalName() {
            QName cQName = getCurrentName();
            if (cQName != null) {
                String name = cQName.getLocalPart();
                resetCurrentQName();
                return name;
            }
            return super.getLocalName();
        }

        private QName getCurrentName() {
            return currentQName != null ? currentQName 
                : previousQName != null ? previousQName : null;
        }
        
        private void resetCurrentQName() {
            currentQName = previousQName;
            previousQName = null;
        }
        
        public NamespaceContext getNamespaceContext() {
            return new DelegatingNamespaceContext(super.getNamespaceContext(), nsMap);
        }

        public String getNamespaceURI() {
         
            QName theName = readCurrentElement();
            QName appendQName = inAppendMap.remove(theName);
            if (appendQName != null) {
                previousDepth = getDepth();
                previousQName = theName;
                currentQName = appendQName;
                return currentQName.getNamespaceURI();
            }
            QName expected = inElementsMap.get(theName);
            if (expected == null) {
                return theName.getNamespaceURI();
            }
            currentQName = expected;
            return currentQName.getNamespaceURI();
        }
        
        private QName readCurrentElement() {
            if (currentQName != null) {
                return currentQName;
            }
            String ns = super.getNamespaceURI();
            String name = super.getLocalName();
            return new QName(ns, name);
        }
    }

    private static class DelegatingNamespaceContext implements NamespaceContext {

        private NamespaceContext nc;
        private Map<String, String> nsMap;
        
        public DelegatingNamespaceContext(NamespaceContext nc, Map<String, String> nsMap) {
            this.nc = nc;
            this.nsMap = nsMap;
        }
        
        public String getNamespaceURI(String prefix) {
            return nc.getNamespaceURI(prefix);
        }

        public String getPrefix(String ns) {
            String value = nsMap.get(ns);
            if (value != null && value.length() == 0) {
                return null;
            }
            return value != null ? nc.getPrefix(value) : nc.getPrefix(ns);
        }

        public Iterator getPrefixes(String ns) {
            return nc.getPrefixes(ns);
        }
        
    }
}
