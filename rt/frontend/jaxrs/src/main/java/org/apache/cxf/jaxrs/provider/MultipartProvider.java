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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.logging.Logger;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.mail.internet.MimeUtility;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import org.apache.cxf.attachment.AttachmentUtil;
import org.apache.cxf.attachment.ByteDataSource;
import org.apache.cxf.common.i18n.BundleUtils;
import org.apache.cxf.common.logging.LogUtils;
import org.apache.cxf.jaxrs.ext.MessageContext;
import org.apache.cxf.jaxrs.ext.form.Form;
import org.apache.cxf.jaxrs.ext.multipart.Attachment;
import org.apache.cxf.jaxrs.ext.multipart.ContentDisposition;
import org.apache.cxf.jaxrs.ext.multipart.InputStreamDataSource;
import org.apache.cxf.jaxrs.ext.multipart.Multipart;
import org.apache.cxf.jaxrs.ext.multipart.MultipartBody;
import org.apache.cxf.jaxrs.impl.MetadataMap;
import org.apache.cxf.jaxrs.utils.AnnotationUtils;
import org.apache.cxf.jaxrs.utils.InjectionUtils;
import org.apache.cxf.jaxrs.utils.JAXRSUtils;
import org.apache.cxf.jaxrs.utils.multipart.AttachmentUtils;
import org.apache.cxf.message.Message;
import org.apache.cxf.message.MessageUtils;

@Provider
@Consumes({"multipart/related", "multipart/mixed", "multipart/alternative", "multipart/form-data" })
@Produces({"multipart/related", "multipart/mixed", "multipart/alternative", "multipart/form-data" })
public class MultipartProvider extends AbstractConfigurableProvider
    implements MessageBodyReader<Object>, MessageBodyWriter<Object> {
    
    private static final String ACTIVE_JAXRS_PROVIDER_KEY = "active.jaxrs.provider";
    private static final Logger LOG = LogUtils.getL7dLogger(MultipartProvider.class);
    private static final ResourceBundle BUNDLE = BundleUtils.getBundle(MultipartProvider.class);

    @Context
    private MessageContext mc;
    private String attachmentDir;
    private String attachmentThreshold;
    private String attachmentMaxSize;

    public void setMessageContext(MessageContext context) {
        this.mc = context;
    }
    
    public void setAttachmentDirectory(String dir) {
        attachmentDir = dir;
    }
    
    public void setAttachmentThreshold(String threshold) {
        attachmentThreshold = threshold;
    }
    
    public void setAttachmentMaxSize(String maxSize) {
        attachmentMaxSize = maxSize;
    }

    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, 
                              MediaType mt) {
        return isSupported(type, genericType, annotations, mt);
    }
    
    private boolean isSupported(Class<?> type, Type genericType, Annotation[] annotations, 
                                MediaType mt) {
        if (DataHandler.class.isAssignableFrom(type) || DataSource.class.isAssignableFrom(type)
            || Attachment.class.isAssignableFrom(type) || MultipartBody.class.isAssignableFrom(type)
            || mediaTypeSupported(mt)
            || isSupportedFormDataType(type, mt)) {
            return true;
        }
        return false;
    }

    private boolean isSupportedFormDataType(Class<?> type, MediaType mt) {
        return mt.getType().equals("multipart") && mt.getSubtype().equals("form-data") 
            && !MultivaluedMap.class.isAssignableFrom(type) && !Form.class.isAssignableFrom(type);
    }
    
    protected void checkContentLength() {
        if (mc != null && isPayloadEmpty(mc.getHttpHeaders())) {
            String message = new org.apache.cxf.common.i18n.Message("EMPTY_BODY", BUNDLE).toString();
            LOG.warning(message);
            throw new WebApplicationException(400);
        }
    }
    
    public Object readFrom(Class<Object> c, Type t, Annotation[] anns, MediaType mt, 
                           MultivaluedMap<String, String> headers, InputStream is) 
        throws IOException, WebApplicationException {
        checkContentLength();
        List<Attachment> infos = AttachmentUtils.getAttachments(
                mc, attachmentDir, attachmentThreshold, attachmentMaxSize);
        
        boolean collectionExpected = Collection.class.isAssignableFrom(c);
        if (collectionExpected
            && AnnotationUtils.getAnnotation(anns, Multipart.class) == null) {
            return getAttachmentCollection(t, infos, anns);
        }
        if (c.isAssignableFrom(Map.class)) {
            Map<String, Object> map = new LinkedHashMap<String, Object>(infos.size());
            Class<?> actual = getActualType(t, 1);
            for (Attachment a : infos) {
                map.put(a.getContentType().toString(), fromAttachment(a, actual, actual, anns));
            }
            return map;
        }
        if (MultipartBody.class.isAssignableFrom(c)) {
            return new MultipartBody(infos);
        }
        
        Multipart id = AnnotationUtils.getAnnotation(anns, Multipart.class);
        Attachment multipart = AttachmentUtils.getMultipart(id, mt, infos);
        if (multipart != null) {
            if (collectionExpected && !mediaTypeSupported(multipart.getContentType())) {
                List<Attachment> allMultiparts = AttachmentUtils.getAllMultiparts(id, mt, infos);
                return getAttachmentCollection(t, allMultiparts, anns);
            } else {
                return fromAttachment(multipart, c, t, anns);
            }
        } 
        
        if (id != null && !id.required()) {
            /*
             * If user asked for a null, give them a null. 
             */
            return null;
        }
        
        throw new WebApplicationException(400);
        
    }
    
    private Object getAttachmentCollection(Type t, List<Attachment> infos, Annotation[] anns) throws IOException {
        Class<?> actual = getActualType(t, 0);
        if (Attachment.class.isAssignableFrom(actual)) {
            return infos;
        }
        Collection<Object> objects = new ArrayList<Object>();
        for (Attachment a : infos) {
            objects.add(fromAttachment(a, actual, actual, anns));
        }
        return objects;
    }
    
    private Class<?> getActualType(Type type, int pos) {
        Class<?> actual = null;
        try {
            actual = InjectionUtils.getActualType(type, pos);
        } catch (Exception ex) {
            // ignore;
        }
        return actual != null && actual != Object.class ? actual : Attachment.class;
    }
    
    @SuppressWarnings("unchecked")
    private Object fromAttachment(Attachment multipart, Class<?> c, Type t, Annotation anns[]) 
        throws IOException {
        if (DataHandler.class.isAssignableFrom(c)) {
            return multipart.getDataHandler();
        } else if (DataSource.class.isAssignableFrom(c)) {
            return multipart.getDataHandler().getDataSource();
        } else if (Attachment.class.isAssignableFrom(c)) {
            return multipart;
        } else {
            if (mediaTypeSupported(multipart.getContentType())) {
                mc.put("org.apache.cxf.multipart.embedded", true);
                mc.put("org.apache.cxf.multipart.embedded.ctype", multipart.getContentType());
                mc.put("org.apache.cxf.multipart.embedded.input", 
                       multipart.getDataHandler().getInputStream());
                anns = new Annotation[]{};
            }
            MessageBodyReader<Object> r = 
                mc.getProviders().getMessageBodyReader((Class)c, t, anns, multipart.getContentType());
            if (r != null) {
                InputStream is = multipart.getDataHandler().getInputStream();
                is = decodeIfNeeded(multipart, is);
                return r.readFrom((Class)c, t, anns, multipart.getContentType(), multipart.getHeaders(), is);
            }
        }
        return null;
    }
    
    
    private InputStream decodeIfNeeded(Attachment multipart, InputStream is) {
        String value = multipart.getHeader("Content-Transfer-Encoding");
        if ("base64".equals(value) || "quoted-printable".equals(value)) {
            try {
                is = MimeUtility.decode(is, value);
            } catch (Exception ex) {
                LOG.warning("Problem with decoding an input stream, encoding : " + value);
            }
        }
        return is;
    }
    
    private boolean mediaTypeSupported(MediaType mt) {
        return mt.getType().equals("multipart") && (mt.getSubtype().equals("related") 
            || mt.getSubtype().equals("mixed") || mt.getSubtype().equals("alternative"));
    }

    public long getSize(Object t, Class<?> type, Type genericType, Annotation[] annotations, 
                        MediaType mediaType) {
        return -1;
    }

    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations,
                               MediaType mt) {
        return isSupported(type, genericType, annotations, mt);
    }

    
    public void writeTo(Object obj, Class<?> type, Type genericType, Annotation[] anns, MediaType mt,
                        MultivaluedMap<String, Object> headers, OutputStream os) 
        throws IOException, WebApplicationException {
        
        List<Attachment> handlers = convertToDataHandlers(obj, type, genericType, anns, mt);
        mc.put(MultipartBody.OUTBOUND_MESSAGE_ATTACHMENTS, handlers);
        handlers.get(0).getDataHandler().writeTo(os);
    }
    
    @SuppressWarnings("unchecked")
    private List<Attachment> convertToDataHandlers(Object obj,
                                                   Class<?> type, Type genericType,                          
                                                   Annotation[] anns, MediaType mt) {
        if (Map.class.isAssignableFrom(obj.getClass())) {
            Map<Object, Object> objects = (Map)obj;
            List<Attachment> handlers = new ArrayList<Attachment>(objects.size());
            int i = 0;
            for (Iterator<Map.Entry<Object, Object>> iter = objects.entrySet().iterator(); 
                iter.hasNext();) {
                Map.Entry entry = iter.next();
                Object value = entry.getValue();
                Attachment handler = createDataHandler(value, value.getClass(), value.getClass(), 
                                                       new Annotation[]{},
                                                       entry.getKey().toString(),
                                                       mt.toString(),
                                                       i++);
                handlers.add(handler);
            }
            return handlers;
        } else {
            String rootMediaType = getRootMediaType(anns, mt); 
            if (List.class.isAssignableFrom(obj.getClass())) {
                return getAttachments((List)obj, rootMediaType);
            } else {
                if (MultipartBody.class.isAssignableFrom(type)) {
                    List<Attachment> atts = ((MultipartBody)obj).getAllAttachments();
                    // these attachments may have no DataHandlers, but objects only
                    return getAttachments(atts, rootMediaType);
                }
                Attachment handler = createDataHandler(obj,
                                                       type, genericType, anns,
                                                       rootMediaType, mt.toString(), 1);
                return Collections.singletonList(handler);
            }
        }
    }
    
    private List<Attachment> getAttachments(List<?> objects, String rootMediaType) {
        List<Attachment> handlers = new ArrayList<Attachment>(objects.size());
        for (int i = 0; i < objects.size(); i++) {
            Object value = objects.get(i);
            Attachment handler = createDataHandler(value,
                                           value.getClass(), value.getClass(), new Annotation[]{},
                                           rootMediaType, rootMediaType, i);
            handlers.add(handler);
        }
        return handlers;
    }
    
    private Attachment createDataHandler(Object obj, 
                                         Class<?> cls, Type genericType,
                                         Annotation[] anns,
                                         String mimeType,
                                         String mainMediaType,
                                         int id) {
        DataHandler dh = null;
        if (InputStream.class.isAssignableFrom(obj.getClass())) {
            dh = createInputStreamDH((InputStream)obj, mimeType);
        } else if (DataHandler.class.isAssignableFrom(obj.getClass())) {
            dh = (DataHandler)obj;
        } else if (DataSource.class.isAssignableFrom(obj.getClass())) {
            dh = new DataHandler((DataSource)obj);
        } else if (File.class.isAssignableFrom(obj.getClass())) {
            File f = (File)obj;
            ContentDisposition cd = mainMediaType.startsWith(MediaType.MULTIPART_FORM_DATA) 
                ? new ContentDisposition("form-data;name=file;filename=" + f.getName()) :  null;
            try {
                return new Attachment(AttachmentUtil.BODY_ATTACHMENT_ID, new FileInputStream(f), cd);
            } catch (FileNotFoundException ex) {
                throw new WebApplicationException(ex);
            }
        } else if (Attachment.class.isAssignableFrom(obj.getClass())) {
            Attachment att = (Attachment)obj;
            if (att.getObject() == null) {
                return att;
            }
            dh = getHandlerForObject(att.getObject(), att.getObject().getClass(), 
                                     att.getObject().getClass(), new Annotation[]{}, 
                                     att.getContentType().toString(), id);
            return new Attachment(att.getContentId(), dh, att.getHeaders());
        } else if (byte[].class.isAssignableFrom(obj.getClass())) {
            ByteDataSource source = new ByteDataSource((byte[])obj);
            source.setContentType(mimeType);
            dh = new DataHandler(source);
        } else {
            dh = getHandlerForObject(obj, cls, genericType, anns, mimeType, id);
        }
        String contentId = getContentId(anns, id);
        
        return new Attachment(contentId, dh, new MetadataMap<String, String>());
    }

    private String getContentId(Annotation[] anns, int id) {
        Multipart part = AnnotationUtils.getAnnotation(anns, Multipart.class);
        if (part != null && !"".equals(part.value())) {
            return part.value();
        }
        return id == 0 ? AttachmentUtil.BODY_ATTACHMENT_ID : Integer.toString(id);
    }
    
    @SuppressWarnings("unchecked")
    private DataHandler getHandlerForObject(Object obj, 
                                            Class<?> cls, Type genericType,
                                            Annotation[] anns,
                                            String mimeType, int id) {
        MediaType mt = JAXRSUtils.toMediaType(mimeType);
        mc.put(ACTIVE_JAXRS_PROVIDER_KEY, this);
        
        MessageBodyWriter<Object> r = null;
        try {
            r = (MessageBodyWriter)mc.getProviders().getMessageBodyWriter(cls, genericType, anns, mt);
        } finally {
            mc.put("active.jaxrs.provider", null); 
        }
        if (r == null) {
            org.apache.cxf.common.i18n.Message message = 
                new org.apache.cxf.common.i18n.Message("NO_MSG_WRITER",
                                                   BUNDLE,
                                                   cls);
            LOG.severe(message.toString());
            throw new WebApplicationException(500);
        }
        return new MessageBodyWriterDataHandler(r, obj, cls, genericType, anns, mt);
    }
    
    private DataHandler createInputStreamDH(InputStream is, String mimeType) {
        return new DataHandler(new InputStreamDataSource(is, mimeType));
    }
    
    private String getRootMediaType(Annotation[] anns, MediaType mt) {
        String mimeType = mt.getParameters().get("type");
        if (mimeType != null) {
            return mimeType;
        }
        Multipart id = AnnotationUtils.getAnnotation(anns, Multipart.class);
        if (id != null && !MediaType.WILDCARD.equals(id.type())) {
            mimeType = id.type();
        }
        if (mimeType == null) {
            if (MessageUtils.isTrue(mc.getContextualProperty(Message.MTOM_ENABLED))) {
                mimeType = "text/xml";
            } else {
                mimeType = MediaType.APPLICATION_OCTET_STREAM;
            }
        }
        return mimeType;
    }
    
    private static class MessageBodyWriterDataHandler extends DataHandler {
        private MessageBodyWriter<Object> writer;
        private Object obj;
        private Class<?> cls;
        private Type genericType;
        private Annotation[] anns;
        private MediaType contentType;
        public MessageBodyWriterDataHandler(MessageBodyWriter<Object> writer,
                                            Object obj,
                                            Class<?> cls,
                                            Type genericType,
                                            Annotation[] anns,
                                            MediaType contentType) {
            super(new ByteDataSource("1".getBytes()));
            this.writer = writer;
            this.obj = obj;
            this.cls = cls;
            this.genericType = genericType;
            this.anns = anns;
            this.contentType = contentType;
        }
        
        @Override
        public void writeTo(OutputStream os) {
            try {
                writer.writeTo(obj, cls, genericType, anns, contentType, 
                               new MetadataMap<String, Object>(), os);
            } catch (IOException ex) {
                throw new WebApplicationException();
            }
        }
        
        @Override
        public String getContentType() {
            return contentType.toString();
        }
        
        // TODO : throw UnsupportedOperationException for all other DataHandler methods
    }
}
