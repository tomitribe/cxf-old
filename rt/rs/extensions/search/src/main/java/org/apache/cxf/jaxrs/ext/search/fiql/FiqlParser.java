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
package org.apache.cxf.jaxrs.ext.search.fiql;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cxf.jaxrs.ext.search.AbstractSearchConditionParser;
import org.apache.cxf.jaxrs.ext.search.AndSearchCondition;
import org.apache.cxf.jaxrs.ext.search.Beanspector;
import org.apache.cxf.jaxrs.ext.search.Beanspector.TypeInfo;
import org.apache.cxf.jaxrs.ext.search.ConditionType;
import org.apache.cxf.jaxrs.ext.search.OrSearchCondition;
import org.apache.cxf.jaxrs.ext.search.PropertyNotFoundException;
import org.apache.cxf.jaxrs.ext.search.SearchBean;
import org.apache.cxf.jaxrs.ext.search.SearchCondition;
import org.apache.cxf.jaxrs.ext.search.SearchParseException;
import org.apache.cxf.jaxrs.ext.search.SearchUtils;
import org.apache.cxf.jaxrs.ext.search.SimpleSearchCondition;
import org.apache.cxf.message.MessageUtils;


/**
 * Parses <a href="http://tools.ietf.org/html/draft-nottingham-atompub-fiql-00">FIQL</a> expression to
 * construct {@link SearchCondition} structure. Since this class operates on Java type T, not on XML
 * structures "selectors" part of specification is not applicable; instead selectors describes getters of type
 * T used as search condition type (see {@link SimpleSearchCondition#isMet(Object)} for details.
 * 
 * @param <T> type of search condition.
 */
public class FiqlParser<T> extends AbstractSearchConditionParser<T> {

    public static final String OR = ",";
    public static final String AND = ";";

    public static final String GT = "=gt=";
    public static final String GE = "=ge=";
    public static final String LT = "=lt=";
    public static final String LE = "=le=";
    public static final String EQ = "==";
    public static final String NEQ = "!=";
    
    public static final Map<ConditionType, String> CONDITION_MAP;
        
    
    public static final String SUPPORT_SINGLE_EQUALS = "fiql.support.single.equals.operator";       
    
    private static final Map<String, ConditionType> OPERATORS_MAP;
    private static final Pattern COMPARATORS_PATTERN;
    private static final Pattern COMPARATORS_PATTERN_SINGLE_EQUALS;
    
    static {
        // operatorsMap
        OPERATORS_MAP = new HashMap<String, ConditionType>();
        OPERATORS_MAP.put(GT, ConditionType.GREATER_THAN);
        OPERATORS_MAP.put(GE, ConditionType.GREATER_OR_EQUALS);
        OPERATORS_MAP.put(LT, ConditionType.LESS_THAN);
        OPERATORS_MAP.put(LE, ConditionType.LESS_OR_EQUALS);
        OPERATORS_MAP.put(EQ, ConditionType.EQUALS);
        OPERATORS_MAP.put(NEQ, ConditionType.NOT_EQUALS);
        
        CONDITION_MAP = new HashMap<ConditionType, String>();
        CONDITION_MAP.put(ConditionType.GREATER_THAN, GT);
        CONDITION_MAP.put(ConditionType.GREATER_OR_EQUALS, GE);
        CONDITION_MAP.put(ConditionType.LESS_THAN, LT);
        CONDITION_MAP.put(ConditionType.LESS_OR_EQUALS, LE);
        CONDITION_MAP.put(ConditionType.EQUALS, EQ);
        CONDITION_MAP.put(ConditionType.NOT_EQUALS, NEQ);
        
        
        // pattern
        String comparators = GT + "|" + GE + "|" + LT + "|" + LE + "|" + EQ + "|" + NEQ;
        String s1 = "[\\p{ASCII}]+(" + comparators + ")";
        COMPARATORS_PATTERN = Pattern.compile(s1);
        
        String s2 = "[\\p{ASCII}]+(" + comparators + "|" + "=" + ")";
        COMPARATORS_PATTERN_SINGLE_EQUALS = Pattern.compile(s2);
    }

    private Beanspector<T> beanspector;       
    private Map<String, String> beanPropertiesMap;
    
    private Map<String, ConditionType> operatorsMap = OPERATORS_MAP;
    private Pattern comparatorsPattern = COMPARATORS_PATTERN;
    /**
     * Creates FIQL parser.
     * 
     * @param tclass - class of T used to create condition objects in built syntax tree. Class T must have
     *            accessible no-arg constructor and complementary setters to these used in FIQL expressions.
     */
    public FiqlParser(Class<T> tclass) {
        this(tclass, Collections.<String, String>emptyMap());
    }
    
    /**
     * Creates FIQL parser.
     * 
     * @param tclass - class of T used to create condition objects in built syntax tree. Class T must have
     *            accessible no-arg constructor and complementary setters to these used in FIQL expressions.
     * @param contextProperties            
     */
    public FiqlParser(Class<T> tclass, Map<String, String> contextProperties) {
        this(tclass, contextProperties, null);
    }
    
    /**
     * Creates FIQL parser.
     * 
     * @param tclass - class of T used to create condition objects in built syntax tree. Class T must have
     *            accessible no-arg constructor and complementary setters to these used in FIQL expressions.
     * @param contextProperties            
     */
    public FiqlParser(Class<T> tclass, 
                      Map<String, String> contextProperties,
                      Map<String, String> beanProperties) {
        super(tclass, contextProperties);
        
        beanspector = SearchBean.class.isAssignableFrom(tclass) 
            ? null : new Beanspector<T>(tclass);
        
        this.beanPropertiesMap = beanProperties;
        if (MessageUtils.isTrue(this.contextProperties.get(SUPPORT_SINGLE_EQUALS))) {
            operatorsMap = new HashMap<String, ConditionType>(operatorsMap);
            operatorsMap.put("=", ConditionType.EQUALS);
            comparatorsPattern = COMPARATORS_PATTERN_SINGLE_EQUALS;
        }
    }
    
    /**
     * Parses expression and builds search filter. Names used in FIQL expression are names of getters/setters
     * in type T.
     * <p>
     * Example:
     * 
     * <pre>
     * class Condition {
     *   public String getFoo() {...}
     *   public void setFoo(String foo) {...}
     *   public int getBar() {...}
     *   public void setBar(int bar) {...}
     * }
     * 
     * FiqlParser&lt;Condition> parser = new FiqlParser&lt;Condition&gt;(Condition.class);
     * parser.parse("foo==mystery*;bar=ge=10");
     * </pre>
     * 
     * @param fiqlExpression expression of filter.
     * @return tree of {@link SearchCondition} objects representing runtime search structure.
     * @throws SearchParseException when expression does not follow FIQL grammar
     */
    public SearchCondition<T> parse(String fiqlExpression) throws SearchParseException {
        ASTNode<T> ast = parseAndsOrsBrackets(fiqlExpression);
        // System.out.println(ast);
        return ast.build();
    }

    private ASTNode<T> parseAndsOrsBrackets(String expr) throws SearchParseException {
        List<String> subexpressions = new ArrayList<String>();
        List<String> operators = new ArrayList<String>();
        int level = 0;
        int lastIdx = 0;
        int idx = 0;
        for (idx = 0; idx < expr.length(); idx++) {
            char c = expr.charAt(idx);
            if (c == '(') {
                level++;
            } else if (c == ')') {
                level--;
                if (level < 0) {
                    throw new SearchParseException(String.format("Unexpected closing bracket at position %d",
                                                               idx));
                }
            }
            String cs = Character.toString(c);
            boolean isOperator = AND.equals(cs) || OR.equals(cs);
            if (level == 0 && isOperator) {
                String s1 = expr.substring(lastIdx, idx);
                String s2 = expr.substring(idx, idx + 1);
                subexpressions.add(s1);
                operators.add(s2);
                lastIdx = idx + 1;
            }
            boolean isEnd = idx == expr.length() - 1;
            if (isEnd) {
                String s1 = expr.substring(lastIdx, idx + 1);
                subexpressions.add(s1);
                operators.add(null);
                lastIdx = idx + 1;
            }
        }
        if (level != 0) {
            throw new SearchParseException(String
                .format("Unmatched opening and closing brackets in expression: %s", expr));
        }
        if (operators.get(operators.size() - 1) != null) {
            String op = operators.get(operators.size() - 1);
            String ex = subexpressions.get(subexpressions.size() - 1);
            throw new SearchParseException("Dangling operator at the end of expression: ..." + ex + op);
        }
        // looking for adjacent ANDs then group them into ORs
        // Note: in case not ANDs is found (e.g only ORs) every single subexpression is
        // treated as "single item group of ANDs"
        int from = 0;
        int to = 0;
        SubExpression ors = new SubExpression(OR);
        while (to < operators.size()) {
            while (to < operators.size() && AND.equals(operators.get(to))) {
                to++;
            }
            SubExpression ands = new SubExpression(AND);
            for (; from <= to; from++) {
                String subex = subexpressions.get(from);
                ASTNode<T> node = null;
                if (subex.startsWith("(")) {
                    node = parseAndsOrsBrackets(subex.substring(1, subex.length() - 1));
                } else {
                    node = parseComparison(subex);
                }
                if (node != null) {
                    ands.add(node);
                }
            }
            to = from;
            if (ands.getSubnodes().size() == 1) {
                ors.add(ands.getSubnodes().get(0));
            } else {
                ors.add(ands);
            }
        }
        if (ors.getSubnodes().size() == 1) {
            return ors.getSubnodes().get(0);
        } else {
            return ors;
        }
    }

    private Comparison parseComparison(String expr) throws SearchParseException {
        Matcher m = comparatorsPattern.matcher(expr);
        if (m.find()) {
            String propertyName = expr.substring(0, m.start(1));
            String operator = m.group(1);
            String value = expr.substring(m.end(1));
            if ("".equals(value)) {
                throw new SearchParseException("Not a comparison expression: " + expr);
            }
            
            String name = unwrapSetter(propertyName);
            String beanPropertyName = beanPropertiesMap == null ? null : beanPropertiesMap.get(name);
            if (beanPropertyName != null) {
                name = beanPropertyName;
            }
            
            TypeInfoObject castedValue = parseType(propertyName, name, value);
            if (castedValue != null) {
                return new Comparison(name, operator, castedValue);
            } else if (MessageUtils.isTrue(contextProperties.get(SearchUtils.LAX_PROPERTY_MATCH))) {
                return null;
            } else {
                throw new PropertyNotFoundException(name, value);
            }
        } else {
            throw new SearchParseException("Not a comparison expression: " + expr);
        }
    }

    
    private TypeInfoObject parseType(String originalName, String setter, String value) throws SearchParseException {
        String name = getSetter(setter);
        
        try {
            TypeInfo typeInfo = 
                beanspector != null ? beanspector.getAccessorTypeInfo(name) 
                    : new TypeInfo(String.class, String.class);
            Object object = parseType(originalName, null, null, setter, typeInfo, value);
            return new TypeInfoObject(object, typeInfo);
        } catch (Exception e) {
            return null;
        }
        
    }
    
    
    
    
    private String unwrapSetter(String setter) {
        if (setter.startsWith(EXTENSION_COUNT_OPEN) && setter.endsWith(")")) {
            return setter.substring(EXTENSION_COUNT_OPEN.length(), setter.length() - 1);        
        } else {
            return setter;
        }
    }
    
    // node of abstract syntax tree
    private interface ASTNode<T> {
        SearchCondition<T> build() throws SearchParseException;
    }

    private class SubExpression implements ASTNode<T> {
        private String operator;
        private List<ASTNode<T>> subnodes = new ArrayList<ASTNode<T>>();

        public SubExpression(String operator) {
            this.operator = operator;
        }

        public void add(ASTNode<T> node) {
            subnodes.add(node);
        }

        public List<ASTNode<T>> getSubnodes() {
            return Collections.unmodifiableList(subnodes);
        }

        @Override
        public String toString() {
            String s = operator.equals(AND) ? "AND" : "OR";
            StringBuilder builder = new StringBuilder(s);
            builder.append(":[");
            for (int i = 0; i < subnodes.size(); i++) {
                builder.append(subnodes.get(i));
                if (i < subnodes.size() - 1) {
                    builder.append(", ");
                }
            }
            builder.append("]");
            return builder.toString();
        }

        public SearchCondition<T> build() throws SearchParseException {
            List<SearchCondition<T>> scNodes = new ArrayList<SearchCondition<T>>();
            for (ASTNode<T> node : subnodes) {
                scNodes.add(node.build());
            }
            if (OR.equals(operator)) {
                return new OrSearchCondition<T>(scNodes);
            } else {
                return new AndSearchCondition<T>(scNodes);
            }
            
        }
    }

    private class Comparison implements ASTNode<T> {
        private String name;
        private String operator;
        private TypeInfoObject tvalue;

        public Comparison(String name, String operator, TypeInfoObject value) {
            this.name = name;
            this.operator = operator;
            this.tvalue = value;
        }

        @Override
        public String toString() {
            return name + " " + operator + " " + tvalue.getObject() 
                + " (" + tvalue.getObject().getClass().getSimpleName() + ")";
        }

        public SearchCondition<T> build() throws SearchParseException {
            String templateName = getSetter(name);
            T cond = createTemplate(templateName);
            ConditionType ct = operatorsMap.get(operator);
            
            if (isPrimitive(cond)) {
                return new SimpleSearchCondition<T>(ct, cond); 
            } else {
                String templateNameLCase = templateName.toLowerCase();
                return new SimpleSearchCondition<T>(Collections.singletonMap(templateNameLCase, ct),
                                                    Collections.singletonMap(templateNameLCase, name),
                                                    Collections.singletonMap(templateNameLCase, tvalue.getTypeInfo()),
                                                    cond);
            }
        }

        private boolean isPrimitive(T pojo) {
            return pojo.getClass().getName().startsWith("java.lang");
        }
        
        @SuppressWarnings("unchecked")
        private T createTemplate(String setter) throws SearchParseException {
            try {
                if (beanspector != null) {
                    beanspector.instantiate().setValue(setter, tvalue.getObject());
                    return beanspector.getBean();
                } else {
                    SearchBean bean = (SearchBean)conditionClass.newInstance();
                    bean.set(setter, tvalue.getObject().toString());
                    return (T)bean;
                }
            } catch (Throwable e) {
                throw new SearchParseException(e);
            }
        }
    }
    
    static class TypeInfoObject {
        private Object object;
        private TypeInfo typeInfo;
        
        public TypeInfoObject(Object object, TypeInfo typeInfo) {
            this.object = object;
            this.typeInfo = typeInfo;
        }

        public TypeInfo getTypeInfo() {
            return typeInfo;
        }

        public Object getObject() {
            return object;
        }
                
    }
}
