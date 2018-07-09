package org.apache.nutch.plugin.selector;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.parse.*;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.NodeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Class to parse the content and apply a blacklist or whitelist. The content is stored in
 * the index in the configured storage field, or replacing the original content.<br/>
 * If a blacklist configuration is provided, all elements plus their subelements are not included in the
 * final content field which is indexed. If a whitelist configuration is provided, only the elements
 * and their subelements are included in the indexed field.<br/><br/>
 * On the basis of {@link https://issues.apache.org/jira/browse/NUTCH-585}
 *
 * Adapted from https://github.com/kaqqao/nutch-element-selector
 *
 * original author Elisabeth Adler
 *
 * @author Charlie Chen
 */
public class HtmlElementSelectorFilter implements HtmlParseFilter {
    private final Logger log = LoggerFactory.getLogger(HtmlElementSelectorFilter.class);

    private Configuration conf;
    private String storageField;
    private Set<String> protectedURLs;
    private Pattern cssSelectorPattern = Pattern.compile("(\\.|#|\\[|^)([a-zA-Z0-9-_]*)(?:=(.+)\\])?", Pattern.CASE_INSENSITIVE);

    private Set<Selector> blackListSelectors = new HashSet<Selector>();
    private Set<Selector> whiteListSelectors = new HashSet<Selector>();

    @Override
    public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {
        Parse parse = parseResult.get(content.getUrl());

        DocumentFragment rootToIndex;
        StringBuilder strippedContent = new StringBuilder();
        if (whiteListSelectors.size() > 0 && !protectedURLs.contains(content.getBaseUrl())) {
            rootToIndex = (DocumentFragment) doc.cloneNode(false);
            whitelisting(doc, rootToIndex);
        } else if (blackListSelectors.size() > 0 && !protectedURLs.contains(content.getBaseUrl())) {
            rootToIndex = (DocumentFragment) doc.cloneNode(true);
            blacklisting(rootToIndex);
        } else {
            return parseResult;
        }

        getText(strippedContent, rootToIndex);
        if (storageField == null) {
            parseResult.put(new Text(content.getUrl()), new ParseText(strippedContent.toString()), parse.getData());
        } else {
            parse.getData().getContentMeta().set(storageField, strippedContent.toString());
        }
        return parseResult;
    }

    private void blacklisting(Node node) {
        boolean wasStripped = false;

        for(Selector blackListSelector : blackListSelectors) {
            if(blackListSelector.matches(node)) {
                log.debug("In blacklist: {}", printNode(node));
                node.setNodeValue("");

                while (node.hasChildNodes()) {
                    node.removeChild(node.getFirstChild());
                }
                wasStripped = true;
                break;
            }
        }

        if (!wasStripped) {
            NodeList children = node.getChildNodes();
            if (children != null) {
                for (int i = 0; i < children.getLength(); i++) {
                    blacklisting(children.item(i));
                }
            }
        }
    }

    private void whitelisting(Node node, Node newNode) {
        boolean wasAppended = false;

        for(Selector whiteListSelector : whiteListSelectors) {
            if(whiteListSelector.matches(node)) {
                log.debug("In whitelist: {}", printNode(node));
                newNode.appendChild(node.cloneNode(true));
                wasAppended = true;
                break;
            }
        }

        if (!wasAppended) {
            NodeList children = node.getChildNodes();
            if (children != null) {
                for (int i = 0; i < children.getLength(); i++) {
                    whitelisting(children.item(i), newNode);
                }
            }
        }
    }

    private void getText(StringBuilder sb, Node node) {
        NodeWalker walker = new NodeWalker(node);

        while (walker.hasNext()) {
            Node currentNode = walker.nextNode();
            String nodeName = currentNode.getNodeName();
            short nodeType = currentNode.getNodeType();

            if ("script".equalsIgnoreCase(nodeName)) {
                walker.skipChildren();
            }
            if ("style".equalsIgnoreCase(nodeName)) {
                walker.skipChildren();
            }
            if (nodeType == Node.COMMENT_NODE) {
                walker.skipChildren();
            }
            if (nodeType == Node.TEXT_NODE) {
                String text = currentNode.getNodeValue();
                text = text.replaceAll("\\s+", " ");
                text = text.trim();
                if (text.length() > 0) {
                    if (sb.length() > 0) sb.append(' ');
                    sb.append(text);
                }
            }
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;

        String elementsToExclude = getConf().get("parser.html.selector.blacklist", null);
        if ((elementsToExclude != null) && (elementsToExclude.trim().length() > 0)) {
            log.info("Configured using [parser.html.blacklist] to ignore elements [{}]...", elementsToExclude);
            String[] blackListCssSelectors = elementsToExclude.split(",");
            for (String cssSelector : blackListCssSelectors) {
                blackListSelectors.add(parseCssSelector(cssSelector));
            }
        }

        String elementsToInclude = getConf().get("parser.html.selector.whitelist", null);
        if ((elementsToInclude != null) && (elementsToInclude.trim().length() > 0)) {
            log.info("Configured using [parser.html.whitelist] to only use elements [{}]...", elementsToInclude);
            String[] whiteListCssSelectors = elementsToInclude.split(",");
            for (String cssSelector : whiteListCssSelectors) {
                whiteListSelectors.add(parseCssSelector(cssSelector));
            }
        }

        this.storageField = getConf().get("parser.html.selector.storage_field", null);
        if (StringUtils.isNotBlank(storageField)) {
            log.info("Configured using [parser.html.selector.storage_field] to use meta field [{}] as storage",
                    storageField);
        }

        this.protectedURLs = new HashSet<>(Arrays.asList(getConf().get("parser.html.selector.protected_urls", "").split(",")));
        if (!this.protectedURLs.isEmpty()) {
            log.info("Configured using [parser.html.selector.protected_urls]: {}", protectedURLs.toString());
        }
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    private Selector parseCssSelector(String cssSelector) {
        Set<Selector> selectors = new HashSet<>();
        Matcher matcher = cssSelectorPattern.matcher(cssSelector);
        while (matcher.find()) {
            Discriminator discriminator = Discriminator.fromString(matcher.group(1));

            switch (discriminator) {
                case TYPE: selectors.add(new TypeSelector(matcher.group(2))); break;
                case ID: selectors.add(new IdSelector(matcher.group(2))); break;
                case CLASS: selectors.add(new ClassSelector(matcher.group(2))); break;
                case ATTRIBUTE: selectors.add(new AttributeSelector(matcher.group(2), matcher.group(3))); break;
            }
        }

        return new AggregatedSelector(selectors);
    }

    private String printNode(Node node) {
        StringBuilder sb = new StringBuilder();
        sb.append("<").append(node.getNodeName());
        NamedNodeMap attributes = node.getAttributes();
        for (int i = 0; i < attributes.getLength(); i++) {
            Node attr = attributes.item(i);
            if (attr != null && attr.getNodeValue() != null) {
                sb.append(" ").append(attr.getNodeName()).append("=").append(attr.getNodeValue());
            }
        }
        sb.append(">");
        return sb.toString();
    }

    private interface Selector {
        boolean matches(Node node);
    }

    private enum Discriminator {
        TYPE(""), ID("#"), CLASS("."), ATTRIBUTE("[");

        private String discriminatorString;

        Discriminator(String discriminatorString) {
            this.discriminatorString = discriminatorString;
        }

        public static Discriminator fromString(String discriminatorString) {
            if (discriminatorString != null) {
                for (Discriminator discriminator : Discriminator.values()) {
                    if (discriminatorString.equalsIgnoreCase(discriminator.discriminatorString)) {
                        return discriminator;
                    }
                }
            }
            throw new IllegalArgumentException(String.format(
                    "String %s is an invalid CSS selector discriminator. " +
                            "Only \"#\", \".\", \"[\" or an empty string are allowed!", discriminatorString));
        }
    }

    private class TypeSelector implements Selector {

        private String type;

        TypeSelector(String type) {
            assert type != null && type.length() > 0;
            this.type = type;
        }

        @Override
        public boolean matches(Node node) {
            return type.equalsIgnoreCase(node.getNodeName());
        }
    }

    private class ClassSelector implements Selector {

        private String cssClass;

        ClassSelector(String cssClass) {
            assert cssClass != null && cssClass.length() > 0;
            this.cssClass = cssClass;
        }

        @Override
        public boolean matches(Node node) {
            Set<String> classes = new HashSet<>();
            if (node.hasAttributes()) {
                Node classNode = node.getAttributes().getNamedItem("class");
                if (classNode != null && classNode.getNodeValue() != null) {
                    classes.addAll(Arrays.asList(classNode.getNodeValue().toLowerCase().split("\\s+")));
                }
            }
            return classes.contains(cssClass);
        }
    }

    private class IdSelector implements Selector {

        private String id;

        IdSelector(String id) {
            assert id != null && id.length() > 0;
            this.id = id;
        }

        @Override
        public boolean matches(Node node) {
            if (node.hasAttributes()) {
                Node idNode = node.getAttributes().getNamedItem("id");
                return idNode != null && id.equalsIgnoreCase(idNode.getNodeValue());
            }

            return false;
        }
    }

    private class AttributeSelector implements Selector {

        private String attributeName, attributeValue;

        AttributeSelector(String attributeName, String attributeValue) {
            assert attributeName != null && attributeName.length() > 0;
            assert attributeValue != null && attributeValue.length() > 0;
            this.attributeName = attributeName;
            this.attributeValue = attributeValue;
        }

        @Override
        public boolean matches(Node node) {
            for (int i = 0; i < node.getAttributes().getLength(); i++) {
                Node currentAttribute = node.getAttributes().item(i);
                if (attributeName.equalsIgnoreCase(currentAttribute.getNodeName()) && attributeValue.equalsIgnoreCase(currentAttribute.getNodeValue())) {
                    return true;
                }
            }

            return false;
        }
    }

    private class AggregatedSelector implements Selector {

        private Collection<Selector> selectors;

        AggregatedSelector(Collection<Selector> selectors) {
            assert selectors != null;
            this.selectors = selectors;
        }

        @Override
        public boolean matches(Node node) {
            for (Selector selector : selectors) {
                if (!selector.matches(node)) {
                    return false;
                }
            }
            return true;
        }
    }
}
