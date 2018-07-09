package org.apache.nutch.plugin.selector;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to index the content which has been parsed and stored in the {@link HtmlElementSelectorFilter}.
 *
 * Adapted from https://github.com/kaqqao/nutch-element-selector
 *
 * original author Elisabeth Adler
 *
 * @author Charlie Chen
 */
public class HtmlElementSelectorIndexer implements IndexingFilter {
    private final Logger log = LoggerFactory.getLogger(HtmlElementSelectorIndexer.class);

    private Configuration conf;
    private String storageField;

    @Override
    public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
            CrawlDatum datum, Inlinks inlinks) throws IndexingException {

        if (storageField != null) {
            String strippedContent = parse.getData().getMeta(storageField);
            if (strippedContent != null) {
                doc.add(storageField, strippedContent);
            }
        }

        return doc;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.storageField = getConf().get("parser.html.selector.storage_field", null);
        if (StringUtils.isNotBlank(this.storageField)) {
            log.info("Configured using [parser.html.selector.storage_field] to use meta field [{}] as storage",
                    storageField);
        }
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
