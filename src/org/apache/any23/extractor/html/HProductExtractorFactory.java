
package org.apache.any23.extractor.html;

import java.util.Arrays;

import org.apache.any23.extractor.ExtractorDescription;
import org.apache.any23.extractor.ExtractorFactory;
import org.apache.any23.extractor.SimpleExtractorFactory;
import org.apache.any23.rdf.PopularPrefixes;
import org.apache.any23.rdf.Prefixes;
import org.kohsuke.MetaInfServices;


@MetaInfServices(ExtractorFactory.class)
public class HProductExtractorFactory extends SimpleExtractorFactory<HProductExtractor> implements
        ExtractorFactory<HProductExtractor> {

    public static final String NAME = "html-mf-hproduct";
    
    public static final Prefixes PREFIXES = PopularPrefixes.createSubset("rdf");

    private static final ExtractorDescription descriptionInstance = new HProductExtractorFactory();
    
    public HProductExtractorFactory() {
        super(
                HProductExtractorFactory.NAME, 
                HProductExtractorFactory.PREFIXES,
                Arrays.asList("text/html;q=0.1", "application/xhtml+xml;q=0.1"),
                "example-mf-hproduct.html");
    }
    
    @Override
    public HProductExtractor createExtractor() {
        return new HProductExtractor();
    }

    public static ExtractorDescription getDescriptionInstance() {
        return descriptionInstance;
    }
}