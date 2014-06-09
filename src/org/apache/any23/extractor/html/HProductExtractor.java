package org.apache.any23.extractor.html;
import org.apache.any23.extractor.ExtractionException;
import org.apache.any23.extractor.ExtractionResult;
import org.apache.any23.extractor.ExtractorDescription;
import org.apache.any23.vocab.HProduct;
import org.openrdf.model.BNode;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;
import org.w3c.dom.Node;


public class HProductExtractor extends EntityBasedMicroformatExtractor {

	private static final HProduct vHPRODUCT = HProduct.getInstance();

    @Override
    public ExtractorDescription getDescription() {
        return HProductExtractorFactory.getDescriptionInstance();
    }

    @Override
    protected String getBaseClassName() {
        return "hproduct";
    }

    @Override
    protected void resetExtractor() {
        // Empty.
    }

    @Override
    protected boolean extractEntity(Node node, ExtractionResult out) throws ExtractionException {
        final BNode product = getBlankNodeFor(node);
        conditionallyAddResourceProperty(product, RDF.TYPE, vHPRODUCT.Product);
        final HTMLDocument fragment = new HTMLDocument(node);
        addBrand(fragment, product);
        addCategory(fragment, product);
        addPrice(fragment, product);
        addDescription(fragment, product);
        addFN(fragment, product);
        addPhoto(fragment, product);
        addURL(fragment, product);
        addReview(fragment, product);
        addListing(fragment, product);
        addIdentifier(fragment, product);
        return true;
    }

    /**
     * Maps a field text with a property.
     *
     * @param fragment
     * @param product
     * @param fieldClass
     * @param property
     */
    private void mapFieldWithProperty(HTMLDocument fragment, BNode product, String fieldClass, URI property) {
        HTMLDocument.TextField title = fragment.getSingularTextField(fieldClass);
        conditionallyAddStringProperty(
                title.source(), product, property, title.value()
        );
    }

    /**
     * Adds the <code>fn</code> triple.
     *
     * @param fragment
     * @param product
     */
    private void addFN(HTMLDocument fragment, BNode recipe) {
        mapFieldWithProperty(fragment, recipe, "fn", vHPRODUCT.fn);
    }

    /**
     * Adds the <code>instruction</code> triples.
     *
     * @param fragment
     * @param product
     */
    private void addDescription(HTMLDocument fragment, BNode product) {
        mapFieldWithProperty(fragment, product, "description", vHPRODUCT.description);
    }

    /**
     * Adds the <code>instruction</code> triples.
     *
     * @param fragment
     * @param product
     */
    private void addBrand(HTMLDocument fragment, BNode product) {
        mapFieldWithProperty(fragment, product, "brand", vHPRODUCT.brand);
    }

    /**
     * Adds the <code>instruction</code> triples.
     *
     * @param fragment
     * @param product
     */
    private void addCategory(HTMLDocument fragment, BNode product) {
        mapFieldWithProperty(fragment, product, "category", vHPRODUCT.category);
    }

    /**
     * Adds the <code>instruction</code> triples.
     *
     * @param fragment
     * @param product
     */
    private void addPrice(HTMLDocument fragment, BNode product) {
        mapFieldWithProperty(fragment, product, "price", vHPRODUCT.price);
    }

    /**
     * Adds the <code>photo</code> triples.
     *
     * @param fragment
     * @param product
     * @throws ExtractionException
     */
    private void addPhoto(HTMLDocument fragment, BNode recipe) throws ExtractionException {
        final HTMLDocument.TextField[] photos = fragment.getPluralUrlField("photo");
        for(HTMLDocument.TextField photo : photos) {
            addURIProperty(recipe, vHPRODUCT.photo, fragment.resolveURI(photo.value()));
        }
    }

    /**
     * Adds the <code>instruction</code> triples.
     *
     * @param fragment
     * @param product
     */
    private void addReview(HTMLDocument fragment, BNode product) {
        mapFieldWithProperty(fragment, product, "review", vHPRODUCT.review);
    }

    /**
     * Adds the <code>instruction</code> triples.
     *
     * @param fragment
     * @param product
     */
    private void addURL(HTMLDocument fragment, BNode product) {
        mapFieldWithProperty(fragment, product, "url", vHPRODUCT.url);
    }

    /**
     * Adds the <code>instruction</code> triples.
     *
     * @param fragment
     * @param product
     */
    private void addListing(HTMLDocument fragment, BNode product) {
        mapFieldWithProperty(fragment, product, "listing", vHPRODUCT.listing);
    }

    /**
     * Adds the <code>instruction</code> triples.
     *
     * @param fragment
     * @param product
     */
    private void addIdentifier(HTMLDocument fragment, BNode product) {
        mapFieldWithProperty(fragment, product, "identifier", vHPRODUCT.identifier);
    }
}
