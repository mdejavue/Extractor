package org.apache.any23.vocab;

import org.apache.any23.vocab.SINDICE;
import org.apache.any23.vocab.Vocabulary;
import org.openrdf.model.URI;


public class HProduct extends Vocabulary {
	 public static final String NS = SINDICE.NS + "hproduct/";

	    private static HProduct instance;

	    public static HProduct getInstance() {
	        if(instance == null) {
	            instance = new HProduct();
	        }
	        return instance;
	    }

	    // Resources.
	    public URI Product     = createClass(NS, "Product");

	    // Properties.
	    public URI brand                     = createProperty(NS, "brand");
	    public URI category              	 = createProperty(NS, "category");
	    public URI price         			 = createProperty(NS, "price");
	    public URI description         		 = createProperty(NS, "description");
	    public URI fn                  		 = createProperty(NS, "fn");
	    public URI photo               		 = createProperty(NS, "photo");
	    public URI url                		 = createProperty(NS, "url");
	    public URI review             		 = createProperty(NS, "review");
	    public URI listing            		 = createProperty(NS, "listing");
	    public URI identifier        		 = createProperty(NS, "identifier");

	    private HProduct() {
	        super(NS);
	    }
}
