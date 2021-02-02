package com.oracle.rtgenerator;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PO {
	LineItems[] lineItems;

	public PO() {
	}

	@JsonProperty("LineItems")
	public LineItems[] getLineItems() {
		return lineItems;
	}

	@JsonProperty("LineItems")
	public void setLineItems(LineItems[] lineItems) {
		this.lineItems = lineItems;
	}
}
