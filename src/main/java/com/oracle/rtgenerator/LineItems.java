package com.oracle.rtgenerator;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LineItems {
	Part part;

	public LineItems() {
	}

	@JsonProperty("Part")
	public Part getPart() {
		return part;
	}

	@JsonProperty("Part")
	public void setPart(Part part) {
		this.part = part;
	}
}
