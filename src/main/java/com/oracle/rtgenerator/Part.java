package com.oracle.rtgenerator;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Part {
	String Description;
	double UnitPrice;
	long UPCCode;

	public Part() {
	}

	@JsonProperty("Description")
	public String getDescription() {
		return Description;
	}

	@JsonProperty("Description")
	public void setDescription(String description) {
		Description = description;
	}

	@JsonProperty("UnitPrice")
	public double getUnitPrice() {
		return UnitPrice;
	}

	@JsonProperty("UnitPrice")
	public void setUnitPrice(double unitPrice) {
		UnitPrice = unitPrice;
	}

	@JsonProperty("UPCCode")
	public long getUPCCode() {
		return UPCCode;
	}

	@JsonProperty("UPCCode")
	public void setUPCCode(long UPCCode) {
		this.UPCCode = UPCCode;
	}
}
