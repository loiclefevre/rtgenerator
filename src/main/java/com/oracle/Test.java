package com.oracle;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

public class Test {
	public static void main(String[] args) {
		final Instant instant3DaysAgo = Instant.now().minus(Duration.ofDays(3));

		System.out.println(instant3DaysAgo.atOffset(ZoneOffset.UTC));

//		OracleOperationBuilder oob = ((OracleOperationBuilderImpl)collection.find()).lastModified();

	}
}
