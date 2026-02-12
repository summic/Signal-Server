/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;

public record OAuthConfiguration(
    @JsonProperty boolean enabled,
    @JsonProperty boolean allowBasicAuth,
    @JsonProperty String issuer,
    @JsonProperty String audience,
    @JsonProperty String jwksUrl,
    @JsonProperty String algorithm,
    @JsonProperty @Min(0) long clockSkewSeconds
) {

  public OAuthConfiguration {
    if (algorithm == null || algorithm.isBlank()) {
      algorithm = "RS256";
    }
  }

  public OAuthConfiguration() {
    this(false, true, null, null, null, "RS256", 60);
  }
}
