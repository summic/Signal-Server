/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.auth;

import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkException;
import com.auth0.jwk.UrlJwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.dropwizard.auth.Authenticator;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.OAuthConfiguration;

public class OAuthTokenAuthenticator implements Authenticator<String, AuthenticatedDevice> {

  private static final Logger log = LoggerFactory.getLogger(OAuthTokenAuthenticator.class);

  private final OAuthConfiguration configuration;
  private final UrlJwkProvider jwkProvider;

  public OAuthTokenAuthenticator(final OAuthConfiguration configuration) throws MalformedURLException {
    this.configuration = configuration;
    this.jwkProvider = new UrlJwkProvider(new URL(configuration.jwksUrl()));
  }

  @Override
  public Optional<AuthenticatedDevice> authenticate(final String token) {
    if (token == null || token.isBlank()) {
      return Optional.empty();
    }

    try {
      final DecodedJWT decoded = JWT.decode(token);
      if (!"RS256".equalsIgnoreCase(configuration.algorithm())) {
        log.warn("Unsupported OAuth algorithm {}; expected RS256", configuration.algorithm());
        return Optional.empty();
      }

      final String keyId = decoded.getKeyId();
      if (keyId == null || keyId.isBlank()) {
        return Optional.empty();
      }

      final Jwk jwk = jwkProvider.get(keyId);
      if (!(jwk.getPublicKey() instanceof RSAPublicKey rsaPublicKey)) {
        return Optional.empty();
      }

      JWTVerifier.BaseVerification verifierBuilder = (JWTVerifier.BaseVerification) JWT.require(Algorithm.RSA256(rsaPublicKey, null))
          .acceptLeeway(configuration.clockSkewSeconds());

      if (configuration.issuer() != null && !configuration.issuer().isBlank()) {
        verifierBuilder = (JWTVerifier.BaseVerification) verifierBuilder.withIssuer(configuration.issuer());
      }
      if (configuration.audience() != null && !configuration.audience().isBlank()) {
        verifierBuilder = (JWTVerifier.BaseVerification) verifierBuilder.withAudience(configuration.audience());
      }

      final DecodedJWT verified = verifierBuilder.build().verify(token);

      final UUID accountIdentifier = UUID.fromString(verified.getSubject());
      final Integer deviceIdInt = verified.getClaim("device_id").asInt();
      if (deviceIdInt == null || deviceIdInt < 1 || deviceIdInt > 127) {
        return Optional.empty();
      }

      final Claim primarySeenClaim = verified.getClaim("primary_device_last_seen_epoch_seconds");
      final Long primarySeenEpochSeconds = primarySeenClaim.isNull() ? null : primarySeenClaim.asLong();
      final Instant primaryDeviceLastSeen = primarySeenEpochSeconds == null
          ? Instant.now()
          : Instant.ofEpochSecond(primarySeenEpochSeconds);

      return Optional.of(new AuthenticatedDevice(accountIdentifier, deviceIdInt.byteValue(), primaryDeviceLastSeen));
    } catch (JwkException | JWTVerificationException | IllegalArgumentException e) {
      return Optional.empty();
    } catch (Exception e) {
      log.warn("Unexpected OAuth auth failure", e);
      return Optional.empty();
    }
  }
}
