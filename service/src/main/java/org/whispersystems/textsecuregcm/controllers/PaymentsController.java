/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.PaymentsServiceConfiguration;
import org.whispersystems.textsecuregcm.currency.CurrencyConversionManager;
import org.whispersystems.textsecuregcm.entities.CurrencyConversionEntityList;

@Path("/v1/payments")
@Tag(name = "Payments")
public class PaymentsController {

  private final ExternalServiceCredentialsGenerator paymentsServiceCredentialsGenerator;
  private final CurrencyConversionManager currencyManager;
  private static final String PAYMENTS_DISABLED_MESSAGE =
      "Payment and donation endpoints are disabled on this server";
  private static final Response.Status PAYMENT_DISABLED_STATUS = Response.Status.NOT_IMPLEMENTED;
  private static final boolean PAYMENT_ENDPOINTS_DISABLED =
      Boolean.parseBoolean(System.getenv().getOrDefault("SIGNAL_DISABLE_PAYMENT_ENDPOINTS", "true"));


  public static ExternalServiceCredentialsGenerator credentialsGenerator(final PaymentsServiceConfiguration cfg) {
    return ExternalServiceCredentialsGenerator
        .builder(cfg.userAuthenticationTokenSharedSecret())
        .prependUsername(true)
        .build();
  }

  public PaymentsController(final CurrencyConversionManager currencyManager,
      final ExternalServiceCredentialsGenerator paymentsServiceCredentialsGenerator) {
    this.currencyManager = currencyManager;
    this.paymentsServiceCredentialsGenerator = paymentsServiceCredentialsGenerator;
  }

  @GET
  @Path("/auth")
  @Produces(MediaType.APPLICATION_JSON)
  public ExternalServiceCredentials getAuth(final @Auth AuthenticatedDevice auth) {
    if (PAYMENT_ENDPOINTS_DISABLED) {
      throw new WebApplicationException(paymentDisabledResponse());
    }

    return paymentsServiceCredentialsGenerator.generateForUuid(auth.accountIdentifier());
  }

  @GET
  @Path("/conversions")
  @Produces(MediaType.APPLICATION_JSON)
  public CurrencyConversionEntityList getConversions(final @Auth AuthenticatedDevice auth) {
    if (PAYMENT_ENDPOINTS_DISABLED) {
      throw new WebApplicationException(paymentDisabledResponse());
    }

    return currencyManager.getCurrencyConversions().orElseThrow();
  }

  private static Response paymentDisabledResponse() {
    return Response.status(PAYMENT_DISABLED_STATUS)
        .type(MediaType.TEXT_PLAIN_TYPE)
        .entity(PAYMENTS_DISABLED_MESSAGE)
        .build();
  }
}
