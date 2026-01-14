package com.novibe.common.config;

import java.util.Objects;

public class EnvironmentVariables {

    public static final String DNS = Objects.requireNonNull(System.getenv("DNS"));

    public static final String NEXTDNS_PROFILE_ID = Objects.requireNonNull(System.getenv("CLIENT_ID"));

    public static final String NEXTDNS_API_KEY = Objects.requireNonNull(System.getenv("AUTH_SECRET"));

    public static final String BLOCK = System.getenv("BLOCK");

    public static final String REDIRECT = System.getenv("REDIRECT");

}
