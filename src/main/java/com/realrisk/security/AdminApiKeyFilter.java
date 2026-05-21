package com.realrisk.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

public class AdminApiKeyFilter extends OncePerRequestFilter {
  private static final String BEARER_PREFIX = "Bearer ";

  private final byte[] expectedApiKey;

  public AdminApiKeyFilter(String adminApiKey) {
    this.expectedApiKey = adminApiKey.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest request) {
    String path = request.getRequestURI();
    return !path.equals("/admin") && !path.startsWith("/admin/");
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
      throws ServletException, IOException {
    String authorization = request.getHeader("Authorization");
    if (authorization == null || !authorization.startsWith(BEARER_PREFIX)) {
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      return;
    }

    byte[] presentedApiKey =
        authorization.substring(BEARER_PREFIX.length()).getBytes(StandardCharsets.UTF_8);
    if (!MessageDigest.isEqual(expectedApiKey, presentedApiKey)) {
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      return;
    }

    var authentication =
        new UsernamePasswordAuthenticationToken("admin-api-key", null, List.of());
    SecurityContextHolder.getContext().setAuthentication(authentication);
    try {
      filterChain.doFilter(request, response);
    } finally {
      SecurityContextHolder.clearContext();
    }
  }
}
