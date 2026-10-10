package dev.relayops;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class ApiKeyFilter extends OncePerRequestFilter {
 private final String key;
 public ApiKeyFilter(@Value("${relay.worker-key:}") String key) { this.key=key; }
 @Override protected void doFilterInternal(HttpServletRequest request,HttpServletResponse response,FilterChain chain)
   throws ServletException,IOException {
   String path=request.getServletPath();
   if(path.startsWith("/api/") && !path.equals("/api/healthz") && !key.isEmpty()) {
     String supplied=request.getHeader("X-API-Key");
     if(supplied==null || !MessageDigest.isEqual(key.getBytes(StandardCharsets.UTF_8), supplied.getBytes(StandardCharsets.UTF_8))) {
       response.setStatus(401);response.setContentType("application/json");
       response.getWriter().write("{\"detail\":\"API key required\"}");return;
     }
   }
   chain.doFilter(request,response);
 }
}
