package dev.relayops;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.*;
import static org.junit.jupiter.api.Assertions.*;
class ApiKeyFilterTest {
 @Test void missingKeyRejected() throws Exception {
   var request=new MockHttpServletRequest();request.setServletPath("/api/events");
   var response=new MockHttpServletResponse();
   new ApiKeyFilter("secret").doFilter(request,response,new MockFilterChain());
   assertEquals(401,response.getStatus());
 }
 @Test void validKeyAccepted() throws Exception {
   var request=new MockHttpServletRequest();request.setServletPath("/api/events");request.addHeader("X-API-Key","secret");
   var response=new MockHttpServletResponse();var chain=new MockFilterChain();
   new ApiKeyFilter("secret").doFilter(request,response,chain);
   assertNotNull(chain.getRequest());
 }
 @Test void healthAvailableWithoutKey() throws Exception {
   var request=new MockHttpServletRequest();request.setServletPath("/api/healthz");
   var response=new MockHttpServletResponse();var chain=new MockFilterChain();
   new ApiKeyFilter("secret").doFilter(request,response,chain);
   assertNotNull(chain.getRequest());
 }
}
