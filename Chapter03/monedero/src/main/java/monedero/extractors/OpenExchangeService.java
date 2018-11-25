package monedero.extractors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class OpenExchangeService {

  private static final String API_KEY = "YOUR_API_KEY_VALUE_HERE";  //1
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public double getPrice(String currency) {
    try {
      final URL url = new URL("https://openexchangerates.org/api/latest.json?app_id=" + API_KEY);  //2

      final JsonNode root = MAPPER.readTree(url);
      final JsonNode node = root.path("rates").path(currency);   //3
      return Double.parseDouble(node.toString());                //4

    } catch (IOException ex) {
      Logger.getLogger(OpenExchangeService.class.getName()).log(Level.SEVERE, null, ex);
    }

    return 0;
  }
}
