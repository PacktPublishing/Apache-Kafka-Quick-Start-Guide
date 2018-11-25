package monedero.extractors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OpenWeatherService {

  private static final String API_KEY = "YOUR API_KEY_VALUE"; //1
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public double getTemperature(String lat, String lon) {
    try {
      final URL url = new URL(
          "http://api.openweathermap.org/data/2.5/weather?lat=" + lat + "&lon="+  lon + "&units=metric&appid=" + API_KEY);
      final JsonNode root = MAPPER.readTree(url);
      final JsonNode node = root.path("main").path("temp");
      return Double.parseDouble(node.toString());

    } catch (IOException ex) {
      Logger.getLogger(OpenWeatherService.class.getName()).log(Level.SEVERE, null, ex);
    }
    return 0;
  }
}
