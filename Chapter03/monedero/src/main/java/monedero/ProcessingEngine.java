package monedero;

public class ProcessingEngine {

  public static void main(String[] args){
    String servers = args[0];
    String groupId = args[1];
    String sourceTopic = args[2];
    String validTopic = args[3];
    String invalidTopic = args[4];

    Reader reader = new Reader(servers, groupId, sourceTopic);

    Enricher enricher = new Enricher(servers, validTopic, invalidTopic);
    reader.run(enricher);

  }
}
