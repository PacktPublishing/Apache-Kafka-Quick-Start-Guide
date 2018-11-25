package monedero;

public class ProcessingEngine {

  public static void main(String[] args) {
    String servers = args[0];
    String groupId = args[1];
    String sourceTopic = args[2];
    String targetTopic = args[3];

    Reader reader = new Reader(servers, groupId, sourceTopic);

    Writer writer = new Writer(servers, targetTopic);
    reader.run(writer);
  }
}

/*
package monedero;

public class ProcessingEngine {

  public static void main(String[] args){
    String servers = args[0];
    String groupId = args[1];
    String sourceTopic = args[2];
    String validTopic = args[3];
    String invalidTopic = args[4];

    Reader reader = new Reader(servers, groupId, sourceTopic);

    //Validator validator = new Validator(servers, validTopic, invalidTopic);
    Enricher enricher = new Enricher(servers, validTopic, invalidTopic);
    reader.run(enricher);

  }
}
*/