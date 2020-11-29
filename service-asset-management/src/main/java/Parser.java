import com.sitewhere.microservice.kafka.EnrichedCommandInvocationsConsumer;
import com.sitewhere.microservice.kafka.KafkaTopicNaming;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import java.io.*;
import java.lang.reflect.Field;
import java.util.*;


public class Parser {
    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException {
        // command delivery ->  erro ao capturar a classe de um produtor
        // device management -> exceção ao algoritmo

        String[] microservices = new String[] {"command delivery", "batch operations", "asset management",
                "device management", "device registration", "device state",
                "event management", "event search", "event sources", "inbound processing", "instance management",
                "label generation", "outbound connectors", "schedule management", "streaming media"};

        List<GraphRelation> graphRelations = generateGraphRelations();

        for (String microservice : microservices) {
            generateRelationForMicroservice(microservice, graphRelations);
        }

        printGraphRelation(graphRelations);

        writeDot(graphRelations);

//        try {
//            stripDuplicatesFromFile("g.dot");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }

    static void writeDot(List<GraphRelation> graphRelations){
        try(BufferedWriter out=new BufferedWriter(new OutputStreamWriter(new FileOutputStream("g.dot")))){
            out.write("digraph {");
            out.newLine();
            for(GraphRelation relation : graphRelations){
                if (relation.producers.isEmpty() && !relation.consumers.isEmpty()) {
                    for (String consumer: relation.consumers) {
                        out.write(consumer.replace(" ", "") + ";\n");
                    }
                } else if (!relation.producers.isEmpty() && relation.consumers.isEmpty()) {
                    for (String producer : relation.producers) {
                        out.write(producer.replace(" ", "") + ";\n");
                    }

                } else if (!relation.producers.isEmpty() && !relation.consumers.isEmpty()) {
                    for (String producer : relation.producers) {
                        for (String consumer : relation.consumers) {
                            out.write(producer.replace(" ", "") + " -> " +
                                    consumer.replace(" ", "") + "[ label=\"" + relation.topic +" \"];\n");
                        }
                    }
                }
            }
            out.write("}");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void stripDuplicatesFromFile(String filename) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        LinkedHashSet<String> lines = new LinkedHashSet<>(10000);
        String line;
        while ((line = reader.readLine()) != null) {
            lines.add(line);
        }
        reader.close();
        BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
        for (String unique : lines) {
            writer.write(unique);
            writer.newLine();
        }
        writer.close();
    }

    public static void printGraphRelation(List<GraphRelation> graphRelations) {
        for (GraphRelation relation : graphRelations) {
            StringBuilder sbConsumer = new StringBuilder();
            StringBuilder sbProducer = new StringBuilder();

            for (String consumer : relation.consumers) {
                sbConsumer.append(" ").append(consumer).append(" ");
            }

            for (String producer : relation.producers) {
                sbProducer.append(" ").append(producer).append(" ");
            }

            System.out.println("Consumers: " + sbConsumer.toString()
                    + "---  Topic: " + relation.topic + " ---" + " Producers:" + sbProducer.toString());
        }
    }

    public static void generateRelationForMicroservice(String microservice, List<GraphRelation> graphRelations)
            throws ClassNotFoundException {

        String microserviceDirectory = locateDirectory("/Users/joaopedrodeamorim/Desktop/swhere 2/sitewhere",
                microservice);

        Set<String> resultSet = new HashSet<>();

        locateDirectoryRecursive(microserviceDirectory + "/src/main/java", "kafka", resultSet);

        // We're not interested in interfaces
        resultSet.removeIf(entry -> entry.contains("spi"));

        Set<String> classSet = new HashSet<>();

        for (String entry : resultSet) {
            getClasses(entry, classSet);
        }

        for (String clazzName : classSet) {
            if (isConsumer(Class.forName(clazzName))) {
                createConsumerVertice(graphRelations, clazzName, microservice);
            } else if (isProducer(Class.forName(clazzName))) {
                createProducerVertice(graphRelations, clazzName, microservice);
            }
        }
    }

    public static List<GraphRelation> generateGraphRelations() throws IllegalAccessException {
        List<GraphRelation> resultList = new ArrayList<>();

        Class clazz = KafkaTopicNaming.class;

        KafkaTopicNaming naming = new KafkaTopicNaming();


        Field[] fields = clazz.getDeclaredFields();

        for (Field field : fields) {
            if (field.getType() != String.class) {
                continue;
            }
            if (field.getName().contains("TOPIC")) {
                field.setAccessible(true);
                Object value = field.get(naming);
                String valueString = value.toString();
                resultList.add(new GraphRelation(valueString));
            }
        }

        return resultList;
    }


    public static void createConsumerVertice(List<GraphRelation> relationList, String consumerName,
                                             String microservice) {
        for (GraphRelation relation : relationList) {
            if(nameMatchingTopicConsumer(relation.topic, consumerName)) {
                relation.consumers.add(microservice);
            }
        }
    }

    public static void createProducerVertice(List<GraphRelation> relationList, String producerName,
                                             String microservice) {
        for (GraphRelation relation : relationList) {
            if(nameMatchingTopicProducer(relation.topic, producerName)) {
                relation.producers.add(microservice);
            }
        }
    }

    public static boolean nameMatchingTopicConsumer(String topic, String consumer) {
        if (!consumer.contains("Consumer")) {
            return false;
        }

        String meaningfulSubstringForConsumer = consumer.substring(consumer.indexOf("kafka") + 6,
                consumer.lastIndexOf("Consumer"));

        String[] meaningfulSubstringsForConsumer = meaningfulSubstringForConsumer.split("(?=\\p{Upper})");

        int containsCount = 0;

        for (String substring : meaningfulSubstringsForConsumer) {
            if(topic.contains(substring.toLowerCase())) {
                containsCount++;
            }
        }

        return containsCount >= meaningfulSubstringsForConsumer.length;
    }

    public static boolean nameMatchingTopicProducer(String topic, String producer) {
        if (!producer.contains("Producer")) {
            return false;
        }

        String meaningfulSubstringForProducer = producer.substring(producer.indexOf("kafka") + 6,
                producer.lastIndexOf("Producer"));

        String[] meaningfulSubstringsForProducer = meaningfulSubstringForProducer.split("(?=\\p{Upper})");

        int containsCount = 0;

        for (String substring : meaningfulSubstringsForProducer) {
            if(topic.contains(substring.toLowerCase())) {
                containsCount++;
            }
        }

        return containsCount >= meaningfulSubstringsForProducer.length;
    }

    static class GraphRelation {
        public GraphRelation(String topic) {
            this.topic = topic;
        }

        public final String topic;
        public Set<String> consumers = new HashSet<>();
        public Set<String> producers = new HashSet<>();
    }

    /**
     * Method to determine whether or not a certain Class is a Kafka Producer.
     *
     * @param subjectClass the class that will be inspected
     * @return a boolean value indicating if the given class is a Kafka Producer
     **/
    public static boolean isProducer(Class<?> subjectClass) {
        // Check the fields within the subject and see if any of them is an instance of KafkaProducer
        Field[] fields = subjectClass.getDeclaredFields();

        for (Field field : fields) {
            boolean isInstanceOfKafkaProducer = field.getType().equals(KafkaProducer.class);
            if (isInstanceOfKafkaProducer) {
                return true;
            }
        }

        // If there's a Superclass, check if it has an instance of a KafkaProducer
        if (subjectClass.getSuperclass() != null) {
            return isProducer(subjectClass.getSuperclass());
        } else { // If there's no Superclass, recursion bottoms out and should return false
            return false;
        }
    }

    /**
     * Method to determine whether or not a certain Class is a Kafka Consumer. It utilizes the same logic as the
     * isProducer method.
     *
     * @param subjectClass the class that will be inspected
     * @return a boolean value indicating if the given class is a Kafka Consumer
     * @see Test
     **/
    public static boolean isConsumer(Class<?> subjectClass) {
        Field[] fields = subjectClass.getDeclaredFields();

        for (Field field : fields) {
            // use instanceof instead
            boolean isInstanceOfKafkaConsumer = field.getType().equals(KafkaConsumer.class);
            if (isInstanceOfKafkaConsumer) {
                return true;
            }
        }

        if (subjectClass.getSuperclass() != null) {
            return isConsumer(subjectClass.getSuperclass());
        } else {
            return false;
        }
    }

    public static void getClasses(String directoryPath, Set<String> resultSet) {
        File directory = new File(directoryPath);
        File[] fileList = directory.listFiles();

        if (fileList == null) {
            throw new NullPointerException();
        }

        for (File file : fileList) {
            if (file.isDirectory()) {
                for (File subfile : file.listFiles()) {
                    String classString = generateClassString(subfile.getAbsolutePath());
                    resultSet.add(classString);
                }
                continue;
            }
            String classString = generateClassString(file.getAbsolutePath());
            resultSet.add(classString);
        }
    }

    public static String generateClassString(String filePath) {
        return filePath.substring(filePath.lastIndexOf("com"), filePath.lastIndexOf(".java"))
                .replace("/", ".");
    }

    public static void locateDirectoryRecursive(String directoryName, String target, Set<String> resultSet) {
        File directory = new File(directoryName);
        File[] fileList = directory.listFiles();

        if (fileList == null) {
            throw new NullPointerException();
        }

        for (File file : fileList) {
            if (file.isDirectory()) {
                if (compare(file.getAbsolutePath(), target)) {
                    resultSet.add(file.getAbsolutePath());
                } else {
                    locateDirectoryRecursive(file.getAbsolutePath(), target, resultSet);
                }
            }
        }
    }

    public static String locateDirectory(String directoryPath, String target) {
        File directory = new File(directoryPath);
        File[] fileList = directory.listFiles();

        if (fileList == null) {
            throw new NullPointerException();
        }

        for (File file : fileList) {
            if (compare(file.getAbsolutePath(), target)) {
                return file.getAbsolutePath();
            }
        }

        return "";
    }

    private static boolean compare(String filePath, String microservice) {
        String meaningfulSubstring = filePath.substring(filePath.lastIndexOf("sitewhere/") + 10);

        meaningfulSubstring = meaningfulSubstring.replace("-", " ");

        return meaningfulSubstring.toLowerCase().contains(microservice.toLowerCase());
    }
}





