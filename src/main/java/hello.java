import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.*;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static java.lang.System.exit;
import static java.lang.System.setErr;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class hello {

//    public static final String allChar = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";


//    /**
//     * 由大小写字母、数字组成的随机字符串
//     * @param length
//     * @return
//     */
//    public static String generateString(int length) {
//        StringBuffer sb = new StringBuffer();
//        Random random = new Random();
//        for (int i = 0; i < length; i++) {
//            sb.append(allChar.charAt(random.nextInt(allChar.length())));
//        }
//        return sb.toString();
//    }



    public static void main(String[] args) {

        int N = 10000000;
//        int LENGTH = 10;
        int BULK_SIZE =10000;
        int BULK_NUM =10000;

        //非集群client
//        RestHighLevelClient client = new RestHighLevelClient(
//                RestClient.builder(
//                        new HttpHost("localhost", 9200, "http"),
//                        new HttpHost("localhost", 9201, "http")));




        //集群client
//
        try {
            Settings settings = Settings.builder().put("cluster.name", "phm").build();
            TransportClient client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("101.37.27.229"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("116.62.52.217"), 9300))
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("116.62.53.46"), 9300));

            client.admin().indices().create(new CreateIndexRequest("newtest")).actionGet();
            PutMappingResponse putMappingResponse = client.admin().indices()
                    .preparePutMapping("newtest")
                    .setType("doc")
                    .setSource(jsonBuilder().prettyPrint()
                        .startObject()
                            .startObject("rawdata").field("type", "string").field("index", "not_analyzed").endObject()
                            .startObject("spectrum").field("type", "string").field("index", "not_analyzed").endObject()
                            .startObject("id").field("type", "string").endObject()
                             .startObject("timestamp").field("type", "integer").endObject()
                            .startObject("health").field("type", "integer").endObject()
                            .startObject("rul").field("type", "integer").endObject()
                            .startObject("RMS").field("type", "integer").endObject()
                            .startObject("VAR").field("type", "integer").endObject()
                            .startObject("peak").field("type", "integer").endObject()
                            .startObject("CrestIndex").field("type", "integer").endObject()
                            .startObject("peakpeak").field("type", "integer").endObject()
                            .startObject("MarginIndex").field("type", "integer").endObject()
                            .startObject("SkewNess").field("type", "integer").endObject()
                            .startObject("SkewnessIndex").field("type", "integer").endObject()
                            .startObject("kurtosis").field("type", "integer").endObject()
                            .startObject("KurtosisIndex").field("type", "integer").endObject()
                            .startObject("InpulseIndex").field("type", "integer").endObject()
                            .startObject("WaveformIndex").field("type", "integer").endObject()
                                    .endObject())
                    .execute().actionGet();






            String raw_data = "";
            String spectrum = "";
            for (int i = 0; i < 1; i++) {
                raw_data = raw_data + "aaaaaaaaaa";
                System.out.println(i);
//            System.out.println(raw_data);
            }
            for (int i = 0; i < 1; i++) {
                spectrum = spectrum + "bbbbbbbbbb";
//            System.out.println(raw_data);

            }


            for (int j = 0; j < BULK_NUM; j++) {
                BulkRequestBuilder request = client.prepareBulk();


                for (int i = 0; i < BULK_SIZE; i++) {
                    Map<String, Object> parseObject = new HashMap<String, Object>();
                    int Max = 10000;
                    int Min = 0;
                    int timestamp = (int) (Math.round(Math.random() * (Max - Min) + Min));
                    int health = (int) (Math.round(Math.random() * (Max - Min) + Min));
                    int rul = (int) (Math.round(Math.random() * (Max - Min) + Min));
                    int RMS = (int) (Math.round(Math.random() * (Max - Min) + Min));
                    int VAR = (int) (Math.round(Math.random() * (Max - Min) + Min));
                    int peak = (int) (Math.round(Math.random() * (Max - Min) + Min));
                    int CrestIndex = (int) (Math.round(Math.random() * (Max - Min) + Min));
                    int peakpeak = (int) (Math.round(Math.random() * (Max - Min) + Min));
                    int MarginIndex = (int) (Math.round(Math.random() * (Max - Min) + Min));
                    int SkewNess = (int) (Math.round(Math.random() * (Max - Min) + Min));
                    int SkewnessIndex = (int) (Math.round(Math.random() * (Max - Min) + Min));
                    int kurtosis = (int) (Math.round(Math.random() * (Max - Min) + Min));
                    int KurtosisIndex = (int) (Math.round(Math.random() * (Max - Min) + Min));
                    int InpulseIndex = (int) (Math.round(Math.random() * (Max - Min) + Min));
                    int WaveformIndex = (int) (Math.round(Math.random() * (Max - Min) + Min));
                    parseObject.put("id", 10000 * j + i);
                    parseObject.put("timestamp", timestamp);
                    parseObject.put("health", health);
                    parseObject.put("rul", rul);
                    parseObject.put("RMS", RMS);
                    parseObject.put("VAR", VAR);
                    parseObject.put("peak", peak);
                    parseObject.put("CrestIndex", CrestIndex);
                    parseObject.put("peakpeak", peakpeak);
                    parseObject.put("MarginIndex", MarginIndex);
                    parseObject.put("SkewNess", SkewNess);
                    parseObject.put("SkewnessIndex", SkewnessIndex);
                    parseObject.put("kurtosis", kurtosis);
                    parseObject.put("KurtosisIndex", KurtosisIndex);
                    parseObject.put("InpulseIndex", InpulseIndex);
                    parseObject.put("WaveformIndex", WaveformIndex);
                    parseObject.put("RawData", raw_data);
                    parseObject.put("Spectrum", spectrum);

                    request.add(new IndexRequest("newtest", "doc")
                            .source(parseObject));
                }

                BulkResponse bulkResponse = request.execute().get();
                System.out.println(j);

            }
                client.close();



        }
        catch (Exception e){
            System.out.println("cluster error!");
            exit(2);
        }

//        for(int i = 0;i < LENGTH; i++){
//
//            parseObject.put("field"+String.valueOf(i), i);
//
//        }


//        String raw_data ="";
//        String spectrum = "";
//        for(int i =0; i<100000; i++){
//            raw_data = raw_data +"aaaaaaaaaa";
//            System.out.println(i);
////            System.out.println(raw_data);
//        }
//        for(int i =0; i<50000; i++){
//            spectrum = spectrum +"bbbbbbbbbb";
////            System.out.println(raw_data);
//
//        }
//
//
//
//        for(int j = 0; j< BULK_NUM;j++) {
//            BulkRequest request = new BulkRequest();
//
//
//            for (int i = 0; i < BULK_SIZE; i++) {
//                Map<String, Object> parseObject = new HashMap<String, Object>();
//                int Max = 10000;
//                int Min = 0;
//                int timestamp=(int)  (Math.round(Math.random()*(Max-Min)+Min));
//                int health = (int)  (Math.round(Math.random()*(Max-Min)+Min));
//                int rul = (int)  (Math.round(Math.random()*(Max-Min)+Min));
//                int RMS = (int)  (Math.round(Math.random()*(Max-Min)+Min));
//                int VAR = (int)  (Math.round(Math.random()*(Max-Min)+Min));
//                int peak = (int)  (Math.round(Math.random()*(Max-Min)+Min));
//                int CrestIndex = (int)  (Math.round(Math.random()*(Max-Min)+Min));
//                int peakpeak = (int)  (Math.round(Math.random()*(Max-Min)+Min));
//                int MarginIndex = (int)  (Math.round(Math.random()*(Max-Min)+Min));
//                int SkewNess = (int)  (Math.round(Math.random()*(Max-Min)+Min));
//                int SkewnessIndex = (int)  (Math.round(Math.random()*(Max-Min)+Min));
//                int kurtosis = (int)  (Math.round(Math.random()*(Max-Min)+Min));
//                int KurtosisIndex = (int)  (Math.round(Math.random()*(Max-Min)+Min));
//                int InpulseIndex = (int)  (Math.round(Math.random()*(Max-Min)+Min));
//                int WaveformIndex = (int)  (Math.round(Math.random()*(Max-Min)+Min));
//                parseObject.put("id", 10000 * j + i );
//                parseObject.put("timestamp",timestamp);
//                parseObject.put("health", health );
//                parseObject.put("rul", rul);
//                parseObject.put("RMS", RMS);
//                parseObject.put("VAR", VAR);
//                parseObject.put("peak", peak);
//                parseObject.put("CrestIndex", CrestIndex);
//                parseObject.put("peakpeak", peakpeak);
//                parseObject.put("MarginIndex", MarginIndex);
//                parseObject.put("SkewNess", SkewNess);
//                parseObject.put("SkewnessIndex", SkewnessIndex);
//                parseObject.put("kurtosis", kurtosis);
//                parseObject.put("KurtosisIndex", KurtosisIndex);
//                parseObject.put("InpulseIndex", InpulseIndex);
//                parseObject.put("WaveformIndex", WaveformIndex);
//                parseObject.put("RawData", raw_data);
//                parseObject.put("Spectrum", spectrum);
//
//                request.add(new IndexRequest("tbea", "doc")
//                        .source(parseObject));
//            }
//            try {
//                BulkResponse bulkResponse = client.bulk(request);
//                System.out.println(j);
//
////                client.close();
//            } catch (IOException a) {
//                System.out.println("Bulk error!");
//                exit(1);
//            }
//        }
//
//        try {
//                            client.close();
//
//        }catch (IOException a){ exit(1);}



        //单个插入实验
//        Map<String, Object> jsonMap = new HashMap<>();
//        for(int i =0;i<LENGTH;i++){
//            jsonMap.put("value"+i, String.valueOf(i) );
//        }
//
//
//
//
//        try {
//
//            for(int i =0 ; i<N;i++) {
//
//                IndexRequest indexRequest = new IndexRequest("posts", "doc", String.valueOf(i))
//                        .source(jsonMap);
//                IndexResponse indexResponse = client.index(indexRequest);
//            }
//            client.close();
////            return indexResponse;
//        }
//        catch (IOException a ) {
//            System.out.println("Error!");
//            exit(1);
//        }






        System.out.println("hello world!");
//        IndexResponse a = new IndexResponse();
//        return a ;


    }
}
