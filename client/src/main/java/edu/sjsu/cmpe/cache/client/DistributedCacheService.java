package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.Headers;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;


public class DistributedCacheService implements CacheServiceInterface {
    private final String cacheSerUrl;
    private final Integer startprt;
    private final Integer numserv;
    private final int wrtQuorum;
    private final int rdQuorum;
    private AtomicInteger numbersuccess;
    private AtomicInteger numberattempts;
    private String[] gtVal;
    private AtomicInteger gtIndex;
    private CountDownLatch respWaiter;



    public DistributedCacheService(String serverUrl, Integer startport, Integer numservice) {
        this.wrtQuorum = this.rdQuorum = 2;
        this.numbersuccess = new AtomicInteger(0);
        this.numberattempts = new AtomicInteger(0);
        this.numserv = numservice;
        this.cacheSerUrl = serverUrl;
        this.startprt = startport;
        this.gtVal = new String[this.numserv];
        this.gtIndex = new AtomicInteger(0);
        this.respWaiter = new CountDownLatch(this.numserv);
    }

    
    private String gtReadQuorumString() {

        HashMap<String, Integer> countMap = new HashMap<String, Integer>();
        int maxCount = 1;
        String  retVal = this.gtVal[0];
        for (int j = 0; j < this.gtIndex.get(); j++) {
            if (countMap.containsKey(this.gtVal[j])) {
                int count = countMap.get(this.gtVal[j]).intValue();
                countMap.put(this.gtVal[j], new Integer(count + 1));
                if (count + 1 > maxCount) {
                    retVal = this.gtVal[j];
                    maxCount = count + 1;
                }
            } else {
                countMap.put(this.gtVal[j], 1);
            }
        }
        if (maxCount == this.numserv) {
            
            retVal = null;
        } else if ((maxCount < this.rdQuorum) && (retVal != null)) {
            System.out.println("ReadQuorum is not satisfied");
            
            for (int j = 0; j < this.gtIndex.get(); j++) {
               
                this.gtVal[j] = null;
            }
        }
        return retVal;
    }
    
    @Override
    public String get(long key) {
        String value = null;
        int i = 0;
        while (i < this.numserv.intValue()) {
            Integer port = this.startprt + new Integer(i);

            String cacheUrl = this.cacheSerUrl + ":" + port.toString();
            System.out.println("From server " + cacheUrl);
            Future<HttpResponse<JsonNode>> future = Unirest.get(cacheUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key)).asJsonAsync(new Callback<JsonNode>() {

                        @Override
                        public void completed(HttpResponse<JsonNode> httpResponse) {
                            synchronized (DistributedCacheService.this) {
                                DistributedCacheService.this.numberattempts.getAndIncrement();
                                if (httpResponse.getCode() != 200) {
                                    System.out.println("Failed to get from the cache");
                                } else {
                                    //System.out.println("Success ");
                                    DistributedCacheService.this.numbersuccess.getAndIncrement();
                                    DistributedCacheService.this.gtVal[DistributedCacheService.this.gtIndex.get()] =
                                            httpResponse.getBody().getObject().getString("value");                               
                                    DistributedCacheService.this.gtIndex.getAndIncrement();
                                }
                               
                                respWaiter.countDown();
                            }
                        }

                        @Override
                        public void failed(UnirestException e) {
                            DistributedCacheService.this.numberattempts.getAndIncrement();
                            System.out.println("GET Failed ");
                            respWaiter.countDown();
                        }

                        @Override
                        public void cancelled() {
                            DistributedCacheService.this.numberattempts.getAndIncrement();
                            System.out.println("Cancelled");
                            respWaiter.countDown();
                        }
                    });

            i++;
        }
        try {
            this.respWaiter.await();
        } catch (Exception e) {
            System.out.println("Error" + e);
        }

        String repairString = gtReadQuorumString();
        if (repairString != null) {
            System.out.println("String to repair with: " + repairString);
            put(key, repairString);
        } else {
            repairString = this.gtVal[0];
        }
        return repairString;
    }

    @Override
    public void delete(final long key) {
        HttpResponse<JsonNode> response = null;
        for (int i = 0 ; i < this.numserv.intValue(); i++) {
            Integer port = this.startprt + new Integer(i);

            String cacheUrl = this.cacheSerUrl + ":" + port.toString();
            System.out.println("Deleting from " + cacheUrl);

            Future<HttpResponse<JsonNode>> future = Unirest
                    .delete(cacheUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .asJsonAsync(new Callback<JsonNode>() {

                        @Override
                        public void completed(HttpResponse<JsonNode> httpResponse) {
                            if (httpResponse.getCode() != 204) {
                                System.out.println("Failed to del from the cache.");
                            } else {
                                  DistributedCacheService.this.numbersuccess.getAndIncrement();
                            }
                        }

                        @Override
                        public void failed(UnirestException e) {
                            System.out.println("Failed : " + e);
                        }

                        @Override
                        public void cancelled() {
                            System.out.println("Cancelled");
                        }
                    });

        }
        DistributedCacheService.this.numbersuccess.set(0);
        DistributedCacheService.this.numberattempts.set(0);
    }
    
    @Override
    public void put(final long key, final String value) {
        HttpResponse<JsonNode> response = null;
        for (int i = 0 ; i < this.numserv.intValue(); i++) {
            Integer port = this.startprt + new Integer(i);

            String cacheUrl = this.cacheSerUrl + ":" + port.toString();
            System.out.println("Putting to " + cacheUrl);

            Future<HttpResponse<JsonNode>> future = Unirest
                    .put(cacheUrl + "/cache/{key}/{value}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .routeParam("value", value).asJsonAsync(new Callback<JsonNode>() {
                        private void checkForSuccess() {
                            int flag = 0;
                           
                            if (DistributedCacheService.this.numberattempts.get() == DistributedCacheService.this.numserv) {
                                if (DistributedCacheService.this.numbersuccess.get() >= DistributedCacheService.this.wrtQuorum) {
                                    
                                } else {
                                    flag = 1;
                                    
                                }

                                DistributedCacheService.this.numberattempts.set(0);
                                DistributedCacheService.this.numbersuccess.set(0);
                                if (flag == 1) {
                                    DistributedCacheService.this.delete(key);
                                }
                            }
                        }

                        @Override
                        public void completed(HttpResponse<JsonNode> httpResponse) {
                            synchronized (DistributedCacheService.this) {
                                DistributedCacheService.this.numberattempts.getAndIncrement();
                                if (httpResponse.getCode() != 200) {
                                    System.out.println("Failed to add to the cache.");
                                } else {
                                    DistributedCacheService.this.numbersuccess.getAndIncrement();
                                }
                                checkForSuccess();
                            }
                        }

                        @Override
                        public void failed(UnirestException e) {
                            DistributedCacheService.this.numberattempts.getAndIncrement();
                            System.out.println("Failed to PUT data.");
                            checkForSuccess();
                        }

                        @Override
                        public void cancelled() {
                            DistributedCacheService.this.numberattempts.getAndIncrement();
                            System.out.println("Cancelled");
                            checkForSuccess();
                        }
                    });

        }
    }
}
