package edu.sjsu.cmpe.cache.repository;

import java.util.List;
import edu.sjsu.cmpe.cache.domain.Entry;


public interface CacheInterface {
    
    Entry save(Entry newEntry);    
    Entry get(Long key);
    void delete(Long key);
    List<Entry> getAll();

}
