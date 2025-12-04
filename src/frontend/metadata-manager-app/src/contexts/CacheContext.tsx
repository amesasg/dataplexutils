import React, { createContext, useContext, useState, useEffect } from 'react';

const CACHE_KEY = 'metadata_details_cache';
const CACHE_TIMESTAMP_KEY = 'metadata_cache_timestamp';
const CACHE_EXPIRY_TIME = 30 * 60 * 1000; // 30 minutes in milliseconds

interface CacheContextType {
  detailsCache: Record<string, any>;
  setDetailsCache: (cache: Record<string, any>) => void;
  clearCache: () => void;
}

const CacheContext = createContext<CacheContextType | undefined>(undefined);

export const CacheProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const loadCacheFromStorage = () => {
    try {
      const cachedData = localStorage.getItem(CACHE_KEY);
      const timestamp = localStorage.getItem(CACHE_TIMESTAMP_KEY);
      
      if (cachedData && timestamp) {
        const now = new Date().getTime();
        const cacheTime = parseInt(timestamp);
        
        if (now - cacheTime < CACHE_EXPIRY_TIME) {
          return JSON.parse(cachedData);
        } else {
          localStorage.removeItem(CACHE_KEY);
          localStorage.removeItem(CACHE_TIMESTAMP_KEY);
        }
      }
    } catch (error) {
      console.error('Error loading cache:', error);
    }
    return {};
  };

  const [detailsCache, setDetailsCache] = useState<Record<string, any>>(() => loadCacheFromStorage());

  useEffect(() => {
    try {
      localStorage.setItem(CACHE_KEY, JSON.stringify(detailsCache));
      localStorage.setItem(CACHE_TIMESTAMP_KEY, new Date().getTime().toString());
    } catch (error) {
      console.error('Error saving cache:', error);
    }
  }, [detailsCache]);

  const clearCache = () => {
    setDetailsCache({});
    localStorage.removeItem(CACHE_KEY);
    localStorage.removeItem(CACHE_TIMESTAMP_KEY);
  };

  return (
    <CacheContext.Provider value={{ detailsCache, setDetailsCache, clearCache }}>
      {children}
    </CacheContext.Provider>
  );
};

export const useCache = () => {
  const context = useContext(CacheContext);
  if (context === undefined) {
    throw new Error('useCache must be used within a CacheProvider');
  }
  return context;
}; 