import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { useCache } from '../../contexts/CacheContext';
import { useReview } from '../../contexts/ReviewContext';
import { Box } from '@mui/material';

interface ReviewPageProps {
  config: {
    project_id: string;
    llm_location: string;
    dataplex_location: string;
  };
}

const ReviewPage: React.FC<ReviewPageProps> = ({ config }) => {
  const { detailsCache, setDetailsCache } = useCache();
  const { state, dispatch } = useReview();
  const { items = [], currentItemIndex, viewMode } = state;
  const [apiUrlBase, setApiUrlBase] = useState<string>('');
  const [error, setError] = useState<string | null>(null);
  const [isLoadingDetails, setIsLoadingDetails] = useState(false);

  // Initialize API URL
  useEffect(() => {
    const apiUrl = process.env.REACT_APP_API_URL || 'http://localhost:8000';
    setApiUrlBase(apiUrl);
  }, []);

  const loadItemDetails = async (itemId: string, setLoading = true) => {
    try {
      if (setLoading) {
        setIsLoadingDetails(true);
      }
      setError(null);

      // Use cache if available and not explicitly refreshing
      if (detailsCache[itemId] && !setLoading) {
        console.log('Using cached item details:', detailsCache[itemId]);
        dispatch({
          type: 'UPDATE_ITEM',
          payload: {
            id: itemId,
            updates: detailsCache[itemId]
          }
        });
        return detailsCache[itemId];
      }

      const [projectId, datasetId, tableId] = itemId.split('#')[0].split('.');

      console.log(`${setLoading ? 'Fetching' : 'Preloading'} details for item:`, itemId);
      const response = await axios.post(`${apiUrlBase}/metadata/review/details`, {
        client_settings: {
          project_id: config.project_id,
          llm_location: config.llm_location,
          dataplex_location: config.dataplex_location,
        },
        table_settings: {
          project_id: projectId,
          dataset_id: datasetId,
          table_id: tableId,
        }
      });

      const details = response.data;
      console.log(`${setLoading ? 'Received' : 'Preloaded'} item details:`, details);

      const updatedDetails = {
        ...details,
        'to-be-regenerated': details.markedForRegeneration,
        lastModified: details.lastModified || new Date().toISOString()
      };

      // Update the item in state
      dispatch({
        type: 'UPDATE_ITEM',
        payload: {
          id: itemId,
          updates: updatedDetails
        }
      });

      // Cache the details
      setDetailsCache({
        ...detailsCache,
        [itemId]: updatedDetails
      });

      return updatedDetails;

    } catch (error: any) {
      const errorMessage = error.response?.data?.detail || 'Failed to load item details';
      if (setLoading) {
        console.error('Error loading item details:', error);
        setError(errorMessage);
      }
      return null;
    } finally {
      if (setLoading) {
        setIsLoadingDetails(false);
      }
    }
  };

  const preloadNextItem = async (currentIndex: number) => {
    if (currentIndex < items.length - 1) {
      const nextItemIndex = currentIndex + 1;
      const nextItem = items[nextItemIndex];
      
      // Only preload if not already in cache
      if (!detailsCache[nextItem.id]) {
        console.log('Preloading next item:', nextItem.id);
        try {
          await loadItemDetails(nextItem.id, false);
          console.log('Successfully preloaded next item:', nextItem.id);
        } catch (error) {
          console.error('Error preloading next item:', error);
        }
      }
    }
  };

  const handleViewDetails = async (itemId: string) => {
    const itemIndex = items.findIndex(item => item.id === itemId);
    if (itemIndex !== -1) {
      dispatch({ type: 'SET_CURRENT_INDEX', payload: itemIndex });
      dispatch({ type: 'SET_VIEW_MODE', payload: 'review' });
      
      try {
        // Only load details if not in cache
        if (!detailsCache[itemId]) {
          await loadItemDetails(itemId);
        } else {
          // Use cached data
          dispatch({
            type: 'UPDATE_ITEM',
            payload: {
              id: itemId,
              updates: detailsCache[itemId]
            }
          });
        }
        
        // Preload next item if there is one
        if (itemIndex < items.length - 1) {
          await preloadNextItem(itemIndex);
        }
      } catch (error) {
        console.error('Error loading item details:', error);
      }
    }
  };

  const handleNext = async () => {
    if (currentItemIndex < items.length - 1) {
      const nextIndex = currentItemIndex + 1;
      dispatch({ type: 'SET_CURRENT_INDEX', payload: nextIndex });
      
      // Use cached data if available
      if (detailsCache[items[nextIndex].id]) {
        dispatch({
          type: 'UPDATE_ITEM',
          payload: {
            id: items[nextIndex].id,
            updates: detailsCache[items[nextIndex].id]
          }
        });
      } else {
        await loadItemDetails(items[nextIndex].id);
      }
      
      // Preload the next item if there is one
      if (nextIndex < items.length - 1) {
        await preloadNextItem(nextIndex);
      }
    }
  };

  const handlePrevious = async () => {
    if (currentItemIndex > 0) {
      const prevIndex = currentItemIndex - 1;
      dispatch({ type: 'SET_CURRENT_INDEX', payload: prevIndex });
      
      // Use cached data if available
      if (detailsCache[items[prevIndex].id]) {
        dispatch({
          type: 'UPDATE_ITEM',
          payload: {
            id: items[prevIndex].id,
            updates: detailsCache[items[prevIndex].id]
          }
        });
      } else {
        await loadItemDetails(items[prevIndex].id);
      }
      
      // Preload the next item (current + 1) if we're not at the end
      if (currentItemIndex < items.length - 1) {
        await preloadNextItem(currentItemIndex);
      }
    }
  };

  return (
    <Box>
      {/* Your existing JSX */}
      {viewMode === 'list' ? (
        <div>List View</div>
      ) : (
        <div>Review View</div>
      )}
    </Box>
  );
};

export default ReviewPage; 