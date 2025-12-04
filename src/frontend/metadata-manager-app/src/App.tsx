import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { Box, CssBaseline, ThemeProvider } from '@mui/material';
import { createTheme } from '@mui/material/styles';
import TopNavBar from './components/navigation/TopNavBar';
import LeftMenu from './components/navigation/LeftMenu';
import ReviewPage from './pages/ReviewPage';
import GenerationPage from './pages/GenerationPage';
import ConfigurationPage from './pages/ConfigurationPage';
import { Task } from './components/TaskTracker';
import { CacheProvider } from './contexts/CacheContext';
import { ReviewProvider } from './contexts/ReviewContext';

export interface DatplexConfig {
  // Client Settings
  project_id: string;
  llm_location: string;
  dataplex_location: string;

  // Client Options Settings
  use_lineage_tables: boolean;
  use_lineage_processes: boolean;
  use_profile: boolean;
  use_data_quality: boolean;
  use_ext_documents: boolean;
  persist_to_dataplex_catalog: boolean;
  stage_for_review: boolean;
  top_values_in_description: boolean;
  description_handling: string;
  description_prefix: string;

  // Table Settings
  dataset_id?: string;
  table_id: string;
  documentation_uri: string;

  // Dataset Settings
  documentation_csv_uri: string;
  strategy: string;
}

function App() {
  const [dataplexConfig, setDataplexConfig] = useState<DatplexConfig>({
    // Client Settings
    project_id: process.env.REACT_APP_PROJECT_ID || '',
    llm_location: process.env.REACT_APP_LLM_LOCATION || '',
    dataplex_location: process.env.REACT_APP_DATAPLEX_LOCATION || '',

    // Client Options Settings
    use_lineage_tables: false,
    use_lineage_processes: false,
    use_profile: false,
    use_data_quality: false,
    use_ext_documents: false,
    persist_to_dataplex_catalog: true,
    stage_for_review: false,
    top_values_in_description: true,
    description_handling: 'append',
    description_prefix: '---AI Generated description---',

    // Table Settings
    dataset_id: '',
    table_id: '',
    documentation_uri: '',

    // Dataset Settings
    documentation_csv_uri: '',
    strategy: 'NAIVE'
  });

  const [tasks, setTasks] = useState<Task[]>([]);

  const handleTaskAdd = (task: Task) => {
    setTasks([...tasks, task]);
  };

  const handleTaskUpdate = (taskId: string, updates: Partial<Task>) => {
    setTasks(tasks.map(task => 
      task.id === taskId ? { ...task, ...updates } : task
    ));
  };

  const theme = createTheme({
    palette: {
      mode: 'light',
    },
    typography: {
      h1: {
        fontSize: '2rem',
        fontWeight: 500,
        marginBottom: '1rem'
      },
      h2: {
        fontSize: '1.5rem',
        fontWeight: 500,
        marginBottom: '0.5rem'
      },
      h3: {
        fontSize: '1.25rem',
        fontWeight: 500
      },
      h4: {
        fontSize: '1.1rem',
        fontWeight: 500
      },
      h5: {
        fontSize: '1rem',
        fontWeight: 500
      },
      h6: {
        fontSize: '0.875rem',
        fontWeight: 500
      },
      body1: {
        fontSize: '0.875rem'
      },
      body2: {
        fontSize: '0.75rem'
      }
    },
    components: {
      MuiButton: {
        styleOverrides: {
          root: {
            textTransform: 'none',
            fontSize: '0.875rem'
          }
        }
      },
      MuiTextField: {
        defaultProps: {
          size: 'small'
        }
      },
      MuiFormControl: {
        defaultProps: {
          size: 'small'
        }
      }
    }
  });

  return (
    <CacheProvider>
      <ReviewProvider>
        <ThemeProvider theme={theme}>
          <Router>
            <Box sx={{ display: 'flex' }}>
              <CssBaseline />
              <TopNavBar tasks={tasks} />
              <LeftMenu />
              <Box component="main" sx={{ flexGrow: 1, p: 3, mt: 8 }}>
                <Routes>
                  <Route 
                    path="/" 
                    element={
                      <GenerationPage 
                        config={dataplexConfig}
                        onTaskAdd={handleTaskAdd}
                        onTaskUpdate={handleTaskUpdate}
                        onConfigChange={setDataplexConfig}
                      />
                    } 
                  />
                  <Route 
                    path="/review" 
                    element={<ReviewPage config={dataplexConfig} />} 
                  />
                  <Route 
                    path="/configuration" 
                    element={
                      <ConfigurationPage 
                        config={dataplexConfig} 
                        onConfigChange={setDataplexConfig}
                      />
                    } 
                  />
                </Routes>
              </Box>
            </Box>
          </Router>
        </ThemeProvider>
      </ReviewProvider>
    </CacheProvider>
  );
}

export default App;
