import React, { useState } from 'react';
import {
  Box,
  Card,
  CardContent,
  Typography,
  TextField,
  Button,
  Alert,
  Grid,
} from '@mui/material';
import { DatplexConfig } from '../App';

interface ConfigurationPageProps {
  config: DatplexConfig;
  onConfigChange: (config: DatplexConfig) => void;
}

const ConfigurationPage: React.FC<ConfigurationPageProps> = ({ config, onConfigChange }) => {
  const [formData, setFormData] = useState<DatplexConfig>(config);
  const [saveStatus, setSaveStatus] = useState<'success' | 'error' | null>(null);

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const handleSave = () => {
    try {
      onConfigChange(formData);
      setSaveStatus('success');
      setTimeout(() => setSaveStatus(null), 3000);
    } catch (error) {
      setSaveStatus('error');
    }
  };

  return (
    <Box>
      <Typography variant="h1" gutterBottom>
        Configuration
      </Typography>

      <Grid container spacing={3}>
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Typography variant="h6" gutterBottom>
                Dataplex Settings
              </Typography>
              
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <TextField
                  fullWidth
                  label="Project ID"
                  name="project_id"
                  value={formData.project_id}
                  onChange={handleInputChange}
                  helperText="The Google Cloud project ID where your Dataplex resources are located"
                />

                <TextField
                  fullWidth
                  label="LLM Location"
                  name="llm_location"
                  value={formData.llm_location}
                  onChange={handleInputChange}
                  helperText="The location of the Language Model service"
                />

                <TextField
                  fullWidth
                  label="Dataplex Location"
                  name="dataplex_location"
                  value={formData.dataplex_location}
                  onChange={handleInputChange}
                  helperText="The location of your Dataplex instance"
                />

                <Button
                  variant="contained"
                  onClick={handleSave}
                  sx={{ alignSelf: 'flex-start', mt: 2 }}
                >
                  Save Configuration
                </Button>

                {saveStatus === 'success' && (
                  <Alert severity="success">
                    Configuration saved successfully
                  </Alert>
                )}

                {saveStatus === 'error' && (
                  <Alert severity="error">
                    Failed to save configuration
                  </Alert>
                )}
              </Box>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

export default ConfigurationPage; 