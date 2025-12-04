import React, { useState } from 'react';
import {
  Box,
  Typography,
  Paper,
  Grid,
  Card,
  CardContent,
  CardActions,
  Button,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Chip,
  Divider,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import EditIcon from '@mui/icons-material/Edit';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import DeleteIcon from '@mui/icons-material/Delete';

const ContractTemplates: React.FC = () => {
  const [openDialog, setOpenDialog] = useState(false);
  const [selectedTemplate, setSelectedTemplate] = useState<any>(null);

  // Mock data for demonstration
  const templates = [
    {
      id: 1,
      name: 'Basic Data Quality',
      description: 'Standard data quality requirements including completeness and accuracy metrics',
      tags: ['Quality', 'Basic'],
      lastModified: '2024-03-10',
      version: '1.0'
    },
    {
      id: 2,
      name: 'Real-time Data SLA',
      description: 'Requirements for real-time data processing and delivery',
      tags: ['Real-time', 'SLA', 'Performance'],
      lastModified: '2024-03-09',
      version: '2.1'
    },
    {
      id: 3,
      name: 'Compliance Requirements',
      description: 'Data handling and privacy compliance requirements',
      tags: ['Compliance', 'Privacy', 'GDPR'],
      lastModified: '2024-03-08',
      version: '1.2'
    }
  ];

  const handleOpenDialog = (template?: any) => {
    setSelectedTemplate(template || null);
    setOpenDialog(true);
  };

  const handleCloseDialog = () => {
    setSelectedTemplate(null);
    setOpenDialog(false);
  };

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 4 }}>
        <Typography variant="h4" component="h1">
          Contract Templates
        </Typography>
        <Button
          variant="contained"
          color="primary"
          startIcon={<AddIcon />}
          onClick={() => handleOpenDialog()}
        >
          Create Template
        </Button>
      </Box>

      <Grid container spacing={3}>
        {templates.map((template) => (
          <Grid item xs={12} md={6} lg={4} key={template.id}>
            <Card>
              <CardContent>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
                  <Typography variant="h6" component="div">
                    {template.name}
                  </Typography>
                  <Typography variant="caption" color="textSecondary">
                    v{template.version}
                  </Typography>
                </Box>
                <Typography color="textSecondary" sx={{ mb: 2 }}>
                  {template.description}
                </Typography>
                <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap', mb: 2 }}>
                  {template.tags.map((tag) => (
                    <Chip key={tag} label={tag} size="small" />
                  ))}
                </Box>
                <Typography variant="caption" color="textSecondary">
                  Last modified: {template.lastModified}
                </Typography>
              </CardContent>
              <Divider />
              <CardActions>
                <IconButton size="small" onClick={() => handleOpenDialog(template)}>
                  <EditIcon />
                </IconButton>
                <IconButton size="small">
                  <ContentCopyIcon />
                </IconButton>
                <IconButton size="small" color="error">
                  <DeleteIcon />
                </IconButton>
              </CardActions>
            </Card>
          </Grid>
        ))}
      </Grid>

      {/* Template Editor Dialog */}
      <Dialog open={openDialog} onClose={handleCloseDialog} maxWidth="md" fullWidth>
        <DialogTitle>
          {selectedTemplate ? 'Edit Template' : 'Create New Template'}
        </DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Template Name"
              fullWidth
              defaultValue={selectedTemplate?.name}
            />
            <TextField
              label="Description"
              fullWidth
              multiline
              rows={4}
              defaultValue={selectedTemplate?.description}
            />
            <TextField
              label="Version"
              defaultValue={selectedTemplate?.version || '1.0'}
            />
            <TextField
              label="Tags"
              fullWidth
              helperText="Enter tags separated by commas"
              defaultValue={selectedTemplate?.tags.join(', ')}
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseDialog}>Cancel</Button>
          <Button variant="contained" color="primary">
            {selectedTemplate ? 'Save Changes' : 'Create Template'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default ContractTemplates; 