import React, { useState } from 'react';
import {
  Box,
  Typography,
  Paper,
  Grid,
  Card,
  CardContent,
  Button,
  TextField,
  Autocomplete,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Chip,
  IconButton,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  Divider,
  Alert,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import TimelineIcon from '@mui/icons-material/Timeline';

interface Dependency {
  id: string;
  name: string;
  type: string;
  sla: {
    availability: number;
    latency: number;
    freshness: number;
  };
}

interface SLARequirement {
  metric: string;
  threshold: number;
  priority: 'High' | 'Medium' | 'Low';
}

const SLAGenerator: React.FC = () => {
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null);
  const [dependencies, setDependencies] = useState<Dependency[]>([]);
  const [openAddDependency, setOpenAddDependency] = useState(false);
  const [requirements, setRequirements] = useState<SLARequirement[]>([]);
  const [openAddRequirement, setOpenAddRequirement] = useState(false);
  const [calculatedSLA, setCalculatedSLA] = useState<any>(null);

  // Mock data for demonstration
  const availableDatasets = [
    'Customer Analytics Dataset',
    'Sales Transactions',
    'Product Inventory',
    'User Behavior Data',
  ];

  const availableDependencies = [
    {
      id: '1',
      name: 'Raw Customer Data',
      type: 'Dataset',
      sla: {
        availability: 99.9,
        latency: 5,
        freshness: 15,
      },
    },
    {
      id: '2',
      name: 'Payment Processing',
      type: 'Service',
      sla: {
        availability: 99.99,
        latency: 1,
        freshness: 1,
      },
    },
  ];

  const handleAddDependency = (dependency: Dependency) => {
    setDependencies([...dependencies, dependency]);
    setOpenAddDependency(false);
  };

  const handleRemoveDependency = (id: string) => {
    setDependencies(dependencies.filter(dep => dep.id !== id));
  };

  const handleAddRequirement = (requirement: SLARequirement) => {
    setRequirements([...requirements, requirement]);
    setOpenAddRequirement(false);
  };

  const handleRemoveRequirement = (index: number) => {
    setRequirements(requirements.filter((_, i) => i !== index));
  };

  const calculateSLA = () => {
    // Simple SLA calculation based on dependencies
    const availability = dependencies.reduce((min, dep) => 
      Math.min(min, dep.sla.availability), 100);
    
    const maxLatency = dependencies.reduce((sum, dep) => 
      sum + dep.sla.latency, 0);
    
    const maxFreshness = dependencies.reduce((max, dep) => 
      Math.max(max, dep.sla.freshness), 0);

    setCalculatedSLA({
      availability,
      latency: maxLatency,
      freshness: maxFreshness,
    });
  };

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 4 }}>
        <Typography variant="h4" component="h1">
          SLA Generator
        </Typography>
        <Button
          variant="contained"
          color="primary"
          onClick={calculateSLA}
          startIcon={<TimelineIcon />}
          disabled={!selectedDataset || dependencies.length === 0}
        >
          Calculate SLA
        </Button>
      </Box>

      {/* Dataset Selection */}
      <Paper sx={{ p: 3, mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          Target Dataset
        </Typography>
        <Autocomplete
          value={selectedDataset}
          onChange={(_, newValue) => setSelectedDataset(newValue)}
          options={availableDatasets}
          renderInput={(params) => (
            <TextField {...params} label="Select Dataset" variant="outlined" />
          )}
          sx={{ maxWidth: 400 }}
        />
      </Paper>

      {/* Dependencies Section */}
      <Grid container spacing={3}>
        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 3, height: '100%' }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
              <Typography variant="h6">
                Dependencies
              </Typography>
              <Button
                startIcon={<AddIcon />}
                onClick={() => setOpenAddDependency(true)}
              >
                Add Dependency
              </Button>
            </Box>
            <List>
              {dependencies.map((dep) => (
                <React.Fragment key={dep.id}>
                  <ListItem>
                    <ListItemText
                      primary={dep.name}
                      secondary={
                        <Box sx={{ mt: 1 }}>
                          <Chip
                            label={`Availability: ${dep.sla.availability}%`}
                            size="small"
                            sx={{ mr: 1 }}
                          />
                          <Chip
                            label={`Latency: ${dep.sla.latency}min`}
                            size="small"
                            sx={{ mr: 1 }}
                          />
                          <Chip
                            label={`Freshness: ${dep.sla.freshness}min`}
                            size="small"
                          />
                        </Box>
                      }
                    />
                    <ListItemSecondaryAction>
                      <IconButton
                        edge="end"
                        onClick={() => handleRemoveDependency(dep.id)}
                      >
                        <DeleteIcon />
                      </IconButton>
                    </ListItemSecondaryAction>
                  </ListItem>
                  <Divider />
                </React.Fragment>
              ))}
            </List>
          </Paper>
        </Grid>

        <Grid item xs={12} md={6}>
          <Paper sx={{ p: 3, height: '100%' }}>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
              <Typography variant="h6">
                Custom Requirements
              </Typography>
              <Button
                startIcon={<AddIcon />}
                onClick={() => setOpenAddRequirement(true)}
              >
                Add Requirement
              </Button>
            </Box>
            <List>
              {requirements.map((req, index) => (
                <React.Fragment key={index}>
                  <ListItem>
                    <ListItemText
                      primary={req.metric}
                      secondary={`Threshold: ${req.threshold} | Priority: ${req.priority}`}
                    />
                    <ListItemSecondaryAction>
                      <IconButton
                        edge="end"
                        onClick={() => handleRemoveRequirement(index)}
                      >
                        <DeleteIcon />
                      </IconButton>
                    </ListItemSecondaryAction>
                  </ListItem>
                  <Divider />
                </React.Fragment>
              ))}
            </List>
          </Paper>
        </Grid>
      </Grid>

      {/* Calculated SLA Display */}
      {calculatedSLA && (
        <Paper sx={{ p: 3, mt: 3 }}>
          <Typography variant="h6" gutterBottom>
            Calculated SLA
          </Typography>
          <Grid container spacing={3}>
            <Grid item xs={12} md={4}>
              <Card>
                <CardContent>
                  <Typography color="textSecondary" gutterBottom>
                    Availability
                  </Typography>
                  <Typography variant="h4">
                    {calculatedSLA.availability.toFixed(2)}%
                  </Typography>
                </CardContent>
              </Card>
            </Grid>
            <Grid item xs={12} md={4}>
              <Card>
                <CardContent>
                  <Typography color="textSecondary" gutterBottom>
                    Maximum Latency
                  </Typography>
                  <Typography variant="h4">
                    {calculatedSLA.latency} min
                  </Typography>
                </CardContent>
              </Card>
            </Grid>
            <Grid item xs={12} md={4}>
              <Card>
                <CardContent>
                  <Typography color="textSecondary" gutterBottom>
                    Data Freshness
                  </Typography>
                  <Typography variant="h4">
                    {calculatedSLA.freshness} min
                  </Typography>
                </CardContent>
              </Card>
            </Grid>
          </Grid>
          <Alert severity="info" sx={{ mt: 2 }}>
            These SLA values are calculated based on the dependencies' SLAs and custom requirements.
            Review and adjust as needed before finalizing.
          </Alert>
        </Paper>
      )}

      {/* Add Dependency Dialog */}
      <Dialog
        open={openAddDependency}
        onClose={() => setOpenAddDependency(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>Add Dependency</DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Autocomplete
              options={availableDependencies}
              getOptionLabel={(option) => option.name}
              renderInput={(params) => (
                <TextField {...params} label="Select Dependency" />
              )}
              onChange={(_, value) => value && handleAddDependency(value)}
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenAddDependency(false)}>Cancel</Button>
        </DialogActions>
      </Dialog>

      {/* Add Requirement Dialog */}
      <Dialog
        open={openAddRequirement}
        onClose={() => setOpenAddRequirement(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>Add Custom Requirement</DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField label="Metric Name" fullWidth />
            <TextField
              label="Threshold"
              type="number"
              fullWidth
              InputProps={{ inputProps: { min: 0, step: 0.1 } }}
            />
            <FormControl fullWidth>
              <InputLabel>Priority</InputLabel>
              <Select label="Priority" defaultValue="Medium">
                <MenuItem value="High">High</MenuItem>
                <MenuItem value="Medium">Medium</MenuItem>
                <MenuItem value="Low">Low</MenuItem>
              </Select>
            </FormControl>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenAddRequirement(false)}>Cancel</Button>
          <Button
            variant="contained"
            onClick={() =>
              handleAddRequirement({
                metric: 'Custom Metric',
                threshold: 99.9,
                priority: 'Medium',
              })
            }
          >
            Add Requirement
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default SLAGenerator; 