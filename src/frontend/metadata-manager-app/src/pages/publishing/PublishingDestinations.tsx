import React, { useState } from 'react';
import {
  Box,
  Typography,
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
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Chip,
  Switch,
  FormControlLabel,
  Tooltip,
  Alert,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import StorageIcon from '@mui/icons-material/Storage';
import CloudIcon from '@mui/icons-material/Cloud';
import SettingsIcon from '@mui/icons-material/Settings';
import SyncIcon from '@mui/icons-material/Sync';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';

interface Destination {
  id: string;
  name: string;
  type: 'storage' | 'catalog' | 'marketplace';
  location: string;
  status: 'active' | 'inactive';
  connectedProducts: number;
  lastSync: string;
  syncStatus: 'success' | 'failed';
  settings: {
    autoSync: boolean;
    syncInterval: string;
    metadataSync: boolean;
    dataSync: boolean;
  };
}

const PublishingDestinations: React.FC = () => {
  const [openDialog, setOpenDialog] = useState(false);
  const [selectedDestination, setSelectedDestination] = useState<Destination | null>(null);

  // Mock data
  const destinations: Destination[] = [
    {
      id: '1',
      name: 'Dataplex Catalog',
      type: 'catalog',
      location: 'dataplex.googleapis.com',
      status: 'active',
      connectedProducts: 45,
      lastSync: '2024-03-10 15:30',
      syncStatus: 'success',
      settings: {
        autoSync: true,
        syncInterval: '1h',
        metadataSync: true,
        dataSync: true
      }
    },
    {
      id: '2',
      name: 'Analytics Hub',
      type: 'marketplace',
      location: 'analyticshub.googleapis.com',
      status: 'active',
      connectedProducts: 12,
      lastSync: '2024-03-10 14:45',
      syncStatus: 'success',
      settings: {
        autoSync: true,
        syncInterval: '6h',
        metadataSync: true,
        dataSync: false
      }
    },
    {
      id: '3',
      name: 'Collibra',
      type: 'catalog',
      location: 'collibra.company.com',
      status: 'inactive',
      connectedProducts: 28,
      lastSync: '2024-03-09 12:15',
      syncStatus: 'failed',
      settings: {
        autoSync: false,
        syncInterval: '24h',
        metadataSync: true,
        dataSync: false
      }
    }
  ];

  const handleEdit = (destination: Destination) => {
    setSelectedDestination(destination);
    setOpenDialog(true);
  };

  const getTypeIcon = (type: string) => {
    switch (type) {
      case 'storage':
        return <StorageIcon />;
      case 'catalog':
        return <CloudIcon />;
      case 'marketplace':
        return <StorageIcon />;
      default:
        return <StorageIcon />;
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 4 }}>
        <Typography variant="h4" component="h1">
          Publishing Destinations
        </Typography>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => setOpenDialog(true)}
        >
          Add Destination
        </Button>
      </Box>

      {/* Alert for sync status */}
      {destinations.some(d => d.syncStatus === 'failed') && (
        <Alert severity="warning" sx={{ mb: 3 }}>
          Some destinations have failed sync status. Please check the connection settings.
        </Alert>
      )}

      {/* Destination Cards */}
      <Grid container spacing={3}>
        {destinations.map((destination) => (
          <Grid item xs={12} md={4} key={destination.id}>
            <Card>
              <CardContent>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    {getTypeIcon(destination.type)}
                    <Typography variant="h6">{destination.name}</Typography>
                  </Box>
                  <Chip
                    label={destination.status}
                    color={destination.status === 'active' ? 'success' : 'error'}
                    size="small"
                  />
                </Box>

                <Typography variant="body2" color="textSecondary" gutterBottom>
                  {destination.location}
                </Typography>

                <Box sx={{ mt: 2 }}>
                  <Typography variant="body2">
                    Connected Products: {destination.connectedProducts}
                  </Typography>
                  <Typography variant="body2">
                    Last Sync: {destination.lastSync}
                  </Typography>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 1 }}>
                    <Typography variant="body2">Sync Status:</Typography>
                    {destination.syncStatus === 'success' ? (
                      <CheckCircleIcon color="success" fontSize="small" />
                    ) : (
                      <ErrorIcon color="error" fontSize="small" />
                    )}
                  </Box>
                </Box>

                <Box sx={{ mt: 2 }}>
                  <Typography variant="subtitle2" gutterBottom>
                    Settings:
                  </Typography>
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                    {destination.settings.autoSync && (
                      <Chip
                        label={`Auto-sync: ${destination.settings.syncInterval}`}
                        size="small"
                        variant="outlined"
                      />
                    )}
                    {destination.settings.metadataSync && (
                      <Chip
                        label="Metadata Sync"
                        size="small"
                        variant="outlined"
                      />
                    )}
                    {destination.settings.dataSync && (
                      <Chip
                        label="Data Sync"
                        size="small"
                        variant="outlined"
                      />
                    )}
                  </Box>
                </Box>
              </CardContent>
              <CardActions sx={{ justifyContent: 'flex-end' }}>
                <Tooltip title="Sync Now">
                  <IconButton size="small">
                    <SyncIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip title="Settings">
                  <IconButton size="small">
                    <SettingsIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip title="Edit">
                  <IconButton
                    size="small"
                    onClick={() => handleEdit(destination)}
                  >
                    <EditIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip title="Remove">
                  <IconButton size="small" color="error">
                    <DeleteIcon />
                  </IconButton>
                </Tooltip>
              </CardActions>
            </Card>
          </Grid>
        ))}
      </Grid>

      {/* Add/Edit Destination Dialog */}
      <Dialog
        open={openDialog}
        onClose={() => setOpenDialog(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          {selectedDestination ? 'Edit Destination' : 'Add New Destination'}
        </DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Destination Name"
              fullWidth
              defaultValue={selectedDestination?.name}
            />
            <FormControl fullWidth>
              <InputLabel>Type</InputLabel>
              <Select
                label="Type"
                defaultValue={selectedDestination?.type || 'storage'}
              >
                <MenuItem value="storage">Storage</MenuItem>
                <MenuItem value="catalog">Catalog</MenuItem>
                <MenuItem value="marketplace">Marketplace</MenuItem>
              </Select>
            </FormControl>
            <TextField
              label="Location"
              fullWidth
              defaultValue={selectedDestination?.location}
            />
            <FormControlLabel
              control={
                <Switch
                  defaultChecked={selectedDestination?.status === 'active'}
                />
              }
              label="Active"
            />
            <Typography variant="subtitle2" gutterBottom>
              Sync Settings
            </Typography>
            <FormControlLabel
              control={
                <Switch
                  defaultChecked={selectedDestination?.settings.autoSync}
                />
              }
              label="Auto-sync"
            />
            <FormControl fullWidth>
              <InputLabel>Sync Interval</InputLabel>
              <Select
                label="Sync Interval"
                defaultValue={selectedDestination?.settings.syncInterval || '1h'}
              >
                <MenuItem value="1h">Every Hour</MenuItem>
                <MenuItem value="6h">Every 6 Hours</MenuItem>
                <MenuItem value="12h">Every 12 Hours</MenuItem>
                <MenuItem value="24h">Every 24 Hours</MenuItem>
              </Select>
            </FormControl>
            <FormControlLabel
              control={
                <Switch
                  defaultChecked={selectedDestination?.settings.metadataSync}
                />
              }
              label="Sync Metadata"
            />
            <FormControlLabel
              control={
                <Switch
                  defaultChecked={selectedDestination?.settings.dataSync}
                />
              }
              label="Sync Data"
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenDialog(false)}>Cancel</Button>
          <Button
            variant="contained"
            onClick={() => setOpenDialog(false)}
          >
            {selectedDestination ? 'Save Changes' : 'Add Destination'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default PublishingDestinations; 