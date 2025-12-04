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
  Chip,
  Tooltip,
  Alert,
} from '@mui/material';
import StorefrontIcon from '@mui/icons-material/Storefront';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';
import CloudSyncIcon from '@mui/icons-material/CloudSync';
import SecurityIcon from '@mui/icons-material/Security';
import SettingsIcon from '@mui/icons-material/Settings';

interface Marketplace {
  id: string;
  name: string;
  type: 'internal' | 'partner' | 'public';
  url: string;
  status: 'active' | 'inactive';
  publishedProducts: number;
  lastSync: string;
  features: string[];
  requiresAuth: boolean;
}

const Marketplaces: React.FC = () => {
  const [openDialog, setOpenDialog] = useState(false);
  const [selectedMarketplace, setSelectedMarketplace] = useState<Marketplace | null>(null);

  // Mock data
  const marketplaces: Marketplace[] = [
    {
      id: '1',
      name: 'Dataplex Catalog',
      type: 'internal',
      url: 'dataplex.googleapis.com',
      status: 'active',
      publishedProducts: 45,
      lastSync: '2024-03-10 15:30',
      features: ['Metadata Sync', 'Access Control', 'Usage Analytics'],
      requiresAuth: true
    },
    {
      id: '2',
      name: 'Analytics Hub',
      type: 'partner',
      url: 'analyticshub.googleapis.com',
      status: 'active',
      publishedProducts: 12,
      lastSync: '2024-03-10 14:45',
      features: ['Limited Access', 'Partner API', 'Usage Tracking'],
      requiresAuth: true
    },
    {
      id: '3',
      name: 'Collibra',
      type: 'public',
      url: 'collibra.company.com',
      status: 'inactive',
      publishedProducts: 8,
      lastSync: '2024-03-09 16:20',
      features: ['Public Access', 'Basic Analytics'],
      requiresAuth: false
    }
  ];

  const handleEdit = (marketplace: Marketplace) => {
    setSelectedMarketplace(marketplace);
    setOpenDialog(true);
  };

  const getTypeChipColor = (type: string) => {
    switch (type) {
      case 'internal':
        return 'primary';
      case 'partner':
        return 'secondary';
      case 'public':
        return 'success';
      default:
        return undefined;
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 4 }}>
        <Typography variant="h4" component="h1">
          Data Marketplaces
        </Typography>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => setOpenDialog(true)}
        >
          Add Marketplace
        </Button>
      </Box>

      {/* Alert for marketplace sync status */}
      <Alert severity="info" sx={{ mb: 3 }}>
        All marketplaces are synced and up to date. Last sync check: 5 minutes ago.
      </Alert>

      {/* Marketplace Cards */}
      <Grid container spacing={3}>
        {marketplaces.map((marketplace) => (
          <Grid item xs={12} md={4} key={marketplace.id}>
            <Card>
              <CardContent>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                  <StorefrontIcon />
                  <Typography variant="h6">{marketplace.name}</Typography>
                </Box>

                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                  <Chip
                    label={marketplace.type}
                    color={getTypeChipColor(marketplace.type)}
                    size="small"
                    sx={{ textTransform: 'lowercase' }}
                  />
                  {marketplace.requiresAuth && (
                    <SecurityIcon fontSize="small" color="action" />
                  )}
                  <Chip
                    label={marketplace.status}
                    color={marketplace.status === 'active' ? 'success' : 'error'}
                    size="small"
                    sx={{ ml: 'auto' }}
                  />
                </Box>

                <Typography variant="body2" color="textSecondary" gutterBottom>
                  {marketplace.url}
                </Typography>

                <Box sx={{ mt: 2 }}>
                  <Typography variant="body2">
                    Published Products: {marketplace.publishedProducts}
                  </Typography>
                  <Typography variant="body2">
                    Last Sync: {marketplace.lastSync}
                  </Typography>
                </Box>

                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 2 }}>
                  {marketplace.features.map((feature) => (
                    <Chip
                      key={feature}
                      label={feature}
                      size="small"
                      variant="outlined"
                    />
                  ))}
                </Box>
              </CardContent>
              <CardActions sx={{ justifyContent: 'flex-end' }}>
                <Tooltip title="Sync Now">
                  <IconButton size="small">
                    <CloudSyncIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip title="Settings">
                  <IconButton size="small">
                    <SettingsIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip title="Edit">
                  <IconButton size="small">
                    <EditIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip title="Remove">
                  <IconButton size="small">
                    <DeleteIcon />
                  </IconButton>
                </Tooltip>
              </CardActions>
            </Card>
          </Grid>
        ))}
      </Grid>

      {/* Add/Edit Marketplace Dialog */}
      <Dialog
        open={openDialog}
        onClose={() => setOpenDialog(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          {selectedMarketplace ? 'Edit Marketplace' : 'Add New Marketplace'}
        </DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Marketplace Name"
              fullWidth
              defaultValue={selectedMarketplace?.name}
            />
            <TextField
              label="URL"
              fullWidth
              defaultValue={selectedMarketplace?.url}
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenDialog(false)}>Cancel</Button>
          <Button
            variant="contained"
            onClick={() => setOpenDialog(false)}
          >
            {selectedMarketplace ? 'Save Changes' : 'Add Marketplace'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default Marketplaces; 