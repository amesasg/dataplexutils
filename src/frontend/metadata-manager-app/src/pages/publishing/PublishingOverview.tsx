import React from 'react';
import {
  Box,
  Typography,
  Paper,
  Grid,
  Card,
  CardContent,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
  Button,
  IconButton,
  LinearProgress,
  Tooltip,
} from '@mui/material';
import CloudUploadIcon from '@mui/icons-material/CloudUpload';
import StorefrontIcon from '@mui/icons-material/Storefront';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import WarningIcon from '@mui/icons-material/Warning';
import VisibilityIcon from '@mui/icons-material/Visibility';
import { useNavigate } from 'react-router-dom';

interface PublishingStatus {
  id: string;
  dataProduct: string;
  domain: string;
  destinations: string[];
  publishedVersion: string;
  lastPublished: string;
  status: 'published' | 'pending' | 'failed';
  marketplaces: string[];
  syncStatus: number;
}

const PublishingOverview: React.FC = () => {
  const navigate = useNavigate();

  // Mock data
  const publishingData: PublishingStatus[] = [
    {
      id: '1',
      dataProduct: 'Customer Analytics Dataset',
      domain: 'Marketing',
      destinations: ['Dataplex Catalog', 'Analytics Hub'],
      publishedVersion: '2.1.0',
      lastPublished: '2024-03-10',
      status: 'published',
      marketplaces: ['Dataplex Catalog', 'Analytics Hub'],
      syncStatus: 100
    },
    {
      id: '2',
      dataProduct: 'Financial Reports',
      domain: 'Finance',
      destinations: ['Dataplex Catalog'],
      publishedVersion: '1.5.0',
      lastPublished: '2024-03-09',
      status: 'pending',
      marketplaces: ['Dataplex Catalog'],
      syncStatus: 65
    },
    {
      id: '3',
      dataProduct: 'Sales Transactions',
      domain: 'Sales',
      destinations: ['Dataplex Catalog', 'Collibra'],
      publishedVersion: '3.0.0',
      lastPublished: '2024-03-08',
      status: 'failed',
      marketplaces: ['Dataplex Catalog', 'Collibra'],
      syncStatus: 45
    }
  ];

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'published':
        return 'success';
      case 'pending':
        return 'warning';
      case 'failed':
        return 'error';
      default:
        return 'default';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'published':
        return <CheckCircleIcon color="success" />;
      case 'pending':
        return <WarningIcon color="warning" />;
      case 'failed':
        return <ErrorIcon color="error" />;
      default:
        return null;
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 4 }}>
        <Typography variant="h4" component="h1">
          Publishing Overview
        </Typography>
        <Button
          variant="contained"
          startIcon={<CloudUploadIcon />}
          onClick={() => navigate('/publishing/new')}
        >
          Publish New Data Product
        </Button>
      </Box>

      {/* Summary Cards */}
      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <CloudUploadIcon color="primary" />
                <Typography color="textSecondary">
                  Published Products
                </Typography>
              </Box>
              <Typography variant="h4">
                {publishingData.filter(d => d.status === 'published').length}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <StorefrontIcon color="primary" />
                <Typography color="textSecondary">
                  Active Marketplaces
                </Typography>
              </Box>
              <Typography variant="h4">
                {new Set(publishingData.flatMap(d => d.marketplaces)).size}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <WarningIcon color="warning" />
                <Typography color="textSecondary">
                  Pending Updates
                </Typography>
              </Box>
              <Typography variant="h4">
                {publishingData.filter(d => d.status === 'pending').length}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <ErrorIcon color="error" />
                <Typography color="textSecondary">
                  Failed Publications
                </Typography>
              </Box>
              <Typography variant="h4">
                {publishingData.filter(d => d.status === 'failed').length}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Publishing Status Table */}
      <Paper sx={{ width: '100%', overflow: 'hidden' }}>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Data Product</TableCell>
                <TableCell>Domain</TableCell>
                <TableCell>Destinations</TableCell>
                <TableCell>Version</TableCell>
                <TableCell>Marketplaces</TableCell>
                <TableCell>Sync Status</TableCell>
                <TableCell>Last Published</TableCell>
                <TableCell>Status</TableCell>
                <TableCell align="center">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {publishingData.map((row) => (
                <TableRow key={row.id}>
                  <TableCell>{row.dataProduct}</TableCell>
                  <TableCell>
                    <Chip label={row.domain} size="small" />
                  </TableCell>
                  <TableCell>
                    <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                      {row.destinations.map((dest) => (
                        <Chip
                          key={dest}
                          label={dest}
                          size="small"
                          variant="outlined"
                        />
                      ))}
                    </Box>
                  </TableCell>
                  <TableCell>{row.publishedVersion}</TableCell>
                  <TableCell>
                    <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                      {row.marketplaces.map((marketplace) => (
                        <Chip
                          key={marketplace}
                          label={marketplace}
                          size="small"
                          variant="outlined"
                          icon={<StorefrontIcon />}
                        />
                      ))}
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <LinearProgress
                        variant="determinate"
                        value={row.syncStatus}
                        color={row.syncStatus === 100 ? 'success' : 'primary'}
                        sx={{ width: 100 }}
                      />
                      <Typography variant="body2">
                        {row.syncStatus}%
                      </Typography>
                    </Box>
                  </TableCell>
                  <TableCell>{row.lastPublished}</TableCell>
                  <TableCell>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      {getStatusIcon(row.status)}
                      <Chip
                        label={row.status}
                        color={getStatusColor(row.status)}
                        size="small"
                      />
                    </Box>
                  </TableCell>
                  <TableCell align="center">
                    <Tooltip title="View Details">
                      <IconButton
                        size="small"
                        onClick={() => navigate(`/publishing/details/${row.id}`)}
                      >
                        <VisibilityIcon />
                      </IconButton>
                    </Tooltip>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </Paper>
    </Box>
  );
};

export default PublishingOverview; 