import React from 'react';
import {
  Box,
  Typography,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
  IconButton,
  Tooltip,
  Button,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
} from '@mui/material';
import VisibilityIcon from '@mui/icons-material/Visibility';
import FilterListIcon from '@mui/icons-material/FilterList';
import RestoreIcon from '@mui/icons-material/Restore';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import WarningIcon from '@mui/icons-material/Warning';

interface PublishingEvent {
  id: string;
  dataProduct: string;
  version: string;
  destination: string;
  publisher: string;
  timestamp: string;
  status: 'success' | 'failed' | 'partial';
  details: string;
}

const PublishingHistory: React.FC = () => {
  // Mock data
  const publishingEvents: PublishingEvent[] = [
    {
      id: '1',
      dataProduct: 'Customer Analytics Dataset',
      version: '2.1.0',
      destination: 'Public Marketplace',
      publisher: 'John Doe',
      timestamp: '2024-03-10 15:30',
      status: 'success',
      details: 'Successfully published to all destinations'
    },
    {
      id: '2',
      dataProduct: 'Financial Reports',
      version: '1.5.0',
      destination: 'Internal Catalog',
      publisher: 'Jane Smith',
      timestamp: '2024-03-09 14:45',
      status: 'partial',
      details: 'Published with warnings: metadata sync incomplete'
    },
    {
      id: '3',
      dataProduct: 'Sales Transactions',
      version: '3.0.0',
      destination: 'Partner Exchange',
      publisher: 'Mike Johnson',
      timestamp: '2024-03-08 12:15',
      status: 'failed',
      details: 'Failed to sync metadata: network timeout'
    }
  ];

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'success':
        return <CheckCircleIcon color="success" />;
      case 'partial':
        return <WarningIcon color="warning" />;
      case 'failed':
        return <ErrorIcon color="error" />;
      default:
        return null;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'success':
        return 'success';
      case 'partial':
        return 'warning';
      case 'failed':
        return 'error';
      default:
        return 'default';
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 4 }}>
        <Typography variant="h4" component="h1">
          Publishing History
        </Typography>
        <Box sx={{ display: 'flex', gap: 2 }}>
          <FormControl size="small" sx={{ minWidth: 200 }}>
            <InputLabel>Time Range</InputLabel>
            <Select
              label="Time Range"
              defaultValue="7d"
            >
              <MenuItem value="24h">Last 24 Hours</MenuItem>
              <MenuItem value="7d">Last 7 Days</MenuItem>
              <MenuItem value="30d">Last 30 Days</MenuItem>
              <MenuItem value="90d">Last 90 Days</MenuItem>
            </Select>
          </FormControl>
          <Button
            variant="outlined"
            startIcon={<FilterListIcon />}
          >
            Filters
          </Button>
        </Box>
      </Box>

      {/* Publishing History Table */}
      <Paper sx={{ width: '100%', overflow: 'hidden' }}>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Timestamp</TableCell>
                <TableCell>Data Product</TableCell>
                <TableCell>Version</TableCell>
                <TableCell>Destination</TableCell>
                <TableCell>Publisher</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Details</TableCell>
                <TableCell align="center">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {publishingEvents.map((event) => (
                <TableRow key={event.id}>
                  <TableCell>{event.timestamp}</TableCell>
                  <TableCell>{event.dataProduct}</TableCell>
                  <TableCell>
                    <Chip
                      label={`v${event.version}`}
                      size="small"
                      variant="outlined"
                    />
                  </TableCell>
                  <TableCell>{event.destination}</TableCell>
                  <TableCell>{event.publisher}</TableCell>
                  <TableCell>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      {getStatusIcon(event.status)}
                      <Chip
                        label={event.status}
                        color={getStatusColor(event.status)}
                        size="small"
                      />
                    </Box>
                  </TableCell>
                  <TableCell>{event.details}</TableCell>
                  <TableCell align="center">
                    <Box sx={{ display: 'flex', justifyContent: 'center', gap: 1 }}>
                      <Tooltip title="View Details">
                        <IconButton size="small">
                          <VisibilityIcon />
                        </IconButton>
                      </Tooltip>
                      {event.status === 'failed' && (
                        <Tooltip title="Retry Publication">
                          <IconButton size="small" color="primary">
                            <RestoreIcon />
                          </IconButton>
                        </Tooltip>
                      )}
                    </Box>
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

export default PublishingHistory; 