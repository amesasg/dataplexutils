import React from 'react';
import {
  Box,
  Typography,
  Paper,
  Grid,
  Card,
  CardContent,
  LinearProgress,
  Chip,
  IconButton,
  Button,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
} from '@mui/material';
import MoreVertIcon from '@mui/icons-material/MoreVert';
import TrendingUpIcon from '@mui/icons-material/TrendingUp';
import TrendingDownIcon from '@mui/icons-material/TrendingDown';
import WarningIcon from '@mui/icons-material/Warning';

const SLADashboard: React.FC = () => {
  // Mock data for demonstration
  const slaMetrics = [
    {
      id: 1,
      name: 'Data Freshness',
      compliance: 98.5,
      trend: 'up',
      status: 'healthy'
    },
    {
      id: 2,
      name: 'Data Quality',
      compliance: 94.2,
      trend: 'down',
      status: 'warning'
    },
    {
      id: 3,
      name: 'Schema Compliance',
      compliance: 100,
      trend: 'up',
      status: 'healthy'
    },
    {
      id: 4,
      name: 'Data Completeness',
      compliance: 89.7,
      trend: 'down',
      status: 'critical'
    }
  ];

  const recentViolations = [
    {
      id: 1,
      dataset: 'Customer Data',
      metric: 'Freshness',
      violation: 'Data more than 24h old',
      timestamp: '2024-03-10 14:30',
      status: 'Open'
    },
    {
      id: 2,
      dataset: 'Sales Transactions',
      metric: 'Quality',
      violation: 'NULL values exceed threshold',
      timestamp: '2024-03-10 12:15',
      status: 'Resolved'
    },
    {
      id: 3,
      dataset: 'Product Catalog',
      metric: 'Completeness',
      violation: 'Missing required fields',
      timestamp: '2024-03-10 10:45',
      status: 'In Progress'
    }
  ];

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 4 }}>
        <Typography variant="h4" component="h1">
          SLA Dashboard
        </Typography>
        <Button variant="contained" color="primary">
          Create New SLA
        </Button>
      </Box>

      {/* SLA Metrics Cards */}
      <Grid container spacing={3} sx={{ mb: 4 }}>
        {slaMetrics.map((metric) => (
          <Grid item xs={12} sm={6} md={3} key={metric.id}>
            <Card>
              <CardContent>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
                  <Typography color="textSecondary" gutterBottom>
                    {metric.name}
                  </Typography>
                  <IconButton size="small">
                    <MoreVertIcon />
                  </IconButton>
                </Box>
                <Typography variant="h4" component="div">
                  {metric.compliance}%
                </Typography>
                <Box sx={{ display: 'flex', alignItems: 'center', mt: 2 }}>
                  {metric.trend === 'up' ? (
                    <TrendingUpIcon color="success" />
                  ) : (
                    <TrendingDownIcon color="error" />
                  )}
                  <Chip
                    label={metric.status}
                    size="small"
                    color={
                      metric.status === 'healthy'
                        ? 'success'
                        : metric.status === 'warning'
                        ? 'warning'
                        : 'error'
                    }
                    sx={{ ml: 1 }}
                  />
                </Box>
                <LinearProgress
                  variant="determinate"
                  value={metric.compliance}
                  sx={{ mt: 2 }}
                  color={
                    metric.status === 'healthy'
                      ? 'success'
                      : metric.status === 'warning'
                      ? 'warning'
                      : 'error'
                  }
                />
              </CardContent>
            </Card>
          </Grid>
        ))}
      </Grid>

      {/* Recent SLA Violations */}
      <Paper sx={{ p: 3 }}>
        <Typography variant="h6" sx={{ mb: 2 }}>
          Recent SLA Violations
        </Typography>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Dataset</TableCell>
                <TableCell>Metric</TableCell>
                <TableCell>Violation</TableCell>
                <TableCell>Timestamp</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {recentViolations.map((violation) => (
                <TableRow key={violation.id}>
                  <TableCell>{violation.dataset}</TableCell>
                  <TableCell>{violation.metric}</TableCell>
                  <TableCell>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <WarningIcon color="warning" fontSize="small" />
                      {violation.violation}
                    </Box>
                  </TableCell>
                  <TableCell>{violation.timestamp}</TableCell>
                  <TableCell>
                    <Chip
                      label={violation.status}
                      size="small"
                      color={
                        violation.status === 'Resolved'
                          ? 'success'
                          : violation.status === 'In Progress'
                          ? 'warning'
                          : 'error'
                      }
                    />
                  </TableCell>
                  <TableCell>
                    <IconButton size="small">
                      <MoreVertIcon />
                    </IconButton>
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

export default SLADashboard; 