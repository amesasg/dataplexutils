import React from 'react';
import {
  Box,
  Typography,
  Paper,
  Grid,
  Card,
  CardContent,
  Button,
  Chip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Breadcrumbs,
  Link,
} from '@mui/material';
import { useNavigate } from 'react-router-dom';
import AssessmentIcon from '@mui/icons-material/Assessment';
import WarningIcon from '@mui/icons-material/Warning';
import ErrorIcon from '@mui/icons-material/Error';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';

interface DiagnosticSummary {
  id: string;
  name: string;
  domain: string;
  criticalIssues: number;
  warnings: number;
  healthScore: number;
  lastDiagnostic: string;
  status: 'healthy' | 'warning' | 'critical';
}

const DiagnosticsOverview: React.FC = () => {
  const navigate = useNavigate();

  // Mock data
  const diagnosticSummaries: DiagnosticSummary[] = [
    {
      id: '1',
      name: 'Customer Analytics Dataset',
      domain: 'Marketing',
      criticalIssues: 1,
      warnings: 2,
      healthScore: 85,
      lastDiagnostic: '2024-03-10 15:30',
      status: 'warning'
    },
    {
      id: '2',
      name: 'Sales Transactions',
      domain: 'Sales',
      criticalIssues: 2,
      warnings: 3,
      healthScore: 75,
      lastDiagnostic: '2024-03-10 14:45',
      status: 'critical'
    },
    {
      id: '3',
      name: 'Product Inventory',
      domain: 'Product',
      criticalIssues: 0,
      warnings: 1,
      healthScore: 95,
      lastDiagnostic: '2024-03-10 13:15',
      status: 'healthy'
    }
  ];

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'healthy':
        return 'success';
      case 'warning':
        return 'warning';
      case 'critical':
        return 'error';
      default:
        return 'default';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'healthy':
        return <CheckCircleIcon color="success" />;
      case 'warning':
        return <WarningIcon color="warning" />;
      case 'critical':
        return <ErrorIcon color="error" />;
      default:
        return null;
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      {/* Breadcrumb Navigation */}
      <Breadcrumbs sx={{ mb: 2 }}>
        <Link
          color="inherit"
          href="#"
          onClick={(e) => {
            e.preventDefault();
            navigate('/contracts');
          }}
        >
          Data Products
        </Link>
        <Typography color="text.primary">Diagnostics Overview</Typography>
      </Breadcrumbs>

      {/* Header */}
      <Box sx={{ mb: 4 }}>
        <Typography variant="h4" component="h1" gutterBottom>
          Diagnostics Overview
        </Typography>
      </Box>

      {/* Summary Cards */}
      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12} md={4}>
          <Card>
            <CardContent>
              <Typography color="error" gutterBottom>
                Critical Issues
              </Typography>
              <Typography variant="h4">
                {diagnosticSummaries.reduce((sum, item) => sum + item.criticalIssues, 0)}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Across {diagnosticSummaries.filter(item => item.criticalIssues > 0).length} products
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={4}>
          <Card>
            <CardContent>
              <Typography color="warning.main" gutterBottom>
                Active Warnings
              </Typography>
              <Typography variant="h4">
                {diagnosticSummaries.reduce((sum, item) => sum + item.warnings, 0)}
              </Typography>
              <Typography variant="body2" color="text.secondary">
                Across {diagnosticSummaries.filter(item => item.warnings > 0).length} products
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={4}>
          <Card>
            <CardContent>
              <Typography color="success.main" gutterBottom>
                Average Health Score
              </Typography>
              <Typography variant="h4">
                {Math.round(
                  diagnosticSummaries.reduce((sum, item) => sum + item.healthScore, 0) /
                  diagnosticSummaries.length
                )}%
              </Typography>
              <Typography variant="body2" color="text.secondary">
                {diagnosticSummaries.filter(item => item.status === 'healthy').length} healthy products
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Diagnostics Table */}
      <Paper sx={{ width: '100%', overflow: 'hidden' }}>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Data Product</TableCell>
                <TableCell>Domain</TableCell>
                <TableCell align="center">Health Score</TableCell>
                <TableCell align="center">Critical Issues</TableCell>
                <TableCell align="center">Warnings</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Last Diagnostic</TableCell>
                <TableCell align="center">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {diagnosticSummaries.map((item) => (
                <TableRow key={item.id}>
                  <TableCell>{item.name}</TableCell>
                  <TableCell>
                    <Chip label={item.domain} size="small" />
                  </TableCell>
                  <TableCell align="center">{item.healthScore}%</TableCell>
                  <TableCell align="center">
                    {item.criticalIssues > 0 ? (
                      <Chip
                        icon={<ErrorIcon />}
                        label={item.criticalIssues}
                        color="error"
                        size="small"
                      />
                    ) : (
                      item.criticalIssues
                    )}
                  </TableCell>
                  <TableCell align="center">
                    {item.warnings > 0 ? (
                      <Chip
                        icon={<WarningIcon />}
                        label={item.warnings}
                        color="warning"
                        size="small"
                      />
                    ) : (
                      item.warnings
                    )}
                  </TableCell>
                  <TableCell>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      {getStatusIcon(item.status)}
                      <Chip
                        label={item.status}
                        color={getStatusColor(item.status)}
                        size="small"
                      />
                    </Box>
                  </TableCell>
                  <TableCell>{item.lastDiagnostic}</TableCell>
                  <TableCell align="center">
                    <Button
                      variant="contained"
                      color="primary"
                      size="small"
                      startIcon={<AssessmentIcon />}
                      onClick={() => navigate(`/contracts/diagnostics/${item.id}`)}
                    >
                      View Details
                    </Button>
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

export default DiagnosticsOverview; 