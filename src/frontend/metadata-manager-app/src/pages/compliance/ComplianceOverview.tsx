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
  LinearProgress,
  Button,
  IconButton,
  Tooltip,
} from '@mui/material';
import SecurityIcon from '@mui/icons-material/Security';
import PolicyIcon from '@mui/icons-material/Policy';
import WarningIcon from '@mui/icons-material/Warning';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import VisibilityIcon from '@mui/icons-material/Visibility';
import { useNavigate } from 'react-router-dom';

interface ComplianceStatus {
  id: string;
  domain: string;
  dataProduct: string;
  sensitivity: string;
  accessPolicies: number;
  retentionStatus: string;
  complianceScore: number;
  lastAudit: string;
  status: 'compliant' | 'warning' | 'violation';
}

const ComplianceOverview: React.FC = () => {
  const navigate = useNavigate();

  // Mock data
  const complianceData: ComplianceStatus[] = [
    {
      id: '1',
      domain: 'Marketing',
      dataProduct: 'Customer Analytics Dataset',
      sensitivity: 'High',
      accessPolicies: 5,
      retentionStatus: 'Active',
      complianceScore: 95,
      lastAudit: '2024-03-10',
      status: 'compliant'
    },
    {
      id: '2',
      domain: 'Finance',
      dataProduct: 'Financial Reports',
      sensitivity: 'Critical',
      accessPolicies: 8,
      retentionStatus: 'Review Required',
      complianceScore: 78,
      lastAudit: '2024-03-09',
      status: 'warning'
    },
    {
      id: '3',
      domain: 'Sales',
      dataProduct: 'Sales Transactions',
      sensitivity: 'Medium',
      accessPolicies: 3,
      retentionStatus: 'Non-Compliant',
      complianceScore: 65,
      lastAudit: '2024-03-08',
      status: 'violation'
    }
  ];

  const getStatusColor = (status: string): "success" | "warning" | "error" | "primary" => {
    switch (status) {
      case 'compliant':
        return 'success';
      case 'warning':
        return 'warning';
      case 'violation':
        return 'error';
      default:
        return 'primary';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'compliant':
        return <CheckCircleIcon color="success" />;
      case 'warning':
        return <WarningIcon color="warning" />;
      case 'violation':
        return <ErrorIcon color="error" />;
      default:
        return null;
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Typography variant="h4" component="h1" gutterBottom>
        Compliance Overview
      </Typography>

      {/* Summary Cards */}
      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <SecurityIcon color="primary" />
                <Typography color="textSecondary">Overall Compliance</Typography>
              </Box>
              <Typography variant="h4">
                {Math.round(
                  complianceData.reduce((acc, item) => acc + item.complianceScore, 0) /
                  complianceData.length
                )}%
              </Typography>
              <LinearProgress
                variant="determinate"
                value={85}
                color="primary"
                sx={{ mt: 2 }}
              />
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <PolicyIcon color="warning" />
                <Typography color="textSecondary">Policy Violations</Typography>
              </Box>
              <Typography variant="h4">
                {complianceData.filter(item => item.status === 'violation').length}
              </Typography>
              <Typography variant="body2" color="error">
                Requires immediate attention
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <WarningIcon color="warning" />
                <Typography color="textSecondary">Pending Reviews</Typography>
              </Box>
              <Typography variant="h4">
                {complianceData.filter(item => item.status === 'warning').length}
              </Typography>
              <Typography variant="body2" color="warning.main">
                Action needed
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <CheckCircleIcon color="success" />
                <Typography color="textSecondary">Compliant Products</Typography>
              </Box>
              <Typography variant="h4">
                {complianceData.filter(item => item.status === 'compliant').length}
              </Typography>
              <Typography variant="body2" color="success.main">
                Meeting all requirements
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Compliance Table */}
      <Paper sx={{ width: '100%', overflow: 'hidden' }}>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Data Product</TableCell>
                <TableCell>Domain</TableCell>
                <TableCell>Sensitivity</TableCell>
                <TableCell align="center">Access Policies</TableCell>
                <TableCell>Retention Status</TableCell>
                <TableCell align="center">Compliance Score</TableCell>
                <TableCell>Last Audit</TableCell>
                <TableCell>Status</TableCell>
                <TableCell align="center">Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {complianceData.map((row) => (
                <TableRow key={row.id}>
                  <TableCell>{row.dataProduct}</TableCell>
                  <TableCell>
                    <Chip label={row.domain} size="small" />
                  </TableCell>
                  <TableCell>
                    <Chip
                      label={row.sensitivity}
                      color={row.sensitivity === 'Critical' ? 'error' : 'default'}
                      size="small"
                    />
                  </TableCell>
                  <TableCell align="center">{row.accessPolicies}</TableCell>
                  <TableCell>{row.retentionStatus}</TableCell>
                  <TableCell align="center">
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <Typography>{row.complianceScore}%</Typography>
                      <LinearProgress
                        variant="determinate"
                        value={row.complianceScore}
                        color={getStatusColor(row.status)}
                        sx={{ width: 100 }}
                      />
                    </Box>
                  </TableCell>
                  <TableCell>{row.lastAudit}</TableCell>
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
                        onClick={() => navigate(`/compliance/details/${row.id}`)}
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

export default ComplianceOverview; 