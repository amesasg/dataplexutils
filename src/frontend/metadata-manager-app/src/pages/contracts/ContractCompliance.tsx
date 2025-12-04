import React, { useState } from 'react';
import {
  Box,
  Typography,
  Paper,
  Grid,
  Card,
  CardContent,
  Button,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
  IconButton,
  Menu,
  MenuItem,
  LinearProgress,
  Select,
  FormControl,
  InputLabel,
} from '@mui/material';
import MoreVertIcon from '@mui/icons-material/MoreVert';
import FilterListIcon from '@mui/icons-material/FilterList';
import TimelineIcon from '@mui/icons-material/Timeline';
import AssessmentIcon from '@mui/icons-material/Assessment';

const ContractCompliance: React.FC = () => {
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const [timeRange, setTimeRange] = useState('7d');

  // Mock data for demonstration
  const complianceData = [
    {
      id: 1,
      dataset: 'Customer Data Lake',
      contract: 'Data Quality Standard',
      compliance: 98.5,
      status: 'Compliant',
      lastChecked: '2024-03-10 15:30',
      violations: 2
    },
    {
      id: 2,
      dataset: 'Sales Analytics',
      contract: 'Real-time SLA',
      compliance: 85.2,
      status: 'At Risk',
      lastChecked: '2024-03-10 15:25',
      violations: 8
    },
    {
      id: 3,
      dataset: 'Product Inventory',
      contract: 'Data Freshness SLA',
      compliance: 92.8,
      status: 'Compliant',
      lastChecked: '2024-03-10 15:20',
      violations: 4
    },
    {
      id: 4,
      dataset: 'User Activity Logs',
      contract: 'Data Privacy Standard',
      compliance: 75.5,
      status: 'Non-Compliant',
      lastChecked: '2024-03-10 15:15',
      violations: 12
    }
  ];

  const handleClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'Compliant':
        return 'success';
      case 'At Risk':
        return 'warning';
      case 'Non-Compliant':
        return 'error';
      default:
        return 'default';
    }
  };

  const getComplianceColor = (compliance: number) => {
    if (compliance >= 95) return 'success';
    if (compliance >= 85) return 'warning';
    return 'error';
  };

  return (
    <Box sx={{ p: 3 }}>
      {/* Header */}
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 4 }}>
        <Typography variant="h4" component="h1">
          Contract Compliance
        </Typography>
        <Box sx={{ display: 'flex', gap: 2 }}>
          <FormControl size="small">
            <InputLabel>Time Range</InputLabel>
            <Select
              value={timeRange}
              label="Time Range"
              onChange={(e) => setTimeRange(e.target.value)}
              sx={{ minWidth: 120 }}
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
          <Button
            variant="contained"
            startIcon={<AssessmentIcon />}
          >
            Generate Report
          </Button>
        </Box>
      </Box>

      {/* Summary Cards */}
      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Overall Compliance
              </Typography>
              <Typography variant="h4">88%</Typography>
              <LinearProgress
                variant="determinate"
                value={88}
                color="warning"
                sx={{ mt: 2 }}
              />
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Active Contracts
              </Typography>
              <Typography variant="h4">24</Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Total Violations
              </Typography>
              <Typography variant="h4">26</Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Typography color="textSecondary" gutterBottom>
                Datasets Monitored
              </Typography>
              <Typography variant="h4">156</Typography>
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
                <TableCell>Dataset</TableCell>
                <TableCell>Contract</TableCell>
                <TableCell>Compliance</TableCell>
                <TableCell>Status</TableCell>
                <TableCell>Violations</TableCell>
                <TableCell>Last Checked</TableCell>
                <TableCell>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {complianceData.map((row) => (
                <TableRow key={row.id}>
                  <TableCell>{row.dataset}</TableCell>
                  <TableCell>{row.contract}</TableCell>
                  <TableCell>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <Typography>{row.compliance}%</Typography>
                      <LinearProgress
                        variant="determinate"
                        value={row.compliance}
                        color={getComplianceColor(row.compliance)}
                        sx={{ width: 100 }}
                      />
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Chip
                      label={row.status}
                      color={getStatusColor(row.status)}
                      size="small"
                    />
                  </TableCell>
                  <TableCell>{row.violations}</TableCell>
                  <TableCell>{row.lastChecked}</TableCell>
                  <TableCell>
                    <IconButton
                      size="small"
                      onClick={handleClick}
                    >
                      <MoreVertIcon />
                    </IconButton>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </Paper>

      {/* Actions Menu */}
      <Menu
        anchorEl={anchorEl}
        open={Boolean(anchorEl)}
        onClose={handleClose}
      >
        <MenuItem onClick={handleClose}>
          <TimelineIcon sx={{ mr: 1 }} /> View Trends
        </MenuItem>
        <MenuItem onClick={handleClose}>
          <AssessmentIcon sx={{ mr: 1 }} /> View Details
        </MenuItem>
      </Menu>
    </Box>
  );
};

export default ContractCompliance; 