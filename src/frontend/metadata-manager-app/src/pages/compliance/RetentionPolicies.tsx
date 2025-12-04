import React, { useState } from 'react';
import {
  Box,
  Typography,
  Paper,
  Grid,
  Card,
  CardContent,
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
  Alert,
  LinearProgress,
  Tooltip,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import ScheduleIcon from '@mui/icons-material/Schedule';
import AutoDeleteIcon from '@mui/icons-material/AutoDelete';
import WarningIcon from '@mui/icons-material/Warning';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';

interface RetentionPolicy {
  id: string;
  name: string;
  dataProduct: string;
  domain: string;
  retentionPeriod: string;
  archivalRule: string;
  deleteAfterRetention: boolean;
  lastReview: string;
  nextReview: string;
  status: 'active' | 'pending-review' | 'expired';
  complianceScore: number;
}

const RetentionPolicies: React.FC = () => {
  const [openDialog, setOpenDialog] = useState(false);
  const [selectedPolicy, setSelectedPolicy] = useState<RetentionPolicy | null>(null);

  // Mock data
  const policies: RetentionPolicy[] = [
    {
      id: '1',
      name: 'Customer Data Retention',
      dataProduct: 'Customer Analytics Dataset',
      domain: 'Marketing',
      retentionPeriod: '2 years',
      archivalRule: 'Archive after 1 year',
      deleteAfterRetention: true,
      lastReview: '2024-03-10',
      nextReview: '2024-06-10',
      status: 'active',
      complianceScore: 95
    },
    {
      id: '2',
      name: 'Financial Records',
      dataProduct: 'Financial Reports',
      domain: 'Finance',
      retentionPeriod: '7 years',
      archivalRule: 'Archive after 2 years',
      deleteAfterRetention: false,
      lastReview: '2024-03-09',
      nextReview: '2024-06-09',
      status: 'pending-review',
      complianceScore: 82
    },
    {
      id: '3',
      name: 'Transaction History',
      dataProduct: 'Sales Transactions',
      domain: 'Sales',
      retentionPeriod: '5 years',
      archivalRule: 'Archive after 1 year',
      deleteAfterRetention: true,
      lastReview: '2024-02-15',
      nextReview: '2024-05-15',
      status: 'expired',
      complianceScore: 65
    }
  ];

  const handleEdit = (policy: RetentionPolicy) => {
    setSelectedPolicy(policy);
    setOpenDialog(true);
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'active':
        return 'success';
      case 'pending-review':
        return 'warning';
      case 'expired':
        return 'error';
      default:
        return 'default';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'active':
        return <CheckCircleIcon color="success" />;
      case 'pending-review':
        return <WarningIcon color="warning" />;
      case 'expired':
        return <ErrorIcon color="error" />;
      default:
        return null;
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 4 }}>
        <Typography variant="h4" component="h1">
          Retention Policies
        </Typography>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => setOpenDialog(true)}
        >
          Create Policy
        </Button>
      </Box>

      {/* Alert for policies requiring review */}
      {policies.some(p => p.status === 'pending-review' || p.status === 'expired') && (
        <Alert severity="warning" sx={{ mb: 3 }}>
          Some policies require review or have expired. Please review them to ensure compliance.
        </Alert>
      )}

      {/* Policy Cards */}
      <Grid container spacing={3}>
        {policies.map((policy) => (
          <Grid item xs={12} md={4} key={policy.id}>
            <Card>
              <CardContent>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <AutoDeleteIcon color="primary" />
                    <Typography variant="h6">{policy.name}</Typography>
                  </Box>
                  <Chip
                    label={policy.status}
                    color={getStatusColor(policy.status)}
                    size="small"
                    icon={getStatusIcon(policy.status) || undefined}
                  />
                </Box>

                <Box sx={{ mb: 2 }}>
                  <Typography variant="body2" color="textSecondary">
                    Data Product: {policy.dataProduct}
                  </Typography>
                  <Typography variant="body2" color="textSecondary">
                    Domain: {policy.domain}
                  </Typography>
                </Box>

                <Box sx={{ mb: 2 }}>
                  <Typography variant="subtitle2">Retention Rules:</Typography>
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mt: 1 }}>
                    <Chip
                      icon={<ScheduleIcon />}
                      label={`Keep for ${policy.retentionPeriod}`}
                      size="small"
                      variant="outlined"
                    />
                    <Chip
                      icon={<AutoDeleteIcon />}
                      label={policy.archivalRule}
                      size="small"
                      variant="outlined"
                    />
                  </Box>
                </Box>

                <Box sx={{ mb: 2 }}>
                  <Typography variant="subtitle2">Compliance Score:</Typography>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 1 }}>
                    <LinearProgress
                      variant="determinate"
                      value={policy.complianceScore}
                      color={
                        policy.complianceScore >= 90 ? 'success' :
                        policy.complianceScore >= 70 ? 'warning' : 'error'
                      }
                      sx={{ flexGrow: 1 }}
                    />
                    <Typography variant="body2">
                      {policy.complianceScore}%
                    </Typography>
                  </Box>
                </Box>

                <Box>
                  <Typography variant="body2" color="textSecondary">
                    Last Review: {policy.lastReview}
                  </Typography>
                  <Typography variant="body2" color="textSecondary">
                    Next Review: {policy.nextReview}
                  </Typography>
                </Box>
              </CardContent>
              <Box sx={{ display: 'flex', justifyContent: 'flex-end', p: 1 }}>
                <IconButton
                  size="small"
                  onClick={() => handleEdit(policy)}
                >
                  <EditIcon />
                </IconButton>
                <IconButton size="small" color="error">
                  <DeleteIcon />
                </IconButton>
              </Box>
            </Card>
          </Grid>
        ))}
      </Grid>

      {/* Add/Edit Policy Dialog */}
      <Dialog
        open={openDialog}
        onClose={() => setOpenDialog(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          {selectedPolicy ? 'Edit Retention Policy' : 'Create Retention Policy'}
        </DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Policy Name"
              fullWidth
              defaultValue={selectedPolicy?.name}
            />
            <TextField
              label="Data Product"
              fullWidth
              defaultValue={selectedPolicy?.dataProduct}
            />
            <TextField
              label="Domain"
              fullWidth
              defaultValue={selectedPolicy?.domain}
            />
            <FormControl fullWidth>
              <InputLabel>Retention Period</InputLabel>
              <Select
                value={selectedPolicy?.retentionPeriod || '2 years'}
                label="Retention Period"
              >
                <MenuItem value="1 year">1 Year</MenuItem>
                <MenuItem value="2 years">2 Years</MenuItem>
                <MenuItem value="5 years">5 Years</MenuItem>
                <MenuItem value="7 years">7 Years</MenuItem>
                <MenuItem value="10 years">10 Years</MenuItem>
              </Select>
            </FormControl>
            <FormControl fullWidth>
              <InputLabel>Archival Rule</InputLabel>
              <Select
                value={selectedPolicy?.archivalRule || 'Archive after 1 year'}
                label="Archival Rule"
              >
                <MenuItem value="Archive after 6 months">Archive after 6 months</MenuItem>
                <MenuItem value="Archive after 1 year">Archive after 1 year</MenuItem>
                <MenuItem value="Archive after 2 years">Archive after 2 years</MenuItem>
                <MenuItem value="No archival">No archival</MenuItem>
              </Select>
            </FormControl>
            <FormControl fullWidth>
              <InputLabel>Delete After Retention</InputLabel>
              <Select
                value={selectedPolicy?.deleteAfterRetention ? "yes" : "no"}
                label="Delete After Retention"
              >
                <MenuItem value="yes">Yes</MenuItem>
                <MenuItem value="no">No</MenuItem>
              </Select>
            </FormControl>
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenDialog(false)}>Cancel</Button>
          <Button
            variant="contained"
            onClick={() => setOpenDialog(false)}
          >
            {selectedPolicy ? 'Save Changes' : 'Create Policy'}
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default RetentionPolicies; 