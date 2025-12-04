import React, { useState } from 'react';
import {
  Box,
  Typography,
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
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  Divider,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import GavelIcon from '@mui/icons-material/Gavel';
import SecurityIcon from '@mui/icons-material/Security';
import VerifiedUserIcon from '@mui/icons-material/VerifiedUser';
import WarningIcon from '@mui/icons-material/Warning';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import RuleIcon from '@mui/icons-material/Rule';

interface GovernancePolicy {
  id: string;
  name: string;
  description: string;
  category: 'data_quality' | 'compliance' | 'security' | 'privacy';
  status: 'active' | 'draft' | 'under_review';
  dataProducts: string[];
  rules: {
    id: string;
    description: string;
    type: string;
    severity: 'high' | 'medium' | 'low';
  }[];
  lastUpdated: string;
  owner: string;
}

const GovernancePolicies: React.FC = () => {
  const [openDialog, setOpenDialog] = useState(false);
  const [selectedPolicy, setSelectedPolicy] = useState<GovernancePolicy | null>(null);

  // Mock data
  const policies: GovernancePolicy[] = [
    {
      id: '1',
      name: 'Data Quality Standards',
      description: 'Ensures data quality meets organizational standards',
      category: 'data_quality',
      status: 'active',
      dataProducts: ['Customer Analytics', 'Sales Data'],
      rules: [
        {
          id: 'r1',
          description: 'Data completeness must be above 95%',
          type: 'completeness',
          severity: 'high'
        },
        {
          id: 'r2',
          description: 'Data accuracy must be validated monthly',
          type: 'accuracy',
          severity: 'medium'
        }
      ],
      lastUpdated: '2024-03-15',
      owner: 'Data Quality Team'
    },
    {
      id: '2',
      name: 'Privacy Requirements',
      description: 'Ensures compliance with privacy regulations',
      category: 'privacy',
      status: 'under_review',
      dataProducts: ['Customer Data', 'User Profiles'],
      rules: [
        {
          id: 'r3',
          description: 'PII must be encrypted at rest',
          type: 'encryption',
          severity: 'high'
        },
        {
          id: 'r4',
          description: 'Data access must be logged',
          type: 'logging',
          severity: 'high'
        }
      ],
      lastUpdated: '2024-03-10',
      owner: 'Privacy Office'
    }
  ];

  const handleEdit = (policy: GovernancePolicy) => {
    setSelectedPolicy(policy);
    setOpenDialog(true);
  };

  const getCategoryIcon = (category: string) => {
    switch (category) {
      case 'data_quality':
        return <VerifiedUserIcon />;
      case 'compliance':
        return <GavelIcon />;
      case 'security':
        return <SecurityIcon />;
      case 'privacy':
        return <RuleIcon />;
      default:
        return <GavelIcon />;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'active':
        return 'success';
      case 'draft':
        return 'info';
      case 'under_review':
        return 'warning';
      default:
        return 'default';
    }
  };

  const getSeverityColor = (severity: string): "error" | "warning" | "info" | "inherit" => {
    switch (severity) {
      case 'high':
        return 'error';
      case 'medium':
        return 'warning';
      case 'low':
        return 'info';
      default:
        return 'inherit';
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 4 }}>
        <Typography variant="h4" component="h1">
          Governance Policies
        </Typography>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => setOpenDialog(true)}
        >
          Create Policy
        </Button>
      </Box>

      {/* Alert for policies under review */}
      {policies.some(p => p.status === 'under_review') && (
        <Alert severity="warning" sx={{ mb: 3 }}>
          Some policies are currently under review and may require attention.
        </Alert>
      )}

      {/* Policy Cards */}
      <Grid container spacing={3}>
        {policies.map((policy) => (
          <Grid item xs={12} md={6} key={policy.id}>
            <Card>
              <CardContent>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    {getCategoryIcon(policy.category)}
                    <Typography variant="h6">{policy.name}</Typography>
                  </Box>
                  <Chip
                    label={policy.status.replace('_', ' ')}
                    color={getStatusColor(policy.status)}
                    size="small"
                  />
                </Box>

                <Typography variant="body2" color="textSecondary" sx={{ mb: 2 }}>
                  {policy.description}
                </Typography>

                <Box sx={{ mb: 2 }}>
                  <Typography variant="subtitle2" gutterBottom>
                    Applies to:
                  </Typography>
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                    {policy.dataProducts.map((product) => (
                      <Chip
                        key={product}
                        label={product}
                        size="small"
                        variant="outlined"
                      />
                    ))}
                  </Box>
                </Box>

                <Typography variant="subtitle2" gutterBottom>
                  Rules:
                </Typography>
                <List dense>
                  {policy.rules.map((rule) => (
                    <ListItem key={rule.id}>
                      <ListItemIcon>
                        <RuleIcon color={getSeverityColor(rule.severity)} />
                      </ListItemIcon>
                      <ListItemText
                        primary={rule.description}
                        secondary={`Type: ${rule.type} | Severity: ${rule.severity}`}
                      />
                    </ListItem>
                  ))}
                </List>

                <Divider sx={{ my: 2 }} />

                <Box sx={{ display: 'flex', justifyContent: 'space-between' }}>
                  <Typography variant="body2" color="textSecondary">
                    Owner: {policy.owner}
                  </Typography>
                  <Typography variant="body2" color="textSecondary">
                    Last Updated: {policy.lastUpdated}
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
          {selectedPolicy ? 'Edit Governance Policy' : 'Create Governance Policy'}
        </DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Policy Name"
              fullWidth
              defaultValue={selectedPolicy?.name}
            />
            <TextField
              label="Description"
              fullWidth
              multiline
              rows={3}
              defaultValue={selectedPolicy?.description}
            />
            <FormControl fullWidth>
              <InputLabel>Category</InputLabel>
              <Select
                value={selectedPolicy?.category || 'data_quality'}
                label="Category"
              >
                <MenuItem value="data_quality">Data Quality</MenuItem>
                <MenuItem value="compliance">Compliance</MenuItem>
                <MenuItem value="security">Security</MenuItem>
                <MenuItem value="privacy">Privacy</MenuItem>
              </Select>
            </FormControl>
            <TextField
              label="Owner"
              fullWidth
              defaultValue={selectedPolicy?.owner}
            />
            <TextField
              label="Data Products (comma-separated)"
              fullWidth
              defaultValue={selectedPolicy?.dataProducts.join(', ')}
            />
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

export default GovernancePolicies; 