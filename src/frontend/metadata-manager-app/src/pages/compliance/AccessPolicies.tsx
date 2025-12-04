import React, { useState } from 'react';
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
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import SecurityIcon from '@mui/icons-material/Security';
import GroupIcon from '@mui/icons-material/Group';
import PersonIcon from '@mui/icons-material/Person';
import DomainIcon from '@mui/icons-material/Domain';

interface AccessPolicy {
  id: string;
  name: string;
  type: 'role-based' | 'user-based' | 'domain-based';
  scope: string[];
  permissions: string[];
  dataProducts: string[];
  lastModified: string;
  status: 'active' | 'inactive';
}

const AccessPolicies: React.FC = () => {
  const [openDialog, setOpenDialog] = useState(false);
  const [selectedPolicy, setSelectedPolicy] = useState<AccessPolicy | null>(null);

  // Mock data
  const policies: AccessPolicy[] = [
    {
      id: '1',
      name: 'Marketing Team Access',
      type: 'role-based',
      scope: ['Marketing Team', 'Analytics Team'],
      permissions: ['read', 'export', 'analyze'],
      dataProducts: ['Customer Analytics', 'Marketing Metrics'],
      lastModified: '2024-03-10',
      status: 'active'
    },
    {
      id: '2',
      name: 'Finance Department',
      type: 'domain-based',
      scope: ['finance.company.com'],
      permissions: ['read', 'write', 'admin'],
      dataProducts: ['Financial Reports', 'Revenue Data'],
      lastModified: '2024-03-09',
      status: 'active'
    },
    {
      id: '3',
      name: 'External Auditors',
      type: 'user-based',
      scope: ['auditor1@audit.com', 'auditor2@audit.com'],
      permissions: ['read', 'export'],
      dataProducts: ['Compliance Reports', 'Audit Logs'],
      lastModified: '2024-03-08',
      status: 'inactive'
    }
  ];

  const handleEdit = (policy: AccessPolicy) => {
    setSelectedPolicy(policy);
    setOpenDialog(true);
  };

  const getTypeIcon = (type: string) => {
    switch (type) {
      case 'role-based':
        return <GroupIcon />;
      case 'user-based':
        return <PersonIcon />;
      case 'domain-based':
        return <DomainIcon />;
      default:
        return <SecurityIcon />;
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 4 }}>
        <Typography variant="h4" component="h1">
          Access Policies
        </Typography>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => setOpenDialog(true)}
        >
          Create Policy
        </Button>
      </Box>

      {/* Policy Cards */}
      <Grid container spacing={3}>
        {policies.map((policy) => (
          <Grid item xs={12} md={4} key={policy.id}>
            <Card>
              <CardContent>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    {getTypeIcon(policy.type)}
                    <Typography variant="h6">{policy.name}</Typography>
                  </Box>
                  <Chip
                    label={policy.status}
                    color={policy.status === 'active' ? 'success' : 'error'}
                    size="small"
                  />
                </Box>

                <Typography variant="body2" color="textSecondary" gutterBottom>
                  Type: {policy.type}
                </Typography>

                <Typography variant="subtitle2" sx={{ mt: 2 }}>Scope:</Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mb: 2 }}>
                  {policy.scope.map((item) => (
                    <Chip
                      key={item}
                      label={item}
                      size="small"
                      variant="outlined"
                    />
                  ))}
                </Box>

                <Typography variant="subtitle2">Permissions:</Typography>
                <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5, mb: 2 }}>
                  {policy.permissions.map((perm) => (
                    <Chip
                      key={perm}
                      label={perm}
                      size="small"
                      color="primary"
                      variant="outlined"
                    />
                  ))}
                </Box>

                <Typography variant="subtitle2">Applied to:</Typography>
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

                <Typography variant="body2" color="textSecondary" sx={{ mt: 2 }}>
                  Last Modified: {policy.lastModified}
                </Typography>
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
          {selectedPolicy ? 'Edit Access Policy' : 'Create Access Policy'}
        </DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
            <TextField
              label="Policy Name"
              fullWidth
              defaultValue={selectedPolicy?.name}
            />
            <FormControl fullWidth>
              <InputLabel>Policy Type</InputLabel>
              <Select
                value={selectedPolicy?.type || 'role-based'}
                label="Policy Type"
              >
                <MenuItem value="role-based">Role-based</MenuItem>
                <MenuItem value="user-based">User-based</MenuItem>
                <MenuItem value="domain-based">Domain-based</MenuItem>
              </Select>
            </FormControl>
            <TextField
              label="Scope"
              fullWidth
              multiline
              rows={2}
              defaultValue={selectedPolicy?.scope.join('\n')}
              helperText="Enter one item per line"
            />
            <FormControl fullWidth>
              <InputLabel>Permissions</InputLabel>
              <Select
                multiple
                value={selectedPolicy?.permissions || []}
                label="Permissions"
                renderValue={(selected) => (
                  <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                    {selected.map((value) => (
                      <Chip key={value} label={value} size="small" />
                    ))}
                  </Box>
                )}
              >
                <MenuItem value="read">Read</MenuItem>
                <MenuItem value="write">Write</MenuItem>
                <MenuItem value="export">Export</MenuItem>
                <MenuItem value="analyze">Analyze</MenuItem>
                <MenuItem value="admin">Admin</MenuItem>
              </Select>
            </FormControl>
            <TextField
              label="Data Products"
              fullWidth
              multiline
              rows={2}
              defaultValue={selectedPolicy?.dataProducts.join('\n')}
              helperText="Enter one product per line"
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

export default AccessPolicies; 