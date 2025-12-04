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
  Alert,
} from '@mui/material';
import EditIcon from '@mui/icons-material/Edit';
import SecurityIcon from '@mui/icons-material/Security';
import ShieldIcon from '@mui/icons-material/Shield';
import PrivacyTipIcon from '@mui/icons-material/PrivacyTip';
import LockIcon from '@mui/icons-material/Lock';

interface SensitiveData {
  id: string;
  dataProduct: string;
  domain: string;
  classificationType: string;
  sensitiveFields: string[];
  encryptionLevel: string;
  accessRestrictions: string[];
  lastReview: string;
}

const DataSensitivity: React.FC = () => {
  const [openDialog, setOpenDialog] = useState(false);
  const [selectedData, setSelectedData] = useState<SensitiveData | null>(null);

  // Mock data
  const sensitiveData: SensitiveData[] = [
    {
      id: '1',
      dataProduct: 'Customer Analytics Dataset',
      domain: 'Marketing',
      classificationType: 'Confidential',
      sensitiveFields: ['email', 'phone', 'address'],
      encryptionLevel: 'AES-256',
      accessRestrictions: ['Marketing Team', 'Data Security'],
      lastReview: '2024-03-10'
    },
    {
      id: '2',
      dataProduct: 'Financial Reports',
      domain: 'Finance',
      classificationType: 'Restricted',
      sensitiveFields: ['account_number', 'transaction_details', 'balance'],
      encryptionLevel: 'AES-256',
      accessRestrictions: ['Finance Team', 'Auditors'],
      lastReview: '2024-03-09'
    },
    {
      id: '3',
      dataProduct: 'Employee Records',
      domain: 'HR',
      classificationType: 'Highly Restricted',
      sensitiveFields: ['ssn', 'salary', 'health_info'],
      encryptionLevel: 'AES-256 with HSM',
      accessRestrictions: ['HR Team', 'Legal Team'],
      lastReview: '2024-03-08'
    }
  ];

  const getClassificationColor = (type: string) => {
    switch (type) {
      case 'Highly Restricted':
        return 'error';
      case 'Restricted':
        return 'warning';
      case 'Confidential':
        return 'info';
      default:
        return 'default';
    }
  };

  const handleEdit = (data: SensitiveData) => {
    setSelectedData(data);
    setOpenDialog(true);
  };

  return (
    <Box sx={{ p: 3 }}>
      <Typography variant="h4" component="h1" gutterBottom>
        Data Sensitivity Management
      </Typography>

      {/* Summary Cards */}
      <Grid container spacing={3} sx={{ mb: 4 }}>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <SecurityIcon color="error" />
                <Typography color="textSecondary">
                  Highly Restricted Data
                </Typography>
              </Box>
              <Typography variant="h4">
                {sensitiveData.filter(d => d.classificationType === 'Highly Restricted').length}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <ShieldIcon color="warning" />
                <Typography color="textSecondary">
                  Restricted Data
                </Typography>
              </Box>
              <Typography variant="h4">
                {sensitiveData.filter(d => d.classificationType === 'Restricted').length}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <PrivacyTipIcon color="info" />
                <Typography color="textSecondary">
                  Confidential Data
                </Typography>
              </Box>
              <Typography variant="h4">
                {sensitiveData.filter(d => d.classificationType === 'Confidential').length}
              </Typography>
            </CardContent>
          </Card>
        </Grid>
        <Grid item xs={12} md={3}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <LockIcon color="success" />
                <Typography color="textSecondary">
                  Encryption Coverage
                </Typography>
              </Box>
              <Typography variant="h4">100%</Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Alert for sensitive data handling */}
      <Alert severity="warning" sx={{ mb: 3 }}>
        Remember that all sensitive data must be handled according to company policy and regulatory requirements.
        Make sure to review and update classifications regularly.
      </Alert>

      {/* Sensitive Data Table */}
      <Paper sx={{ width: '100%', overflow: 'hidden' }}>
        <TableContainer>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Data Product</TableCell>
                <TableCell>Domain</TableCell>
                <TableCell>Classification</TableCell>
                <TableCell>Sensitive Fields</TableCell>
                <TableCell>Encryption Level</TableCell>
                <TableCell>Access Restrictions</TableCell>
                <TableCell>Last Review</TableCell>
                <TableCell>Actions</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {sensitiveData.map((row) => (
                <TableRow key={row.id}>
                  <TableCell>{row.dataProduct}</TableCell>
                  <TableCell>
                    <Chip label={row.domain} size="small" />
                  </TableCell>
                  <TableCell>
                    <Chip
                      label={row.classificationType}
                      color={getClassificationColor(row.classificationType)}
                      size="small"
                    />
                  </TableCell>
                  <TableCell>
                    <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                      {row.sensitiveFields.map((field) => (
                        <Chip
                          key={field}
                          label={field}
                          size="small"
                          variant="outlined"
                        />
                      ))}
                    </Box>
                  </TableCell>
                  <TableCell>{row.encryptionLevel}</TableCell>
                  <TableCell>
                    <Box sx={{ display: 'flex', gap: 0.5, flexWrap: 'wrap' }}>
                      {row.accessRestrictions.map((restriction) => (
                        <Chip
                          key={restriction}
                          label={restriction}
                          size="small"
                          variant="outlined"
                        />
                      ))}
                    </Box>
                  </TableCell>
                  <TableCell>{row.lastReview}</TableCell>
                  <TableCell>
                    <IconButton
                      size="small"
                      onClick={() => handleEdit(row)}
                    >
                      <EditIcon />
                    </IconButton>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </Paper>

      {/* Edit Dialog */}
      <Dialog
        open={openDialog}
        onClose={() => setOpenDialog(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          Edit Sensitivity Classification
        </DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 2, display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Typography variant="subtitle1">
              {selectedData?.dataProduct}
            </Typography>
            <FormControl fullWidth>
              <InputLabel>Classification Type</InputLabel>
              <Select
                value={selectedData?.classificationType}
                label="Classification Type"
              >
                <MenuItem value="Highly Restricted">Highly Restricted</MenuItem>
                <MenuItem value="Restricted">Restricted</MenuItem>
                <MenuItem value="Confidential">Confidential</MenuItem>
              </Select>
            </FormControl>
            <TextField
              label="Sensitive Fields"
              multiline
              rows={2}
              value={selectedData?.sensitiveFields.join(', ')}
            />
            <FormControl fullWidth>
              <InputLabel>Encryption Level</InputLabel>
              <Select
                value={selectedData?.encryptionLevel}
                label="Encryption Level"
              >
                <MenuItem value="AES-256">AES-256</MenuItem>
                <MenuItem value="AES-256 with HSM">AES-256 with HSM</MenuItem>
              </Select>
            </FormControl>
            <TextField
              label="Access Restrictions"
              multiline
              rows={2}
              value={selectedData?.accessRestrictions.join(', ')}
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenDialog(false)}>Cancel</Button>
          <Button variant="contained" onClick={() => setOpenDialog(false)}>
            Save Changes
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default DataSensitivity; 