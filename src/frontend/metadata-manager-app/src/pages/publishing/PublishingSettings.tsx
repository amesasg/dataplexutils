import React, { useState } from 'react';
import {
  Box,
  Typography,
  Paper,
  Grid,
  Card,
  CardContent,
  Button,
  Switch,
  FormControlLabel,
  TextField,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Alert,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  ListItemSecondaryAction,
  IconButton,
  Divider,
} from '@mui/material';
import SaveIcon from '@mui/icons-material/Save';
import NotificationsIcon from '@mui/icons-material/Notifications';
import SecurityIcon from '@mui/icons-material/Security';
import StorageIcon from '@mui/icons-material/Storage';
import SyncIcon from '@mui/icons-material/Sync';
import EditIcon from '@mui/icons-material/Edit';
import DeleteIcon from '@mui/icons-material/Delete';
import AddIcon from '@mui/icons-material/Add';

interface PublishingSettings {
  notifications: {
    enabled: boolean;
    emailNotifications: boolean;
    slackNotifications: boolean;
    notifyOnSuccess: boolean;
    notifyOnFailure: boolean;
  };
  security: {
    requireApproval: boolean;
    validateMetadata: boolean;
    enforceDataQuality: boolean;
    minimumQualityScore: number;
  };
  sync: {
    autoSync: boolean;
    defaultSyncInterval: string;
    retryAttempts: number;
    retryDelay: number;
  };
  storage: {
    defaultLocation: string;
    compressionEnabled: boolean;
    encryptionEnabled: boolean;
    retentionPeriod: string;
  };
}

const PublishingSettings: React.FC = () => {
  // Mock data
  const [settings, setSettings] = useState<PublishingSettings>({
    notifications: {
      enabled: true,
      emailNotifications: true,
      slackNotifications: true,
      notifyOnSuccess: false,
      notifyOnFailure: true,
    },
    security: {
      requireApproval: true,
      validateMetadata: true,
      enforceDataQuality: true,
      minimumQualityScore: 80,
    },
    sync: {
      autoSync: true,
      defaultSyncInterval: '1h',
      retryAttempts: 3,
      retryDelay: 300,
    },
    storage: {
      defaultLocation: 'gs://data-products-prod',
      compressionEnabled: true,
      encryptionEnabled: true,
      retentionPeriod: '90d',
    },
  });

  const handleSave = () => {
    // Handle saving settings
    console.log('Saving settings:', settings);
  };

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 4 }}>
        <Typography variant="h4" component="h1">
          Publishing Settings
        </Typography>
        <Button
          variant="contained"
          startIcon={<SaveIcon />}
          onClick={handleSave}
        >
          Save Changes
        </Button>
      </Box>

      <Alert severity="info" sx={{ mb: 3 }}>
        These settings will be applied as defaults for all new publishing operations.
        Individual destinations can override these settings.
      </Alert>

      <Grid container spacing={3}>
        {/* Notification Settings */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <NotificationsIcon color="primary" />
                <Typography variant="h6">Notification Settings</Typography>
              </Box>
              <List>
                <ListItem>
                  <ListItemText primary="Enable Notifications" />
                  <ListItemSecondaryAction>
                    <Switch
                      checked={settings.notifications.enabled}
                      onChange={(e) =>
                        setSettings({
                          ...settings,
                          notifications: {
                            ...settings.notifications,
                            enabled: e.target.checked,
                          },
                        })
                      }
                    />
                  </ListItemSecondaryAction>
                </ListItem>
                <ListItem>
                  <ListItemText primary="Email Notifications" />
                  <ListItemSecondaryAction>
                    <Switch
                      checked={settings.notifications.emailNotifications}
                      disabled={!settings.notifications.enabled}
                    />
                  </ListItemSecondaryAction>
                </ListItem>
                <ListItem>
                  <ListItemText primary="Slack Notifications" />
                  <ListItemSecondaryAction>
                    <Switch
                      checked={settings.notifications.slackNotifications}
                      disabled={!settings.notifications.enabled}
                    />
                  </ListItemSecondaryAction>
                </ListItem>
                <ListItem>
                  <ListItemText
                    primary="Notify on Success"
                    secondary="Send notifications for successful operations"
                  />
                  <ListItemSecondaryAction>
                    <Switch
                      checked={settings.notifications.notifyOnSuccess}
                      disabled={!settings.notifications.enabled}
                    />
                  </ListItemSecondaryAction>
                </ListItem>
                <ListItem>
                  <ListItemText
                    primary="Notify on Failure"
                    secondary="Send notifications for failed operations"
                  />
                  <ListItemSecondaryAction>
                    <Switch
                      checked={settings.notifications.notifyOnFailure}
                      disabled={!settings.notifications.enabled}
                    />
                  </ListItemSecondaryAction>
                </ListItem>
              </List>
            </CardContent>
          </Card>
        </Grid>

        {/* Security Settings */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <SecurityIcon color="primary" />
                <Typography variant="h6">Security Settings</Typography>
              </Box>
              <List>
                <ListItem>
                  <ListItemText
                    primary="Require Approval"
                    secondary="Require approval before publishing"
                  />
                  <ListItemSecondaryAction>
                    <Switch checked={settings.security.requireApproval} />
                  </ListItemSecondaryAction>
                </ListItem>
                <ListItem>
                  <ListItemText
                    primary="Validate Metadata"
                    secondary="Ensure metadata meets requirements"
                  />
                  <ListItemSecondaryAction>
                    <Switch checked={settings.security.validateMetadata} />
                  </ListItemSecondaryAction>
                </ListItem>
                <ListItem>
                  <ListItemText
                    primary="Enforce Data Quality"
                    secondary="Check data quality before publishing"
                  />
                  <ListItemSecondaryAction>
                    <Switch checked={settings.security.enforceDataQuality} />
                  </ListItemSecondaryAction>
                </ListItem>
                <ListItem>
                  <ListItemText
                    primary="Minimum Quality Score"
                    secondary="Required quality score for publishing"
                  />
                  <ListItemSecondaryAction>
                    <TextField
                      type="number"
                      size="small"
                      value={settings.security.minimumQualityScore}
                      InputProps={{
                        endAdornment: '%',
                        inputProps: { min: 0, max: 100 },
                      }}
                      sx={{ width: 100 }}
                    />
                  </ListItemSecondaryAction>
                </ListItem>
              </List>
            </CardContent>
          </Card>
        </Grid>

        {/* Sync Settings */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <SyncIcon color="primary" />
                <Typography variant="h6">Sync Settings</Typography>
              </Box>
              <List>
                <ListItem>
                  <ListItemText
                    primary="Auto-Sync"
                    secondary="Automatically sync data and metadata"
                  />
                  <ListItemSecondaryAction>
                    <Switch checked={settings.sync.autoSync} />
                  </ListItemSecondaryAction>
                </ListItem>
                <ListItem>
                  <ListItemText
                    primary="Default Sync Interval"
                    secondary="How often to sync data"
                  />
                  <ListItemSecondaryAction>
                    <FormControl size="small" sx={{ width: 150 }}>
                      <Select
                        value={settings.sync.defaultSyncInterval}
                        disabled={!settings.sync.autoSync}
                      >
                        <MenuItem value="1h">Every Hour</MenuItem>
                        <MenuItem value="6h">Every 6 Hours</MenuItem>
                        <MenuItem value="12h">Every 12 Hours</MenuItem>
                        <MenuItem value="24h">Every 24 Hours</MenuItem>
                      </Select>
                    </FormControl>
                  </ListItemSecondaryAction>
                </ListItem>
                <ListItem>
                  <ListItemText
                    primary="Retry Attempts"
                    secondary="Number of retry attempts on failure"
                  />
                  <ListItemSecondaryAction>
                    <TextField
                      type="number"
                      size="small"
                      value={settings.sync.retryAttempts}
                      InputProps={{
                        inputProps: { min: 0, max: 10 },
                      }}
                      sx={{ width: 100 }}
                    />
                  </ListItemSecondaryAction>
                </ListItem>
                <ListItem>
                  <ListItemText
                    primary="Retry Delay"
                    secondary="Delay between retry attempts (seconds)"
                  />
                  <ListItemSecondaryAction>
                    <TextField
                      type="number"
                      size="small"
                      value={settings.sync.retryDelay}
                      InputProps={{
                        inputProps: { min: 0 },
                      }}
                      sx={{ width: 100 }}
                    />
                  </ListItemSecondaryAction>
                </ListItem>
              </List>
            </CardContent>
          </Card>
        </Grid>

        {/* Storage Settings */}
        <Grid item xs={12} md={6}>
          <Card>
            <CardContent>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <StorageIcon color="primary" />
                <Typography variant="h6">Storage Settings</Typography>
              </Box>
              <List>
                <ListItem>
                  <ListItemText
                    primary="Default Storage Location"
                    secondary="Default location for published data"
                  />
                  <ListItemSecondaryAction>
                    <TextField
                      size="small"
                      value={settings.storage.defaultLocation}
                      sx={{ width: 250 }}
                    />
                  </ListItemSecondaryAction>
                </ListItem>
                <ListItem>
                  <ListItemText
                    primary="Enable Compression"
                    secondary="Compress data before storage"
                  />
                  <ListItemSecondaryAction>
                    <Switch checked={settings.storage.compressionEnabled} />
                  </ListItemSecondaryAction>
                </ListItem>
                <ListItem>
                  <ListItemText
                    primary="Enable Encryption"
                    secondary="Encrypt data at rest"
                  />
                  <ListItemSecondaryAction>
                    <Switch checked={settings.storage.encryptionEnabled} />
                  </ListItemSecondaryAction>
                </ListItem>
                <ListItem>
                  <ListItemText
                    primary="Retention Period"
                    secondary="How long to keep published data"
                  />
                  <ListItemSecondaryAction>
                    <FormControl size="small" sx={{ width: 150 }}>
                      <Select value={settings.storage.retentionPeriod}>
                        <MenuItem value="30d">30 Days</MenuItem>
                        <MenuItem value="90d">90 Days</MenuItem>
                        <MenuItem value="180d">180 Days</MenuItem>
                        <MenuItem value="365d">1 Year</MenuItem>
                        <MenuItem value="unlimited">Unlimited</MenuItem>
                      </Select>
                    </FormControl>
                  </ListItemSecondaryAction>
                </ListItem>
              </List>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Box>
  );
};

export default PublishingSettings; 