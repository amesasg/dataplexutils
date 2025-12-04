import React, { useState } from 'react';
import {
  Box,
  Typography,
  Paper,
  Tabs,
  Tab,
  Grid,
  Card,
  CardContent,
  Button,
  IconButton,
  List,
  ListItem,
  ListItemText,
  ListItemSecondaryAction,
  Chip,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Alert,
  Accordion,
  AccordionSummary,
  AccordionDetails,
} from '@mui/material';
import {
  Timeline,
  TimelineItem,
  TimelineSeparator,
  TimelineConnector,
  TimelineContent,
  TimelineDot,
  TimelineOppositeContent,
} from '@mui/lab';
import EditIcon from '@mui/icons-material/Edit';
import AddIcon from '@mui/icons-material/Add';
import HistoryIcon from '@mui/icons-material/History';
import SchemaIcon from '@mui/icons-material/Schema';
import DescriptionIcon from '@mui/icons-material/Description';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import CompareArrowsIcon from '@mui/icons-material/CompareArrows';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`terms-tabpanel-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ p: 3 }}>{children}</Box>}
    </div>
  );
}

const ContractTerms: React.FC = () => {
  const [tabValue, setTabValue] = useState(0);
  const [openSchemaDialog, setOpenSchemaDialog] = useState(false);
  const [selectedVersion, setSelectedVersion] = useState<string | null>(null);

  // Mock data for demonstration
  const schemaVersions = [
    {
      version: '2.1.0',
      date: '2024-03-10',
      author: 'John Doe',
      changes: ['Added data quality metrics', 'Updated SLA parameters'],
      status: 'current'
    },
    {
      version: '2.0.0',
      date: '2024-02-15',
      author: 'Jane Smith',
      changes: ['Major schema restructuring', 'Added compliance fields'],
      status: 'deprecated'
    },
    {
      version: '1.1.0',
      date: '2024-01-20',
      author: 'Mike Johnson',
      changes: ['Added versioning support'],
      status: 'deprecated'
    }
  ];

  const termsOfUsage = [
    {
      id: 1,
      title: 'Data Access',
      content: 'Defines who can access the data and under what circumstances.',
      category: 'Access Control'
    },
    {
      id: 2,
      title: 'Data Quality',
      content: 'Specifies the required data quality standards and metrics.',
      category: 'Quality'
    },
    {
      id: 3,
      title: 'Compliance',
      content: 'Outlines compliance requirements and regulatory standards.',
      category: 'Regulatory'
    }
  ];

  const sampleSchema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
      "contractId": {
        "type": "string",
        "description": "Unique identifier for the contract"
      },
      "version": {
        "type": "string",
        "pattern": "^\\d+\\.\\d+\\.\\d+$",
        "description": "Semantic version of the contract"
      },
      "terms": {
        "type": "array",
        "items": {
          "type": "object",
          "properties": {
            "category": {
              "type": "string",
              "enum": ["Access Control", "Quality", "Regulatory"]
            },
            "requirements": {
              "type": "array",
              "items": {
                "type": "object",
                "properties": {
                  "description": { "type": "string" },
                  "threshold": { "type": "number" },
                  "priority": {
                    "type": "string",
                    "enum": ["High", "Medium", "Low"]
                  }
                }
              }
            }
          }
        }
      }
    }
  };

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  const handleSchemaVersionSelect = (version: string) => {
    setSelectedVersion(version);
    setOpenSchemaDialog(true);
  };

  return (
    <Box sx={{ p: 3 }}>
      <Typography variant="h4" component="h1" gutterBottom>
        Contract Terms & Versioning
      </Typography>

      <Box sx={{ borderBottom: 1, borderColor: 'divider', mb: 3 }}>
        <Tabs value={tabValue} onChange={handleTabChange}>
          <Tab icon={<SchemaIcon />} label="Schema" />
          <Tab icon={<DescriptionIcon />} label="Terms of Usage" />
          <Tab icon={<HistoryIcon />} label="Version History" />
        </Tabs>
      </Box>

      {/* Schema Tab */}
      <TabPanel value={tabValue} index={0}>
        <Grid container spacing={3}>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 3, mb: 3 }}>
              <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
                <Typography variant="h6">Current Schema</Typography>
                <Button
                  variant="contained"
                  startIcon={<AddIcon />}
                >
                  New Version
                </Button>
              </Box>
              <Box sx={{ height: '400px', border: '1px solid', borderColor: 'divider', overflow: 'auto' }}>
                <TextField
                  multiline
                  fullWidth
                  variant="outlined"
                  value={JSON.stringify(sampleSchema, null, 2)}
                  InputProps={{
                    readOnly: true,
                    sx: { 
                      fontFamily: 'monospace',
                      height: '100%',
                      '& .MuiOutlinedInput-root': {
                        height: '100%',
                      },
                      '& .MuiOutlinedInput-input': {
                        height: '100% !important',
                        overflow: 'auto !important',
                      }
                    }
                  }}
                />
              </Box>
            </Paper>
          </Grid>
          <Grid item xs={12} md={6}>
            <Paper sx={{ p: 3 }}>
              <Typography variant="h6" gutterBottom>
                Schema Versions
              </Typography>
              <List>
                {schemaVersions.map((version) => (
                  <ListItem key={version.version}>
                    <ListItemText
                      primary={
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          <Typography>Version {version.version}</Typography>
                          <Chip
                            size="small"
                            label={version.status}
                            color={version.status === 'current' ? 'success' : 'default'}
                          />
                        </Box>
                      }
                      secondary={`${version.date} by ${version.author}`}
                    />
                    <ListItemSecondaryAction>
                      <IconButton
                        edge="end"
                        onClick={() => handleSchemaVersionSelect(version.version)}
                      >
                        <CompareArrowsIcon />
                      </IconButton>
                    </ListItemSecondaryAction>
                  </ListItem>
                ))}
              </List>
            </Paper>
          </Grid>
        </Grid>
      </TabPanel>

      {/* Terms of Usage Tab */}
      <TabPanel value={tabValue} index={1}>
        <Grid container spacing={3}>
          {termsOfUsage.map((term) => (
            <Grid item xs={12} key={term.id}>
              <Accordion>
                <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                    <Typography variant="h6">{term.title}</Typography>
                    <Chip size="small" label={term.category} />
                  </Box>
                </AccordionSummary>
                <AccordionDetails>
                  <Typography>{term.content}</Typography>
                  <Box sx={{ mt: 2 }}>
                    <Button
                      startIcon={<EditIcon />}
                      variant="outlined"
                      size="small"
                    >
                      Edit Term
                    </Button>
                  </Box>
                </AccordionDetails>
              </Accordion>
            </Grid>
          ))}
          <Grid item xs={12}>
            <Button
              startIcon={<AddIcon />}
              variant="contained"
              sx={{ mt: 2 }}
            >
              Add New Term
            </Button>
          </Grid>
        </Grid>
      </TabPanel>

      {/* Version History Tab */}
      <TabPanel value={tabValue} index={2}>
        <Paper sx={{ p: 3 }}>
          <Timeline>
            {schemaVersions.map((version, index) => (
              <TimelineItem key={version.version}>
                <TimelineOppositeContent color="text.secondary">
                  {version.date}
                </TimelineOppositeContent>
                <TimelineSeparator>
                  <TimelineDot color={version.status === 'current' ? 'success' : 'grey'} />
                  {index < schemaVersions.length - 1 && <TimelineConnector />}
                </TimelineSeparator>
                <TimelineContent>
                  <Typography variant="h6" component="span">
                    Version {version.version}
                  </Typography>
                  <Typography>Author: {version.author}</Typography>
                  <Box sx={{ mt: 1 }}>
                    {version.changes.map((change, i) => (
                      <Typography key={i} variant="body2" color="text.secondary">
                        • {change}
                      </Typography>
                    ))}
                  </Box>
                </TimelineContent>
              </TimelineItem>
            ))}
          </Timeline>
        </Paper>
      </TabPanel>

      {/* Schema Version Comparison Dialog */}
      <Dialog
        open={openSchemaDialog}
        onClose={() => setOpenSchemaDialog(false)}
        maxWidth="lg"
        fullWidth
      >
        <DialogTitle>
          Schema Version Comparison
        </DialogTitle>
        <DialogContent>
          <Grid container spacing={2}>
            <Grid item xs={6}>
              <Typography variant="subtitle1" gutterBottom>
                Current Version
              </Typography>
              <Box sx={{ height: '500px', border: '1px solid', borderColor: 'divider', overflow: 'auto' }}>
                <TextField
                  multiline
                  fullWidth
                  variant="outlined"
                  value={JSON.stringify(sampleSchema, null, 2)}
                  InputProps={{
                    readOnly: true,
                    sx: { 
                      fontFamily: 'monospace',
                      height: '100%',
                      '& .MuiOutlinedInput-root': {
                        height: '100%',
                      },
                      '& .MuiOutlinedInput-input': {
                        height: '100% !important',
                        overflow: 'auto !important',
                      }
                    }
                  }}
                />
              </Box>
            </Grid>
            <Grid item xs={6}>
              <Typography variant="subtitle1" gutterBottom>
                Version {selectedVersion}
              </Typography>
              <Box sx={{ height: '500px', border: '1px solid', borderColor: 'divider', overflow: 'auto' }}>
                <TextField
                  multiline
                  fullWidth
                  variant="outlined"
                  value={JSON.stringify(sampleSchema, null, 2)}
                  InputProps={{
                    readOnly: true,
                    sx: { 
                      fontFamily: 'monospace',
                      height: '100%',
                      '& .MuiOutlinedInput-root': {
                        height: '100%',
                      },
                      '& .MuiOutlinedInput-input': {
                        height: '100% !important',
                        overflow: 'auto !important',
                      }
                    }
                  }}
                />
              </Box>
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenSchemaDialog(false)}>Close</Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default ContractTerms; 