import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  Paper,
  Grid,
  Card,
  CardContent,
  Rating,
  Chip,
  LinearProgress,
  Tooltip,
  IconButton,
  Button,
  List,
  ListItem,
  ListItemText,
  ListItemIcon,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  Alert,
  Divider,
  Breadcrumbs,
  Link,
  ButtonGroup,
} from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import WarningIcon from '@mui/icons-material/Warning';
import TimelineIcon from '@mui/icons-material/Timeline';
import DescriptionIcon from '@mui/icons-material/Description';
import StorageIcon from '@mui/icons-material/Storage';
import SecurityIcon from '@mui/icons-material/Security';
import SpeedIcon from '@mui/icons-material/Speed';
import BugReportIcon from '@mui/icons-material/BugReport';
import UpdateIcon from '@mui/icons-material/Update';
import SchemaIcon from '@mui/icons-material/Schema';
import BuildIcon from '@mui/icons-material/Build';
import NavigateNextIcon from '@mui/icons-material/NavigateNext';
import NavigateBeforeIcon from '@mui/icons-material/NavigateBefore';
import { useNavigate, useParams } from 'react-router-dom';
import { useCache } from '../../contexts/CacheContext';

interface Recommendation {
  id: string;
  severity: 'high' | 'medium' | 'low';
  category: string;
  title: string;
  description: string;
  impact: string;
  action: string;
}

interface MetricScore {
  score: number;
  status: string;
  details: string[];
}

interface DataProductDiagnostics {
  id: string;
  name: string;
  description: string;
  owner: string;
  domain: string;
  lastUpdated: string;
  metrics: {
    sla: MetricScore;
    dataQuality: MetricScore;
    documentation: MetricScore;
    security: MetricScore;
    schema: MetricScore;
    lineage: MetricScore;
  };
  recommendations: Recommendation[];
}

const DataProductDiagnostics: React.FC = () => {
  const navigate = useNavigate();
  const { productId } = useParams();
  const { detailsCache, setDetailsCache } = useCache();
  const [isLoading, setIsLoading] = useState(false);
  const [currentIndex, setCurrentIndex] = useState(-1);
  const [dataProducts, setDataProducts] = useState<any[]>([]);
  const [diagnostics, setDiagnostics] = useState<DataProductDiagnostics | null>(null);

  // Load data products and find current index
  useEffect(() => {
    const products = detailsCache['dataProducts'] || [];
    setDataProducts(products);
    const index = products.findIndex(p => p.id === productId);
    setCurrentIndex(index);

    // Get cached details for the current product
    if (productId) {
      const tableKey = `table_${productId}`;
      const cachedDetails = detailsCache[tableKey];
      
      const loadAndCacheDetails = async () => {
        setIsLoading(true);
        try {
          // Load details for the current table
          const details = await loadTableDetails(productId);
          const updatedDetails = {
            ...products[index],
            ...details
          };
          
          // Update cache with the loaded details
          setDetailsCache(prevCache => ({
            ...prevCache,
            [tableKey]: updatedDetails
          }));

          // Set diagnostics with the loaded details
          setDiagnosticsFromDetails(updatedDetails);
          
          // After loading current table details, preload next table in background
          preloadNextTable(index);
        } catch (error) {
          console.error('Error loading table details:', error);
        } finally {
          setIsLoading(false);
        }
      };

      if (cachedDetails?.details) {
        // Use cached details
        setDiagnosticsFromDetails(cachedDetails);
        // Preload next table in background if we used cache
        preloadNextTable(index);
      } else {
        // Load details if not cached
        loadAndCacheDetails();
      }
    }
  }, [detailsCache, productId]);

  // Helper function to set diagnostics from details
  const setDiagnosticsFromDetails = (details: any) => {
    setDiagnostics({
      id: details.id,
      name: details.name,
      description: details.details.description,
      owner: details.owner,
      domain: details.domain,
      lastUpdated: details.lastUpdated,
      metrics: {
        sla: {
          score: Math.round(details.ratings.latency * 20),
          status: details.status,
          details: [
            `Latency: ${details.ratings.latency}`,
            `SLA Violations: ${details.slaViolations}`,
            `Trend: ${details.trend}`
          ]
        },
        dataQuality: {
          score: Math.round(details.ratings.dataQuality * 20),
          status: details.status,
          details: [
            `Quality Score: ${details.ratings.dataQuality}`,
            `Completeness: ${details.ratings.completeness}`,
            `Freshness: ${details.ratings.freshness}`
          ]
        },
        documentation: {
          score: Math.round(details.details.dataAccuracy),
          status: details.status,
          details: [
            'Documentation metrics from cached details',
            `Processing Time: ${details.details.additionalMetrics.avgProcessingTime.toFixed(2)}`,
            `Data Accuracy: ${details.details.additionalMetrics.dataAccuracy.toFixed(2)}`
          ]
        },
        security: {
          score: 95,
          status: 'good',
          details: [
            'Access controls properly configured',
            'Encryption at rest enabled',
            'Regular security audits performed'
          ]
        },
        schema: {
          score: 88,
          status: details.status,
          details: [
            `Schema: ${details.details.schema.join(', ')}`,
            `Row Count: ${details.details.rowCount}`,
            `Last Modified: ${details.details.lastModified}`
          ]
        },
        lineage: {
          score: 90,
          status: 'good',
          details: [
            'Complete upstream dependencies',
            'Impact analysis available',
            'Transform logic documented'
          ]
        }
      },
      recommendations: [
        {
          id: 'REC001',
          severity: details.status === 'critical' ? 'high' : details.status === 'warning' ? 'medium' : 'low',
          category: 'Documentation',
          title: 'Data Quality Improvement',
          description: `Current data quality score is ${details.ratings.dataQuality * 20}%. Consider improvements.`,
          impact: 'Reduced data reliability and trust.',
          action: 'Review and implement data quality checks.'
        },
        {
          id: 'REC002',
          severity: details.slaViolations > 0 ? 'high' : 'low',
          category: 'SLA',
          title: 'SLA Compliance',
          description: `${details.slaViolations} SLA violations detected.`,
          impact: 'Service reliability concerns.',
          action: 'Investigate and address SLA violations.'
        },
        {
          id: 'REC003',
          severity: 'low',
          category: 'Schema',
          title: 'Schema Optimization',
          description: `Schema contains ${details.details.schema.length} columns.`,
          impact: 'Potential performance impact.',
          action: 'Review schema design and optimize.'
        }
      ]
    });
  };

  // Simulate loading table details from an API
  const loadTableDetails = async (id: string) => {
    console.log('Loading details for table:', id);
    // Simulate API call
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    // In a real app, this would be actual API data
    return {
      details: {
        schema: ['id', 'name', 'value'],
        rowCount: Math.floor(Math.random() * 1000000),
        lastModified: new Date().toISOString(),
        description: `Detailed information for table ${id}`,
        additionalMetrics: {
          avgProcessingTime: Math.random() * 100,
          dataAccuracy: Math.random() * 100,
        }
      }
    };
  };

  // Preload next table details
  const preloadNextTable = async (index: number) => {
    if (index < dataProducts.length - 1) {
      const nextProduct = dataProducts[index + 1];
      const nextTableKey = `table_${nextProduct.id}`;
      
      // Only preload if not already in cache
      if (!detailsCache[nextTableKey]?.details) {
        console.log('Preloading next table details:', nextProduct.id);
        try {
          const details = await loadTableDetails(nextProduct.id);
          
          // Update cache with the loaded details
          setDetailsCache(prevCache => ({
            ...prevCache,
            [nextTableKey]: {
              ...nextProduct,
              ...details
            }
          }));
        } catch (error) {
          console.error('Error preloading table details:', error);
        }
      }
    }
  };

  const handleNavigate = async (direction: 'prev' | 'next') => {
    const newIndex = direction === 'next' ? currentIndex + 1 : currentIndex - 1;
    if (newIndex >= 0 && newIndex < dataProducts.length) {
      const nextProduct = dataProducts[newIndex];
      const nextTableKey = `table_${nextProduct.id}`;
      
      setIsLoading(true);
      try {
        // Check if we need to load the details
        if (!detailsCache[nextTableKey]?.details) {
          // Load details for the next table
          const details = await loadTableDetails(nextProduct.id);
          
          // Update cache with the loaded details
          setDetailsCache(prevCache => ({
            ...prevCache,
            [nextTableKey]: {
              ...nextProduct,
              ...details
            }
          }));
        }
        
        // Navigate to the new table
        navigate(`/contracts/diagnostics/${nextProduct.id}`);
      } catch (error) {
        console.error('Error navigating to next table:', error);
      } finally {
        setIsLoading(false);
      }
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'good':
        return 'success';
      case 'warning':
        return 'warning';
      case 'critical':
        return 'error';
      default:
        return 'primary';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'good':
        return <CheckCircleIcon color="success" />;
      case 'warning':
        return <WarningIcon color="warning" />;
      case 'critical':
        return <ErrorIcon color="error" />;
      default:
        return null;
    }
  };

  const getSeverityColor = (severity: string) => {
    switch (severity) {
      case 'high':
        return 'error';
      case 'medium':
        return 'warning';
      case 'low':
        return 'info';
      default:
        return 'default';
    }
  };

  const getCategoryIcon = (category: string) => {
    switch (category) {
      case 'Documentation':
        return <DescriptionIcon />;
      case 'SLA':
        return <SpeedIcon />;
      case 'Schema':
        return <SchemaIcon />;
      case 'Security':
        return <SecurityIcon />;
      case 'Data Quality':
        return <BugReportIcon />;
      default:
        return <BuildIcon />;
    }
  };

  if (!diagnostics) {
    return (
      <Box sx={{ p: 3 }}>
        <Typography>Loading...</Typography>
      </Box>
    );
  }

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
        <Link
          color="inherit"
          href="#"
          onClick={(e) => {
            e.preventDefault();
            navigate('/contracts/diagnostics');
          }}
        >
          Diagnostics
        </Link>
        <Typography color="text.primary">{diagnostics.name}</Typography>
      </Breadcrumbs>

      {/* Header */}
      <Box sx={{ mb: 4 }}>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Typography variant="h4" component="h1">
            Data Product Diagnostics
          </Typography>
          <Box sx={{ display: 'flex', gap: 2 }}>
            <ButtonGroup variant="outlined">
              <Button
                startIcon={<NavigateBeforeIcon />}
                onClick={() => handleNavigate('prev')}
                disabled={isLoading || currentIndex <= 0}
              >
                Prev
              </Button>
              <Button
                endIcon={<NavigateNextIcon />}
                onClick={() => handleNavigate('next')}
                disabled={isLoading || currentIndex >= dataProducts.length - 1}
              >
                Next
              </Button>
            </ButtonGroup>
            <Button
              variant="outlined"
              onClick={() => navigate('/contracts/diagnostics')}
            >
              Back to Diagnostics
            </Button>
          </Box>
        </Box>
        <Paper sx={{ p: 2, mb: 3 }}>
          <Grid container spacing={2} alignItems="center">
            <Grid item xs={12} md={4}>
              <Typography variant="h5">{diagnostics.name}</Typography>
              <Typography variant="body2" color="textSecondary">
                {diagnostics.description}
              </Typography>
            </Grid>
            <Grid item xs={12} md={4}>
              <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
                <Chip label={diagnostics.domain} />
                <Typography variant="body2">
                  Owner: {diagnostics.owner}
                </Typography>
              </Box>
            </Grid>
            <Grid item xs={12} md={4}>
              <Typography variant="body2" color="textSecondary">
                Last Updated: {diagnostics.lastUpdated}
              </Typography>
            </Grid>
          </Grid>
        </Paper>
      </Box>

      {/* Overall Health Score */}
      <Paper sx={{ p: 3, mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          Overall Health Score
        </Typography>
        <Grid container spacing={3}>
          {Object.entries(diagnostics.metrics).map(([key, metric]) => (
            <Grid item xs={12} sm={6} md={4} key={key}>
              <Card>
                <CardContent>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                    {getStatusIcon(metric.status)}
                    <Typography variant="h6" component="div" sx={{ textTransform: 'capitalize' }}>
                      {key}
                    </Typography>
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
                    <Typography variant="h4" component="div" sx={{ mr: 2 }}>
                      {metric.score}%
                    </Typography>
                    <Chip
                      label={metric.status}
                      color={getStatusColor(metric.status)}
                      size="small"
                    />
                  </Box>
                  <LinearProgress
                    variant="determinate"
                    value={metric.score}
                    color={getStatusColor(metric.status)}
                    sx={{ mb: 2 }}
                  />
                  <List dense>
                    {metric.details.map((detail, index) => (
                      <ListItem key={index}>
                        <ListItemText
                          primary={detail}
                          primaryTypographyProps={{ variant: 'body2' }}
                        />
                      </ListItem>
                    ))}
                  </List>
                </CardContent>
              </Card>
            </Grid>
          ))}
        </Grid>
      </Paper>

      {/* Recommendations */}
      <Paper sx={{ p: 3 }}>
        <Typography variant="h6" gutterBottom>
          Improvement Recommendations
        </Typography>
        {diagnostics.recommendations.map((rec) => (
          <Accordion key={rec.id} sx={{ mb: 1 }}>
            <AccordionSummary expandIcon={<ExpandMoreIcon />}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, width: '100%' }}>
                <ListItemIcon>
                  {getCategoryIcon(rec.category)}
                </ListItemIcon>
                <Box sx={{ flex: 1 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Typography variant="subtitle1">{rec.title}</Typography>
                    <Chip
                      label={rec.severity}
                      color={getSeverityColor(rec.severity)}
                      size="small"
                    />
                    <Chip label={rec.category} size="small" />
                  </Box>
                </Box>
              </Box>
            </AccordionSummary>
            <AccordionDetails>
              <Grid container spacing={2}>
                <Grid item xs={12}>
                  <Typography variant="body1" paragraph>
                    {rec.description}
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Alert severity="warning" sx={{ mb: 2 }}>
                    <Typography variant="subtitle2">Impact</Typography>
                    {rec.impact}
                  </Alert>
                </Grid>
                <Grid item xs={12} md={6}>
                  <Alert severity="info">
                    <Typography variant="subtitle2">Recommended Action</Typography>
                    {rec.action}
                  </Alert>
                </Grid>
              </Grid>
              <Box sx={{ mt: 2, display: 'flex', justifyContent: 'flex-end' }}>
                <Button
                  variant="contained"
                  color="primary"
                  startIcon={<BuildIcon />}
                >
                  Take Action
                </Button>
              </Box>
            </AccordionDetails>
          </Accordion>
        ))}
      </Paper>
    </Box>
  );
};

export default DataProductDiagnostics; 