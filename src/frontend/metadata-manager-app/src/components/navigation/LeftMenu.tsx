import React, { useState } from 'react';
import {
  Box,
  Drawer,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  ListItemButton,
  Divider,
  Toolbar,
  Collapse,
} from '@mui/material';
import { useLocation, useNavigate } from 'react-router-dom';
import SettingsIcon from '@mui/icons-material/Settings';
import RateReviewIcon from '@mui/icons-material/RateReview';
import AutoFixHighIcon from '@mui/icons-material/AutoFixHigh';
import HandshakeIcon from '@mui/icons-material/Handshake';
import SpeedIcon from '@mui/icons-material/Speed';
import DescriptionIcon from '@mui/icons-material/Description';
import AssessmentIcon from '@mui/icons-material/Assessment';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import ExpandLess from '@mui/icons-material/ExpandLess';
import ExpandMore from '@mui/icons-material/ExpandMore';
import HistoryIcon from '@mui/icons-material/History';
import SearchIcon from '@mui/icons-material/Search';
import DataObjectIcon from '@mui/icons-material/DataObject';
import SecurityIcon from '@mui/icons-material/Security';
import PolicyIcon from '@mui/icons-material/Policy';
import LockIcon from '@mui/icons-material/Lock';
import DeleteIcon from '@mui/icons-material/Delete';
import GavelIcon from '@mui/icons-material/Gavel';
import CloudUploadIcon from '@mui/icons-material/CloudUpload';
import StorefrontIcon from '@mui/icons-material/Storefront';
import SettingsApplicationsIcon from '@mui/icons-material/SettingsApplications';
import CompareArrowsIcon from '@mui/icons-material/CompareArrows';

const drawerWidth = 240;

const configItems = [
  {
    text: 'Configuration',
    icon: <SettingsIcon />,
    path: '/configuration'
  }
];

const metadataSubItems = [
  {
    text: 'Generate',
    icon: <AutoFixHighIcon />,
    path: '/generate'
  },
  {
    text: 'Review',
    icon: <RateReviewIcon />,
    path: '/review'
  }
];

const contractSubItems = [
  {
    text: 'Data Products',
    icon: <SearchIcon />,
    path: '/contracts'
  },
  {
    text: 'Diagnostics Overview',
    icon: <AssessmentIcon />,
    path: '/contracts/diagnostics'
  },
  {
    text: 'SLA Dashboard',
    icon: <SpeedIcon />,
    path: '/contracts/dashboard'
  },
  {
    text: 'Templates',
    icon: <DescriptionIcon />,
    path: '/contracts/templates'
  },
  {
    text: 'SLA Generator',
    icon: <AccountTreeIcon />,
    path: '/contracts/sla-generator'
  },
  {
    text: 'Terms & Versioning',
    icon: <HistoryIcon />,
    path: '/contracts/terms'
  }
];

const complianceSubItems = [
  {
    text: 'Compliance Overview',
    icon: <GavelIcon />,
    path: '/compliance'
  },
  {
    text: 'Data Sensitivity',
    icon: <SecurityIcon />,
    path: '/compliance/sensitivity'
  },
  {
    text: 'Access Policies',
    icon: <LockIcon />,
    path: '/compliance/access'
  },
  {
    text: 'Retention Policies',
    icon: <DeleteIcon />,
    path: '/compliance/retention'
  },
  {
    text: 'Governance Policies',
    icon: <PolicyIcon />,
    path: '/compliance/governance'
  }
];

const publishingSubItems = [
  {
    text: 'Publishing Overview',
    icon: <CloudUploadIcon />,
    path: '/publishing'
  },
  {
    text: 'Marketplaces',
    icon: <StorefrontIcon />,
    path: '/publishing/marketplaces'
  },
  {
    text: 'Publishing History',
    icon: <HistoryIcon />,
    path: '/publishing/history'
  },
  {
    text: 'Destinations',
    icon: <CompareArrowsIcon />,
    path: '/publishing/destinations'
  },
  {
    text: 'Publishing Settings',
    icon: <SettingsApplicationsIcon />,
    path: '/publishing/settings'
  }
];

const LeftMenu: React.FC = () => {
  const location = useLocation();
  const navigate = useNavigate();

  return (
    <Drawer
      variant="permanent"
      sx={{
        width: 240,
        flexShrink: 0,
        '& .MuiDrawer-paper': {
          width: 240,
          boxSizing: 'border-box',
        },
      }}
    >
      <Toolbar />
      <Box sx={{ overflow: 'auto' }}>
        <List>
          <ListItem disablePadding>
            <ListItemButton
              selected={location.pathname === '/'}
              onClick={() => navigate('/')}
            >
              <ListItemIcon>
                <AutoFixHighIcon />
              </ListItemIcon>
              <ListItemText primary="Generate" />
            </ListItemButton>
          </ListItem>

          <ListItem disablePadding>
            <ListItemButton
              selected={location.pathname === '/review'}
              onClick={() => navigate('/review')}
            >
              <ListItemIcon>
                <RateReviewIcon />
              </ListItemIcon>
              <ListItemText primary="Review" />
            </ListItemButton>
          </ListItem>

          <ListItem disablePadding>
            <ListItemButton
              selected={location.pathname === '/configuration'}
              onClick={() => navigate('/configuration')}
            >
              <ListItemIcon>
                <SettingsIcon />
              </ListItemIcon>
              <ListItemText primary="Configuration" />
            </ListItemButton>
          </ListItem>
        </List>
      </Box>
    </Drawer>
  );
};

export default LeftMenu; 