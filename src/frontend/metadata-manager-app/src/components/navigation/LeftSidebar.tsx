import React from 'react';
import {
  Drawer,
  List,
  ListItem,
  ListItemIcon,
  ListItemText,
  ListItemButton,
  Collapse,
  Box,
  Tooltip,
} from '@mui/material';
import { useLocation, useNavigate } from 'react-router-dom';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome';
import RateReviewIcon from '@mui/icons-material/RateReview';
import ExpandLess from '@mui/icons-material/ExpandLess';
import ExpandMore from '@mui/icons-material/ExpandMore';
import StorageIcon from '@mui/icons-material/Storage';
import TableChartIcon from '@mui/icons-material/TableChart';

const drawerWidth = 240;

const LeftSidebar = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const [openMetadata, setOpenMetadata] = React.useState<boolean>(true);

  const handleMetadataClick = () => {
    setOpenMetadata(!openMetadata);
  };

  return (
    <Drawer
      variant="permanent"
      sx={{
        width: drawerWidth,
        flexShrink: 0,
        '& .MuiDrawer-paper': {
          width: drawerWidth,
          boxSizing: 'border-box',
          top: 112,
          height: 'calc(100% - 112px)',
          borderRight: '1px solid',
          borderColor: 'divider',
        },
      }}
    >
      <Box sx={{ overflow: 'auto' }}>
        <List>
          <ListItem disablePadding>
            <ListItemButton onClick={handleMetadataClick}>
              <ListItemIcon>
                <StorageIcon />
              </ListItemIcon>
              <ListItemText primary="Metadata" />
              {openMetadata ? <ExpandLess /> : <ExpandMore />}
            </ListItemButton>
          </ListItem>

          <Collapse in={openMetadata} timeout="auto" unmountOnExit>
            <List component="div" disablePadding>
              <Tooltip title="Generate metadata descriptions" placement="right">
                <ListItemButton
                  sx={{ pl: 4 }}
                  selected={location.pathname === '/'}
                  onClick={() => navigate('/')}
                >
                  <ListItemIcon>
                    <AutoAwesomeIcon />
                  </ListItemIcon>
                  <ListItemText primary="Generation" />
                </ListItemButton>
              </Tooltip>

              <Tooltip title="Review and manage metadata" placement="right">
                <ListItemButton
                  sx={{ pl: 4 }}
                  selected={location.pathname === '/review'}
                  onClick={() => navigate('/review')}
                >
                  <ListItemIcon>
                    <RateReviewIcon />
                  </ListItemIcon>
                  <ListItemText primary="Review" />
                </ListItemButton>
              </Tooltip>

              <Tooltip title="Browse tables and columns" placement="right">
                <ListItemButton sx={{ pl: 4 }}>
                  <ListItemIcon>
                    <TableChartIcon />
                  </ListItemIcon>
                  <ListItemText primary="Browse" />
                </ListItemButton>
              </Tooltip>
            </List>
          </Collapse>
        </List>
      </Box>
    </Drawer>
  );
};

export default LeftSidebar; 