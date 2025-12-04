import React from 'react';
import {
  AppBar,
  Box,
  Toolbar,
  IconButton,
  Typography,
  Theme,
} from '@mui/material';
import { styled } from '@mui/material/styles';
import MenuIcon from '@mui/icons-material/Menu';
import HelpIcon from '@mui/icons-material/Help';
import AccountCircleIcon from '@mui/icons-material/AccountCircle';
import TaskTracker, { Task } from '../TaskTracker';

// Logo container styling
const LogoContainer = styled('div')(({ theme }: { theme: Theme }) => ({
  display: 'flex',
  alignItems: 'center',
  marginRight: theme.spacing(2),
  marginLeft: theme.spacing(1),
  height: '40px',
  width: '40px',
}));

interface TopNavBarProps {
  tasks: Task[];
}

const TopNavBar: React.FC<TopNavBarProps> = ({ tasks }) => {
  return (
    <AppBar 
      position="fixed" 
      sx={{ 
        zIndex: (theme: Theme) => theme.zIndex.drawer + 1,
        backgroundColor: 'white',
        color: 'text.primary',
        borderBottom: '1px solid',
        borderColor: 'divider',
        boxShadow: 'none',
      }}
    >
      <Toolbar>
        <IconButton
          size="large"
          edge="start"
          color="inherit"
          aria-label="open drawer"
          sx={{ mr: 2 }}
        >
          <MenuIcon />
        </IconButton>
        
        {/* Logo placeholder */}
        <LogoContainer>
          {/* Logo can be placed here */}
          {/* Example: <img src="/path/to/logo.png" alt="Logo" style={{ height: '100%' }} /> */}
        </LogoContainer>
        
        <Typography
          variant="h6"
          noWrap
          component="div"
          sx={{ display: { xs: 'none', sm: 'block' } }}
        >
          Metadata Manager
        </Typography>
        
        <Box sx={{ flexGrow: 1 }} />
        
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <TaskTracker tasks={tasks} />
          <IconButton
            size="large"
            aria-label="help"
            color="inherit"
          >
            <HelpIcon />
          </IconButton>
          <IconButton
            size="large"
            edge="end"
            aria-label="account"
            color="inherit"
          >
            <AccountCircleIcon />
          </IconButton>
        </Box>
      </Toolbar>
    </AppBar>
  );
};

export default TopNavBar; 