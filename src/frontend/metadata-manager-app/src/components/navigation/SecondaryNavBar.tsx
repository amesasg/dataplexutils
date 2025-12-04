import React from 'react';
import {
  AppBar,
  Toolbar,
  Breadcrumbs,
  Link,
  Button,
  Box,
  Typography,
} from '@mui/material';
import { Link as RouterLink, useLocation } from 'react-router-dom';
import NavigateNextIcon from '@mui/icons-material/NavigateNext';
import AddIcon from '@mui/icons-material/Add';
import RefreshIcon from '@mui/icons-material/Refresh';

const SecondaryNavBar = () => {
  const location = useLocation();

  const getBreadcrumbText = (path: string): string => {
    switch (path) {
      case '/':
        return 'Generation';
      case '/review':
        return 'Review';
      default:
        const pathStr = String(path);
        if (pathStr && pathStr.length > 1) {
          return pathStr.charAt(1).toUpperCase() + pathStr.slice(2);
        }
        return String(path);
    }
  };

  const pathnames = location.pathname.split('/').filter(Boolean);

  return (
    <AppBar
      position="fixed"
      sx={{
        top: 64,
        backgroundColor: 'white',
        color: 'text.primary',
        borderBottom: '1px solid',
        borderColor: 'divider',
        boxShadow: 'none',
      }}
    >
      <Toolbar>
        <Breadcrumbs
          separator={<NavigateNextIcon fontSize="small" />}
          aria-label="breadcrumb"
        >
          <Link
            component={RouterLink}
            to="/"
            color="inherit"
            sx={{ 
              textDecoration: 'none',
              '&:hover': {
                textDecoration: 'underline',
              },
            }}
          >
            Home
          </Link>
          {pathnames.map((value: string, index: number) => {
            const last = index === pathnames.length - 1;
            const to = `/${pathnames.slice(0, index + 1).join('/')}`;

            return last ? (
              <Typography color="text.primary" key={to}>
                {getBreadcrumbText(to)}
              </Typography>
            ) : (
              <Link
                component={RouterLink}
                to={to}
                key={to}
                color="inherit"
                sx={{ 
                  textDecoration: 'none',
                  '&:hover': {
                    textDecoration: 'underline',
                  },
                }}
              >
                {getBreadcrumbText(to)}
              </Link>
            );
          })}
        </Breadcrumbs>

        <Box sx={{ flexGrow: 1 }} />

        <Box sx={{ display: 'flex', gap: 1 }}>
          <Button
            variant="outlined"
            startIcon={<RefreshIcon />}
            size="small"
            color="primary"
          >
            Refresh
          </Button>
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            size="small"
            color="primary"
          >
            New Generation
          </Button>
        </Box>
      </Toolbar>
    </AppBar>
  );
};

export default SecondaryNavBar; 