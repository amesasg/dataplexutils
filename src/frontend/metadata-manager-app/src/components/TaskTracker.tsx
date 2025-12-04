import React from 'react';
import {
  Box,
  IconButton,
  Badge,
  Popover,
  List,
  ListItem,
  ListItemText,
  Typography,
  Chip,
  Tooltip,
  Paper,
} from '@mui/material';
import AssignmentIcon from '@mui/icons-material/Assignment';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import PendingIcon from '@mui/icons-material/Pending';
import AutorenewIcon from '@mui/icons-material/Autorenew';

export interface Task {
  id: string;
  type: 'generate' | 'regenerate';
  action: string;
  status: 'running' | 'completed' | 'failed';
  timestamp: Date;
  error?: string;
  details: {
    project?: string;
    dataset?: string;
    table?: string;
    scope: 'table' | 'column' | 'dataset' | 'dataset_tables' | 'dataset_all';
    description: string;
  };
}

interface TaskTrackerProps {
  tasks: Task[];
}

const TaskTracker: React.FC<TaskTrackerProps> = ({ tasks }) => {
  const [anchorEl, setAnchorEl] = React.useState<HTMLButtonElement | null>(null);

  const handleClick = (event: React.MouseEvent<HTMLButtonElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const open = Boolean(anchorEl);
  const activeTasks = tasks.filter(task => task.status === 'running');

  const getStatusIcon = (status: Task['status']) => {
    switch (status) {
      case 'running':
        return <AutorenewIcon sx={{ animation: 'spin 2s linear infinite' }} />;
      case 'completed':
        return <CheckCircleIcon color="success" />;
      case 'failed':
        return <ErrorIcon color="error" />;
      default:
        return <PendingIcon />;
    }
  };

  const getStatusColor = (status: Task['status']) => {
    switch (status) {
      case 'running':
        return 'primary';
      case 'completed':
        return 'success';
      case 'failed':
        return 'error';
      default:
        return 'default';
    }
  };

  const formatTaskDescription = (task: Task) => {
    const { details } = task;
    let description = `${task.type === 'generate' ? 'Generate' : 'Regenerate'}: ${details.description}`;
    
    if (details.project || details.dataset || details.table) {
      description += ' for';
      if (details.project) {
        description += ` project: ${details.project}`;
      }
      if (details.dataset) {
        description += ` dataset: ${details.dataset}`;
      }
      if (details.table) {
        description += ` table: ${details.table}`;
      }
    }
    
    return description;
  };

  return (
    <Box>
      <Tooltip title="Task Tracker">
        <IconButton
          onClick={handleClick}
          size="large"
          sx={{ mr: 2 }}
        >
          <Badge badgeContent={activeTasks.length} color="primary">
            <AssignmentIcon />
          </Badge>
        </IconButton>
      </Tooltip>

      <Popover
        open={open}
        anchorEl={anchorEl}
        onClose={handleClose}
        anchorOrigin={{
          vertical: 'bottom',
          horizontal: 'right',
        }}
        transformOrigin={{
          vertical: 'top',
          horizontal: 'right',
        }}
      >
        <Paper sx={{ width: 500, maxHeight: 500, overflow: 'auto' }}>
          <Box sx={{ p: 2, borderBottom: 1, borderColor: 'divider' }}>
            <Typography variant="h6">Recent Tasks</Typography>
          </Box>
          <List>
            {tasks.length === 0 ? (
              <ListItem>
                <ListItemText primary="No tasks yet" />
              </ListItem>
            ) : (
              tasks.map((task) => (
                <ListItem
                  key={task.id}
                  sx={{
                    borderBottom: '1px solid',
                    borderColor: 'divider',
                  }}
                >
                  <ListItemText
                    primary={
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        {getStatusIcon(task.status)}
                        <Typography variant="body1">
                          {formatTaskDescription(task)}
                        </Typography>
                      </Box>
                    }
                    secondary={
                      <Box sx={{ mt: 1 }}>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                          <Chip
                            size="small"
                            label={task.status.toUpperCase()}
                            color={getStatusColor(task.status)}
                          />
                          <Typography variant="caption" color="text.secondary">
                            {task.timestamp.toLocaleString()}
                          </Typography>
                        </Box>
                        {task.details.scope && (
                          <Typography variant="caption" color="text.secondary" display="block">
                            Scope: {task.details.scope.replace(/_/g, ' ')}
                          </Typography>
                        )}
                        {task.error && (
                          <Typography variant="caption" color="error" display="block">
                            Error: {task.error}
                          </Typography>
                        )}
                      </Box>
                    }
                  />
                </ListItem>
              ))
            )}
          </List>
        </Paper>
      </Popover>
    </Box>
  );
};

export default TaskTracker; 