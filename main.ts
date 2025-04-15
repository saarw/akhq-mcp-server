#!/usr/bin/env node

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';

let baseUrl = 'http://localhost:8080'; // Base URL from Swagger spec or default

const server = new McpServer({
  name: 'AKHQ',
  version: '1.0.0'
});

function parameterizeEndpoint(endpoint: string, parameters: Record<string, any>): string {
  // Handle path parameters
  let path = endpoint.replace(/\{([^}]+)\}/g, (match, paramName) => {
    const value = parameters[paramName];
    if (value === undefined || value === null) {
      throw new Error(`Missing required parameter: ${paramName}`);
    }
    return encodeURIComponent(value);
  });

  // Handle query parameters
  const queryParams = Object.entries(parameters)
    .filter(([key]) => !endpoint.includes(`{${key}}`)) // Exclude path parameters
    .filter(([_, value]) => value !== undefined && value !== null) // Exclude null/undefined values
    .map(([key, value]) => {
      if (Array.isArray(value)) {
        return value.map(v => `${encodeURIComponent(key)}=${encodeURIComponent(v)}`).join('&');
      }
      return `${encodeURIComponent(key)}=${encodeURIComponent(value)}`;
    })
    .join('&');

  if (queryParams) {
    path += `?${queryParams}`;
  }

  return path;
}

async function callApi(endpoint: string, method: string, body?: any, contentType?: string) {
  const headers: Record<string, string> = {};
  if (contentType) {
    headers['Content-Type'] = contentType;
  }
  const response = await fetch(`${baseUrl}${endpoint}`, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined
  });
  const data = await response.json();
  return {
    content: [
      {
        type: 'text',
        text: JSON.stringify(data)
      }
    ]
  };
}

function registerTool(name: string, description: string, parameters: any, handler: (params: any) => Promise<any>) {
  try {
    server.tool(name, description, parameters, handler);
  } catch (error) {
    console.error(`Failed to register tool ${name}:`, error);
  }
}

// Default tools
registerTool(
  'get_servers',
  'Get available servers from the Swagger spec',
  {},
  async () => {
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify([])
        }
      ]
    };
  }
);

registerTool(
  'set_base_url',
  'Set the base URL for API requests',
  {
    url: z.string().describe('The new base URL')
  },
  async (params) => {
    const validatedParams = z.object({ url: z.string() }).parse(params);
    baseUrl = validatedParams.url;
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify({ success: true, newBaseUrl: baseUrl })
        }
      ]
    };
  }
);

// Get all auth details for current instance
registerTool(
  'get_auths',
  'Get all auth details for current instance',
  {},
  async (params) => {
    try {
      const validatedParams = z.object({}).parse(params);
      const endpoint = parameterizeEndpoint('/api/auths', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Get all cluster for current instance
registerTool(
  'get_cluster',
  'Get all cluster for current instance',
  {},
  async (params) => {
    try {
      const validatedParams = z.object({}).parse(params);
      const endpoint = parameterizeEndpoint('/api/cluster', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Get current user
registerTool(
  'get_me',
  'Get current user',
  {},
  async (params) => {
    try {
      const validatedParams = z.object({}).parse(params);
      const endpoint = parameterizeEndpoint('/api/me', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Get default topic configuration
registerTool(
  'get_topic_defaults-configs',
  'Get default topic configuration',
  {},
  async (params) => {
    try {
      const validatedParams = z.object({}).parse(params);
      const endpoint = parameterizeEndpoint('/api/topic/defaults-configs', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all acls
registerTool(
  'get_acls',
  'List all acls',
  {
    cluster: z.string(),
    search: z.string().nullable().optional()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    search: z.string().nullable().optional()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/acls', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Get acls for a principal
registerTool(
  'get_acls_by_principal',
  'Get acls for a principal',
  {
    cluster: z.string(),
    principal: z.string(),
    resourceType: z.string().nullable().optional()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    principal: z.string(),
    resourceType: z.string().nullable().optional()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/acls/{principal}', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all connect definitions
registerTool(
  'get_connect_by_connectId',
  'List all connect definitions',
  {
    cluster: z.string(),
    connectId: z.string(),
    search: z.string().nullable().optional(),
    page: z.number().nullable().optional()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    connectId: z.string(),
    search: z.string().nullable().optional(),
    page: z.number().nullable().optional()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/connect/{connectId}', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Create a new connect definition
registerTool(
  'post_connect_by_connectId',
  'Create a new connect definition',
  {
    cluster: z.string(),
    connectId: z.string(),
    body: z.object({
      name: z.string().optional(),
      configs: z.record(z.any()).optional()
    })
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    connectId: z.string(),
    body: z.object({
      name: z.string().optional(),
      configs: z.record(z.any()).optional()
    })
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/connect/{connectId}', validatedParams);
      return callApi(endpoint, 'POST', validatedParams, 'application/json');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all connect plugins
registerTool(
  'get_connect_plugins',
  'List all connect plugins',
  {
    cluster: z.string(),
    connectId: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    connectId: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/connect/{connectId}/plugins', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Retrieve a connect plugin
registerTool(
  'get_connect_plugins_by_type',
  'Retrieve a connect plugin',
  {
    cluster: z.string(),
    connectId: z.string(),
    type: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    connectId: z.string(),
    type: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/connect/{connectId}/plugins/{type}', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Validate plugin configs
registerTool(
  'put_connect_plugins_validate',
  'Validate plugin configs',
  {
    cluster: z.string(),
    connectId: z.string(),
    type: z.string(),
    body: z.object({
      configs: z.record(z.any()).optional()
    })
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    connectId: z.string(),
    type: z.string(),
    body: z.object({
      configs: z.record(z.any()).optional()
    })
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/connect/{connectId}/plugins/{type}/validate', validatedParams);
      return callApi(endpoint, 'PUT', validatedParams, 'application/json');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Retrieve a connect definition
registerTool(
  'get_connect_by_connectId_name',
  'Retrieve a connect definition',
  {
    cluster: z.string(),
    connectId: z.string(),
    name: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    connectId: z.string(),
    name: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/connect/{connectId}/{name}', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Delete a connect definition
registerTool(
  'delete_connect_by_connectId_name',
  'Delete a connect definition',
  {
    cluster: z.string(),
    connectId: z.string(),
    name: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    connectId: z.string(),
    name: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/connect/{connectId}/{name}', validatedParams);
      return callApi(endpoint, 'DELETE');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Retrieve a connect config
registerTool(
  'get_connect_configs',
  'Retrieve a connect config',
  {
    cluster: z.string(),
    connectId: z.string(),
    name: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    connectId: z.string(),
    name: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/connect/{connectId}/{name}/configs', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Update a connect definition config
registerTool(
  'post_connect_configs',
  'Update a connect definition config',
  {
    cluster: z.string(),
    connectId: z.string(),
    name: z.string(),
    body: z.object({
      configs: z.record(z.any()).optional()
    })
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    connectId: z.string(),
    name: z.string(),
    body: z.object({
      configs: z.record(z.any()).optional()
    })
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/connect/{connectId}/{name}/configs', validatedParams);
      return callApi(endpoint, 'POST', validatedParams, 'application/json');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Pause a connect definition
registerTool(
  'get_connect_pause',
  'Pause a connect definition',
  {
    cluster: z.string(),
    connectId: z.string(),
    name: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    connectId: z.string(),
    name: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/connect/{connectId}/{name}/pause', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Restart a connect definition
registerTool(
  'get_connect_restart',
  'Restart a connect definition',
  {
    cluster: z.string(),
    connectId: z.string(),
    name: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    connectId: z.string(),
    name: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/connect/{connectId}/{name}/restart', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Resume a connect definition
registerTool(
  'get_connect_resume',
  'Resume a connect definition',
  {
    cluster: z.string(),
    connectId: z.string(),
    name: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    connectId: z.string(),
    name: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/connect/{connectId}/{name}/resume', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Retrieve a connect task
registerTool(
  'get_connect_tasks',
  'Retrieve a connect task',
  {
    cluster: z.string(),
    connectId: z.string(),
    name: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    connectId: z.string(),
    name: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/connect/{connectId}/{name}/tasks', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Restart a connect task
registerTool(
  'get_connect_tasks_restart',
  'Restart a connect task',
  {
    cluster: z.string(),
    connectId: z.string(),
    name: z.string(),
    taskId: z.number()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    connectId: z.string(),
    name: z.string(),
    taskId: z.number()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/connect/{connectId}/{name}/tasks/{taskId}/restart', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all consumer groups
registerTool(
  'get_group',
  'List all consumer groups',
  {
    cluster: z.string(),
    search: z.string().nullable().optional(),
    page: z.number().nullable().optional()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    search: z.string().nullable().optional(),
    page: z.number().nullable().optional()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/group', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Retrieve consumer group for list of topics
registerTool(
  'get_group_topics',
  'Retrieve consumer group for list of topics',
  {
    cluster: z.string(),
    topics: z.array(z.string()).nullable().optional()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topics: z.array(z.string()).nullable().optional()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/group/topics', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Retrieve a consumer group
registerTool(
  'get_group_by_groupName',
  'Retrieve a consumer group',
  {
    cluster: z.string(),
    groupName: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    groupName: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/group/{groupName}', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Delete a consumer group
registerTool(
  'delete_group_by_groupName',
  'Delete a consumer group',
  {
    cluster: z.string(),
    groupName: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    groupName: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/group/{groupName}', validatedParams);
      return callApi(endpoint, 'DELETE');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Retrieve a consumer group acls
registerTool(
  'get_group_acls',
  'Retrieve a consumer group acls',
  {
    cluster: z.string(),
    groupName: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    groupName: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/group/{groupName}/acls', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Retrieve a consumer group members
registerTool(
  'get_group_members',
  'Retrieve a consumer group members',
  {
    cluster: z.string(),
    groupName: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    groupName: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/group/{groupName}/members', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Retrieve a consumer group offsets
registerTool(
  'get_group_offsets',
  'Retrieve a consumer group offsets',
  {
    cluster: z.string(),
    groupName: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    groupName: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/group/{groupName}/offsets', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Update consumer group offsets
registerTool(
  'post_group_offsets',
  'Update consumer group offsets',
  {
    cluster: z.string(),
    groupName: z.string(),
    body: z.array(z.object({
      topic: z.string().optional(),
      partition: z.number().optional(),
      offset: z.number().optional()
    }))
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    groupName: z.string(),
    body: z.array(z.object({
      topic: z.string().optional(),
      partition: z.number().optional(),
      offset: z.number().optional()
    }))
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/group/{groupName}/offsets', validatedParams);
      return callApi(endpoint, 'POST', validatedParams, 'application/json');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Retrive consumer group offsets by timestamp
registerTool(
  'get_group_offsets_start',
  'Retrive consumer group offsets by timestamp',
  {
    cluster: z.string(),
    groupName: z.string(),
    timestamp: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    groupName: z.string(),
    timestamp: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/group/{groupName}/offsets/start', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Delete group offsets of given topic
registerTool(
  'delete_group_topic_by_topicName',
  'Delete group offsets of given topic',
  {
    cluster: z.string(),
    groupName: z.string(),
    topicName: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    groupName: z.string(),
    topicName: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/group/{groupName}/topic/{topicName}', validatedParams);
      return callApi(endpoint, 'DELETE');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Execute a statement
registerTool(
  'put_ksqldb_execute',
  'Execute a statement',
  {
    cluster: z.string(),
    ksqlDbId: z.string(),
    body: z.object({
      sql: z.string().optional()
    })
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    ksqlDbId: z.string(),
    body: z.object({
      sql: z.string().optional()
    })
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/ksqldb/{ksqlDbId}/execute', validatedParams);
      return callApi(endpoint, 'PUT', validatedParams, 'application/json');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Retrieve server info
registerTool(
  'get_ksqldb_info',
  'Retrieve server info',
  {
    cluster: z.string(),
    ksqlDbId: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    ksqlDbId: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/ksqldb/{ksqlDbId}/info', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all queries
registerTool(
  'get_ksqldb_queries',
  'List all queries',
  {
    cluster: z.string(),
    ksqlDbId: z.string(),
    search: z.string().nullable().optional(),
    page: z.number().nullable().optional()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    ksqlDbId: z.string(),
    search: z.string().nullable().optional(),
    page: z.number().nullable().optional()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/ksqldb/{ksqlDbId}/queries', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Execute a query
registerTool(
  'put_ksqldb_queries_pull',
  'Execute a query',
  {
    cluster: z.string(),
    ksqlDbId: z.string(),
    body: z.object({
      sql: z.string().optional(),
      properties: z.record(z.any()).optional()
    })
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    ksqlDbId: z.string(),
    body: z.object({
      sql: z.string().optional(),
      properties: z.record(z.any()).optional()
    })
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/ksqldb/{ksqlDbId}/queries/pull', validatedParams);
      return callApi(endpoint, 'PUT', validatedParams, 'application/json');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all streams
registerTool(
  'get_ksqldb_streams',
  'List all streams',
  {
    cluster: z.string(),
    ksqlDbId: z.string(),
    search: z.string().nullable().optional(),
    page: z.number().nullable().optional()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    ksqlDbId: z.string(),
    search: z.string().nullable().optional(),
    page: z.number().nullable().optional()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/ksqldb/{ksqlDbId}/streams', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all tables
registerTool(
  'get_ksqldb_tables',
  'List all tables',
  {
    cluster: z.string(),
    ksqlDbId: z.string(),
    search: z.string().nullable().optional(),
    page: z.number().nullable().optional()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    ksqlDbId: z.string(),
    search: z.string().nullable().optional(),
    page: z.number().nullable().optional()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/ksqldb/{ksqlDbId}/tables', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all nodes
registerTool(
  'get_node',
  'List all nodes',
  {
    cluster: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/node', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// partition counts
registerTool(
  'get_node_partitions',
  'partition counts',
  {
    cluster: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/node/partitions', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Retrieve a nodes
registerTool(
  'get_node_by_nodeId',
  'Retrieve a nodes',
  {
    cluster: z.string(),
    nodeId: z.number()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    nodeId: z.number()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/node/{nodeId}', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all configs for a node
registerTool(
  'get_node_configs',
  'List all configs for a node',
  {
    cluster: z.string(),
    nodeId: z.number()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    nodeId: z.number()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/node/{nodeId}/configs', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Update configs for a node
registerTool(
  'post_node_configs',
  'Update configs for a node',
  {
    cluster: z.string(),
    nodeId: z.number(),
    body: z.object({
      configs: z.record(z.any()).optional()
    })
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    nodeId: z.number(),
    body: z.object({
      configs: z.record(z.any()).optional()
    })
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/node/{nodeId}/configs', validatedParams);
      return callApi(endpoint, 'POST', validatedParams, 'application/json');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all logs for a node
registerTool(
  'get_node_logs',
  'List all logs for a node',
  {
    cluster: z.string(),
    nodeId: z.number()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    nodeId: z.number()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/node/{nodeId}/logs', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all schemas
registerTool(
  'get_schema',
  'List all schemas',
  {
    cluster: z.string(),
    search: z.string().nullable().optional(),
    page: z.number().nullable().optional()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    search: z.string().nullable().optional(),
    page: z.number().nullable().optional()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/schema', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Create a new schema
registerTool(
  'post_schema',
  'Create a new schema',
  {
    cluster: z.string(),
    body: z.object({
      id: z.number().optional(),
      subject: z.string(),
      version: z.number(),
      compatibilityLevel: z.string().optional(),
      schema: z.string().optional(),
      schemaType: z.string().optional(),
      references: z.array(z.object({
      name: z.string().optional(),
      subject: z.string().optional(),
      version: z.number().optional()
    })).optional(),
      exception: z.string().optional()
    })
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    body: z.object({
      id: z.number().optional(),
      subject: z.string(),
      version: z.number(),
      compatibilityLevel: z.string().optional(),
      schema: z.string().optional(),
      schemaType: z.string().optional(),
      references: z.array(z.object({
      name: z.string().optional(),
      subject: z.string().optional(),
      version: z.number().optional()
    })).optional(),
      exception: z.string().optional()
    })
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/schema', validatedParams);
      return callApi(endpoint, 'POST', validatedParams, 'application/json');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Find a subject by the schema id In case of several subjects matching the schema id, we use the topic name to get the most relevant subject that matches the topic name (TopicNameStrategy). If there is no topic or if the topic doesn't match any subject, return the first subject that matches the schema id.
registerTool(
  'get_schema_id_by_id',
  'Find a subject by the schema id In case of several subjects matching the schema id, we use the topic name to get the most relevant subject that matches the topic name (TopicNameStrategy). If there is no topic or if the topic doesn\'t match any subject, return the first subject that matches the schema id.',
  {
    cluster: z.string().describe('- The cluster name'),
    id: z.number().describe('- The schema id'),
    topic: z.string().nullable().optional().describe('- (Optional) The topic name')
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string().describe('- The cluster name'),
    id: z.number().describe('- The schema id'),
    topic: z.string().nullable().optional().describe('- (Optional) The topic name')
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/schema/id/{id}', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all schemas prefered schemas for this topic
registerTool(
  'get_schema_topic_by_topic',
  'List all schemas prefered schemas for this topic',
  {
    cluster: z.string(),
    topic: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topic: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/schema/topic/{topic}', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Retrieve a schema
registerTool(
  'get_schema_by_subject',
  'Retrieve a schema',
  {
    cluster: z.string(),
    subject: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    subject: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/schema/{subject}', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Update a schema
registerTool(
  'post_schema_by_subject',
  'Update a schema',
  {
    cluster: z.string(),
    subject: z.string(),
    body: z.object({
      id: z.number().optional(),
      subject: z.string(),
      version: z.number(),
      compatibilityLevel: z.string().optional(),
      schema: z.string().optional(),
      schemaType: z.string().optional(),
      references: z.array(z.object({
      name: z.string().optional(),
      subject: z.string().optional(),
      version: z.number().optional()
    })).optional(),
      exception: z.string().optional()
    })
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    subject: z.string(),
    body: z.object({
      id: z.number().optional(),
      subject: z.string(),
      version: z.number(),
      compatibilityLevel: z.string().optional(),
      schema: z.string().optional(),
      schemaType: z.string().optional(),
      references: z.array(z.object({
      name: z.string().optional(),
      subject: z.string().optional(),
      version: z.number().optional()
    })).optional(),
      exception: z.string().optional()
    })
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/schema/{subject}', validatedParams);
      return callApi(endpoint, 'POST', validatedParams, 'application/json');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Delete a schema
registerTool(
  'delete_schema_by_subject',
  'Delete a schema',
  {
    cluster: z.string(),
    subject: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    subject: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/schema/{subject}', validatedParams);
      return callApi(endpoint, 'DELETE');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all version for a schema
registerTool(
  'get_schema_version',
  'List all version for a schema',
  {
    cluster: z.string(),
    subject: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    subject: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/schema/{subject}/version', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Delete a version for a schema
registerTool(
  'delete_schema_version_by_version',
  'Delete a version for a schema',
  {
    cluster: z.string(),
    subject: z.string(),
    version: z.number()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    subject: z.string(),
    version: z.number()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/schema/{subject}/version/{version}', validatedParams);
      return callApi(endpoint, 'DELETE');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all schemas
registerTool(
  'get_schemas',
  'List all schemas',
  {
    cluster: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/schemas', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Tail for data on multiple topic
registerTool(
  'get_tail_sse',
  'Tail for data on multiple topic',
  {
    cluster: z.string(),
    topics: z.array(z.string()),
    search: z.string().nullable().optional(),
    after: z.array(z.string()).nullable().optional()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topics: z.array(z.string()),
    search: z.string().nullable().optional(),
    after: z.array(z.string()).nullable().optional()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/tail/sse', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all topics
registerTool(
  'get_topic',
  'List all topics',
  {
    cluster: z.string(),
    search: z.string().nullable().optional(),
    show: z.string().nullable().optional(),
    page: z.number().nullable().optional(),
    uiPageSize: z.number().nullable().optional()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    search: z.string().nullable().optional(),
    show: z.string().nullable().optional(),
    page: z.number().nullable().optional(),
    uiPageSize: z.number().nullable().optional()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Create a topic
registerTool(
  'post_topic',
  'Create a topic',
  {
    cluster: z.string(),
    body: z.object({
      name: z.string().optional(),
      partition: z.number().nullable().optional(),
      replication: z.number().nullable().optional(),
      configs: z.record(z.any()).optional()
    })
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    body: z.object({
      name: z.string().optional(),
      partition: z.number().nullable().optional(),
      replication: z.number().nullable().optional(),
      configs: z.record(z.any()).optional()
    })
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic', validatedParams);
      return callApi(endpoint, 'POST', validatedParams, 'application/json');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Retrieve the last record for a list of topics
registerTool(
  'get_topic_last-record',
  'Retrieve the last record for a list of topics',
  {
    cluster: z.string(),
    topics: z.array(z.string())
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topics: z.array(z.string())
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/last-record', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all topics name
registerTool(
  'get_topic_name',
  'List all topics name',
  {
    cluster: z.string(),
    show: z.string().nullable().optional()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    show: z.string().nullable().optional()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/name', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Retrieve a topic
registerTool(
  'get_topic_by_topicName',
  'Retrieve a topic',
  {
    cluster: z.string(),
    topicName: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Delete a topic
registerTool(
  'delete_topic_by_topicName',
  'Delete a topic',
  {
    cluster: z.string(),
    topicName: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}', validatedParams);
      return callApi(endpoint, 'DELETE');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all acls from a topic
registerTool(
  'get_topic_acls',
  'List all acls from a topic',
  {
    cluster: z.string(),
    topicName: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}/acls', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all configs from a topic
registerTool(
  'get_topic_configs',
  'List all configs from a topic',
  {
    cluster: z.string(),
    topicName: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}/configs', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Update configs from a topic
registerTool(
  'post_topic_configs',
  'Update configs from a topic',
  {
    cluster: z.string(),
    topicName: z.string(),
    body: z.object({
      configs: z.record(z.any()).optional()
    })
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string(),
    body: z.object({
      configs: z.record(z.any()).optional()
    })
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}/configs', validatedParams);
      return callApi(endpoint, 'POST', validatedParams, 'application/json');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Read datas from a topic
registerTool(
  'get_topic_data',
  'Read datas from a topic',
  {
    cluster: z.string(),
    topicName: z.string(),
    after: z.string().nullable().optional(),
    partition: z.number().nullable().optional(),
    sort: z.string().nullable().optional(),
    timestamp: z.string().nullable().optional(),
    endTimestamp: z.string().nullable().optional(),
    searchByKey: z.string().nullable().optional(),
    searchByValue: z.string().nullable().optional(),
    searchByHeaderKey: z.string().nullable().optional(),
    searchByHeaderValue: z.string().nullable().optional(),
    searchByKeySubject: z.string().nullable().optional(),
    searchByValueSubject: z.string().nullable().optional()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string(),
    after: z.string().nullable().optional(),
    partition: z.number().nullable().optional(),
    sort: z.string().nullable().optional(),
    timestamp: z.string().nullable().optional(),
    endTimestamp: z.string().nullable().optional(),
    searchByKey: z.string().nullable().optional(),
    searchByValue: z.string().nullable().optional(),
    searchByHeaderKey: z.string().nullable().optional(),
    searchByHeaderValue: z.string().nullable().optional(),
    searchByKeySubject: z.string().nullable().optional(),
    searchByValueSubject: z.string().nullable().optional()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}/data', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Produce data to a topic
registerTool(
  'post_topic_data',
  'Produce data to a topic',
  {
    cluster: z.string(),
    topicName: z.string(),
    body: z.object({
      value: z.string().nullable().optional(),
      key: z.string().nullable().optional(),
      partition: z.number().nullable().optional(),
      timestamp: z.string().nullable().optional(),
      headers: z.array(z.object({
      key: z.string(),
      value: z.string()
    })).optional(),
      keySchema: z.string().nullable().optional(),
      valueSchema: z.string().nullable().optional(),
      multiMessage: z.boolean().optional(),
      keyValueSeparator: z.string().nullable().optional()
    })
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string(),
    body: z.object({
      value: z.string().nullable().optional(),
      key: z.string().nullable().optional(),
      partition: z.number().nullable().optional(),
      timestamp: z.string().nullable().optional(),
      headers: z.array(z.object({
      key: z.string(),
      value: z.string()
    })).optional(),
      keySchema: z.string().nullable().optional(),
      valueSchema: z.string().nullable().optional(),
      multiMessage: z.boolean().optional(),
      keyValueSeparator: z.string().nullable().optional()
    })
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}/data', validatedParams);
      return callApi(endpoint, 'POST', validatedParams, 'application/json');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Delete data from a topic by key
registerTool(
  'delete_topic_data',
  'Delete data from a topic by key',
  {
    cluster: z.string(),
    topicName: z.string(),
    body: z.object({
      partition: z.number().optional(),
      key: z.string().optional()
    })
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string(),
    body: z.object({
      partition: z.number().optional(),
      key: z.string().optional()
    })
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}/data', validatedParams);
      return callApi(endpoint, 'DELETE');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Download data for a topic
registerTool(
  'get_topic_data_download',
  'Download data for a topic',
  {
    cluster: z.string(),
    topicName: z.string(),
    after: z.string().nullable().optional(),
    partition: z.number().nullable().optional(),
    sort: z.string().nullable().optional(),
    timestamp: z.string().nullable().optional(),
    endTimestamp: z.string().nullable().optional(),
    searchByKey: z.string().nullable().optional(),
    searchByValue: z.string().nullable().optional(),
    searchByHeaderKey: z.string().nullable().optional(),
    searchByHeaderValue: z.string().nullable().optional(),
    searchByKeySubject: z.string().nullable().optional(),
    searchByValueSubject: z.string().nullable().optional()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string(),
    after: z.string().nullable().optional(),
    partition: z.number().nullable().optional(),
    sort: z.string().nullable().optional(),
    timestamp: z.string().nullable().optional(),
    endTimestamp: z.string().nullable().optional(),
    searchByKey: z.string().nullable().optional(),
    searchByValue: z.string().nullable().optional(),
    searchByHeaderKey: z.string().nullable().optional(),
    searchByHeaderValue: z.string().nullable().optional(),
    searchByKeySubject: z.string().nullable().optional(),
    searchByValueSubject: z.string().nullable().optional()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}/data/download', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Empty data from a topic
registerTool(
  'delete_topic_data_empty',
  'Empty data from a topic',
  {
    cluster: z.string(),
    topicName: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}/data/empty', validatedParams);
      return callApi(endpoint, 'DELETE');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Get a single record by partition and offset
registerTool(
  'get_topic_data_record_by_partition_offset',
  'Get a single record by partition and offset',
  {
    cluster: z.string(),
    topicName: z.string(),
    partition: z.number(),
    offset: z.number()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string(),
    partition: z.number(),
    offset: z.number()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}/data/record/{partition}/{offset}', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Search for data for a topic
registerTool(
  'get_topic_data_search',
  'Search for data for a topic',
  {
    cluster: z.string(),
    topicName: z.string(),
    after: z.string().nullable().optional(),
    partition: z.number().nullable().optional(),
    sort: z.string().nullable().optional(),
    timestamp: z.string().nullable().optional(),
    endTimestamp: z.string().nullable().optional(),
    searchByKey: z.string().nullable().optional(),
    searchByValue: z.string().nullable().optional(),
    searchByHeaderKey: z.string().nullable().optional(),
    searchByHeaderValue: z.string().nullable().optional(),
    searchByKeySubject: z.string().nullable().optional(),
    searchByValueSubject: z.string().nullable().optional()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string(),
    after: z.string().nullable().optional(),
    partition: z.number().nullable().optional(),
    sort: z.string().nullable().optional(),
    timestamp: z.string().nullable().optional(),
    endTimestamp: z.string().nullable().optional(),
    searchByKey: z.string().nullable().optional(),
    searchByValue: z.string().nullable().optional(),
    searchByHeaderKey: z.string().nullable().optional(),
    searchByHeaderValue: z.string().nullable().optional(),
    searchByKeySubject: z.string().nullable().optional(),
    searchByValueSubject: z.string().nullable().optional()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}/data/search', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all consumer groups from a topic
registerTool(
  'get_topic_groups',
  'List all consumer groups from a topic',
  {
    cluster: z.string(),
    topicName: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}/groups', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all logs from a topic
registerTool(
  'get_topic_logs',
  'List all logs from a topic',
  {
    cluster: z.string(),
    topicName: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}/logs', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Get topic partition offsets by timestamp
registerTool(
  'get_topic_offsets_start',
  'Get topic partition offsets by timestamp',
  {
    cluster: z.string(),
    topicName: z.string(),
    timestamp: z.string().nullable().optional()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string(),
    timestamp: z.string().nullable().optional()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}/offsets/start', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// List all partition from a topic
registerTool(
  'get_topic_partitions',
  'List all partition from a topic',
  {
    cluster: z.string(),
    topicName: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}/partitions', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Increase partition for a topic
registerTool(
  'post_topic_partitions',
  'Increase partition for a topic',
  {
    cluster: z.string(),
    topicName: z.string(),
    body: z.record(z.any())
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string(),
    topicName: z.string(),
    body: z.record(z.any())
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/topic/{topicName}/partitions', validatedParams);
      return callApi(endpoint, 'POST', validatedParams, 'application/json');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Get ui options for cluster
registerTool(
  'get_ui-options',
  'Get ui options for cluster',
  {
    cluster: z.string()
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    cluster: z.string()
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{cluster}/ui-options', validatedParams);
      return callApi(endpoint, 'GET');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

// Copy from a topic to another topic
registerTool(
  'post_topic_copy_topic_by_toTopicName',
  'Copy from a topic to another topic',
  {
    fromCluster: z.string(),
    fromTopicName: z.string(),
    toCluster: z.string(),
    toTopicName: z.string(),
    body: z.array(z.object({
      partition: z.number().optional(),
      offset: z.number().optional()
    }))
  },
  async (params) => {
    try {
      const validatedParams = z.object({
    fromCluster: z.string(),
    fromTopicName: z.string(),
    toCluster: z.string(),
    toTopicName: z.string(),
    body: z.array(z.object({
      partition: z.number().optional(),
      offset: z.number().optional()
    }))
  }).parse(params);
      const endpoint = parameterizeEndpoint('/api/{fromCluster}/topic/{fromTopicName}/copy/{toCluster}/topic/{toTopicName}', validatedParams);
      return callApi(endpoint, 'POST', validatedParams, 'application/json');
    } catch (error) {
      if (error instanceof z.ZodError) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Validation error', details: error.errors })
            }
          ]
        };
      }
      throw error;
    }
  }
);

const transport = new StdioServerTransport();
await server.connect(transport);
