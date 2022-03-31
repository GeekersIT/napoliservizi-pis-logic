import { GraphQLClient } from 'graphql-request';
// const { GraphQLClient } = require('graphql-request')
import config from './config.js';

const queryFetch = {
  queryFetch: async function (query, variables) {
    const endpoint = config.db.url;
    const graphQLClient = new GraphQLClient(endpoint, {
      headers: {
        'x-hasura-admin-secret': process.env.ADMIN_PWD,
      },
    })
    return await graphQLClient.request(query, variables);
  }
}
export default queryFetch;