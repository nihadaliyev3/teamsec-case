import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8002/api';

export function createApiClient(apiKey) {
    return axios.create({
        baseURL: API_URL,
        headers: {
            'Content-Type': 'application/json',
            'X-API-Key': apiKey || '',
        },
    });
}
