import axios from 'axios';

// Talks to the FastAPI Gateway
const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8002/api';

// You'll need to store the API Key in localStorage or hardcode for demo
const API_KEY = "sk_12345"; // Ideally, simulate a login to get this

const api = axios.create({
    baseURL: API_URL,
    headers: {
        'Content-Type': 'application/json',
        'X-API-Key': API_KEY
    },
});

export const dashboardService = {
    triggerSync: (loanType, force) => 
        api.post('/sync', { loan_type: loanType, force }),
        
    getProfiling: (loanType) => 
        api.get(`/profiling?loan_type=${loanType}`),
        
    getData: (loanType) => 
        api.get(`/data?loan_type=${loanType}&limit=50`)
};