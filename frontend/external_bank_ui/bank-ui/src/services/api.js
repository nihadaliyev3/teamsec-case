import axios from 'axios';

// Pointing to your External Bank Django Container
// Note: In local dev, use localhost:8001. In Docker, this would need nginx proxy.
const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8001/api';

const api = axios.create({
    baseURL: API_URL,
    headers: {
        'Content-Type': 'application/json',
    },
});

export const bankService = {
    uploadFile: async (fileType, file) => {
        const formData = new FormData();
        formData.append('file_type', fileType);
        formData.append('file', file);
        
        return api.post('/upload/', formData, {
            headers: { 'Content-Type': 'multipart/form-data' }
        });
    },

    updateData: async (fileType) => {
        return api.post('/update/', { file_type: fileType });
    }
};