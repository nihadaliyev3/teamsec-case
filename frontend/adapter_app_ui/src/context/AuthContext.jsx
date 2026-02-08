import React, { createContext, useContext, useState, useEffect, useCallback } from 'react';

const API_KEY_STORAGE = 'teamsec_api_key';

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
    const [apiKey, setApiKeyState] = useState(() => localStorage.getItem(API_KEY_STORAGE) || '');

    const setApiKey = useCallback((key) => {
        const trimmed = (key || '').trim();
        setApiKeyState(trimmed);
        if (trimmed) {
            localStorage.setItem(API_KEY_STORAGE, trimmed);
        } else {
            localStorage.removeItem(API_KEY_STORAGE);
        }
    }, []);

    const logout = useCallback(() => {
        setApiKeyState('');
        localStorage.removeItem(API_KEY_STORAGE);
    }, []);

    return (
        <AuthContext.Provider value={{ apiKey, setApiKey, logout, isAuthenticated: !!apiKey }}>
            {children}
        </AuthContext.Provider>
    );
}

export function useAuth() {
    const ctx = useContext(AuthContext);
    if (!ctx) throw new Error('useAuth must be used within AuthProvider');
    return ctx;
}
