import React, { useState } from 'react';
import { useAuth } from '../context/AuthContext';
import { LayoutDashboard, KeyRound, ArrowRight } from 'lucide-react';

export default function WelcomePage() {
    const { setApiKey } = useAuth();
    const [key, setKey] = useState('');
    const [error, setError] = useState('');

    const handleSubmit = (e) => {
        e.preventDefault();
        setError('');
        const trimmed = key.trim();
        if (!trimmed) {
            setError('Please enter your API key.');
            return;
        }
        setApiKey(trimmed);
    };

    return (
        <div className="min-h-screen bg-fintech-primary flex items-center justify-center p-4">
            <div className="w-full max-w-md">
                <div className="text-center mb-8">
                    <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-gradient-to-tr from-blue-600 to-cyan-400 mb-4">
                        <LayoutDashboard className="w-8 h-8 text-white" />
                    </div>
                    <h1 className="text-2xl font-bold text-fintech-text tracking-tight">
                        TeamSec <span className="text-fintech-accent">Analytics</span>
                    </h1>
                    <p className="text-fintech-muted text-sm mt-2">
                        Connect to your data warehouse
                    </p>
                </div>

                <form
                    onSubmit={handleSubmit}
                    className="bg-fintech-secondary rounded-xl border border-slate-700 p-6 shadow-xl"
                >
                    <label className="block text-sm font-medium text-fintech-muted mb-2">
                        API Key
                    </label>
                    <div className="relative">
                        <KeyRound className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500" />
                        <input
                            type="password"
                            value={key}
                            onChange={(e) => setKey(e.target.value)}
                            placeholder="Paste your tenant API key from init_tenants"
                            className="w-full pl-10 pr-4 py-3 bg-slate-900/50 border border-slate-600 rounded-lg text-fintech-text placeholder-slate-500 focus:ring-2 focus:ring-fintech-accent focus:border-transparent outline-none transition-all"
                            autoComplete="off"
                        />
                    </div>
                    {error && (
                        <p className="mt-2 text-sm text-fintech-danger">{error}</p>
                    )}
                    <button
                        type="submit"
                        className="mt-4 w-full flex items-center justify-center gap-2 py-3 px-4 bg-fintech-accent hover:bg-blue-600 text-white font-semibold rounded-lg transition-colors"
                    >
                        Continue <ArrowRight className="w-4 h-4" />
                    </button>
                </form>

                <p className="mt-4 text-center text-xs text-fintech-muted">
                    Run <code className="bg-slate-800 px-1 rounded">init_tenants</code> in the adapter to obtain a key.
                </p>
            </div>
        </div>
    );
}
