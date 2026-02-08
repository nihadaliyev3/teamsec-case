import React, { useState } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ToastContainer, toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { AuthProvider, useAuth } from './context/AuthContext';
import {
    useLoanData,
    useLoanCount,
    useProfilingStats,
    useTriggerSync,
} from './hooks/useDashboardData';
import SyncStatusCard from './components/SyncStatusCard';
import DataQualityPanel from './components/DataQualityPanel';
import LoanDistributionChart from './components/LoanDistributionChart';
import LiveDataTable from './components/LiveDataTable';
import ProfilingStatsPanel from './components/ProfilingStatsPanel';
import WelcomePage from './components/WelcomePage';
import { LayoutDashboard, LogOut } from 'lucide-react';

const queryClient = new QueryClient();

function DashboardContent() {
    const { apiKey, logout } = useAuth();
    const [loanType, setLoanType] = useState('COMMERCIAL');
    const [page, setPage] = useState(0);
    const [pageSize, setPageSize] = useState(50);

    const { data: loans, isLoading: loansLoading } = useLoanData(loanType, apiKey, {
        limit: pageSize,
        offset: page * pageSize,
    });
    const { data: chartData } = useLoanData(loanType, apiKey, { limit: 5000, offset: 0 });
    const { data: totalCount = 0 } = useLoanCount(loanType, apiKey);
    const { data: profiling } = useProfilingStats(loanType, apiKey);
    const triggerSync = useTriggerSync(apiKey);

    const lastRun = profiling?.[0];
    const profilingStats = lastRun?.profiling_stats;

    const handleSync = () => {
        triggerSync.mutate(
            { loanType, force: true },
            {
                onSuccess: () => toast.success('Sync queued. Data will refresh shortly.'),
                onError: (err) =>
                    toast.error(err?.response?.data?.detail || 'Sync failed. Check API key and services.'),
            }
        );
    };

    return (
        <div className="min-h-screen bg-fintech-primary text-fintech-text font-sans selection:bg-fintech-accent selection:text-white pb-20">
            <header className="border-b border-slate-700 bg-fintech-secondary/50 backdrop-blur-md sticky top-0 z-50">
                <div className="max-w-[1600px] mx-auto px-4 sm:px-6 py-4 flex flex-col sm:flex-row sm:justify-between sm:items-center gap-4">
                    <div className="flex items-center justify-between sm:justify-start gap-3">
                        <div className="flex items-center gap-3">
                            <div className="bg-gradient-to-tr from-blue-600 to-cyan-400 p-2 rounded-lg">
                                <LayoutDashboard className="w-6 h-6 text-white" />
                            </div>
                            <h1 className="text-xl font-bold tracking-tight">
                                TeamSec <span className="text-fintech-accent">Analytics</span>
                            </h1>
                        </div>
                        <button
                            onClick={logout}
                            className="sm:hidden p-2 rounded-lg hover:bg-slate-700 transition-colors"
                            title="Log out"
                        >
                            <LogOut className="w-5 h-5 text-slate-400" />
                        </button>
                    </div>
                    <div className="flex items-center gap-3">
                        <select
                            value={loanType}
                            onChange={(e) => {
                                setLoanType(e.target.value);
                                setPage(0);
                            }}
                            className="bg-slate-800 border border-slate-600 rounded-lg px-4 py-2 text-sm focus:ring-2 focus:ring-fintech-accent outline-none"
                        >
                            <option value="COMMERCIAL">Commercial Loans</option>
                            <option value="RETAIL">Retail Loans</option>
                        </select>
                        <button
                            onClick={logout}
                            className="hidden sm:flex items-center gap-2 px-3 py-2 rounded-lg hover:bg-slate-700 transition-colors text-sm text-fintech-muted"
                        >
                            <LogOut className="w-4 h-4" /> Log out
                        </button>
                    </div>
                </div>
            </header>

            <main className="max-w-[1600px] mx-auto px-4 sm:px-6 py-6 lg:py-8 space-y-6 lg:space-y-8">
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 lg:gap-6">
                    <SyncStatusCard
                        lastRun={lastRun}
                        isSyncing={triggerSync.isPending}
                        onSync={handleSync}
                    />
                    <div className="lg:col-span-2">
                        <DataQualityPanel stats={lastRun} />
                    </div>
                </div>

                <LoanDistributionChart loans={chartData || []} />

                <ProfilingStatsPanel profilingStats={profilingStats} />

                <LiveDataTable
                    loans={loans || []}
                    totalCount={totalCount}
                    page={page}
                    pageSize={pageSize}
                    onPageChange={setPage}
                    onPageSizeChange={(s) => {
                        setPageSize(s);
                        setPage(0);
                    }}
                    isLoading={loansLoading}
                    profilingStats={profilingStats}
                />
            </main>

        </div>
    );
}

function AppContent() {
    const { isAuthenticated } = useAuth();
    return isAuthenticated ? <DashboardContent /> : <WelcomePage />;
}

export default function App() {
    return (
        <QueryClientProvider client={queryClient}>
            <AuthProvider>
                <AppContent />
            </AuthProvider>
            <ToastContainer
                position="bottom-right"
                theme="dark"
                autoClose={3000}
                toastClassName="bg-slate-800 border border-slate-600"
            />
        </QueryClientProvider>
    );
}
