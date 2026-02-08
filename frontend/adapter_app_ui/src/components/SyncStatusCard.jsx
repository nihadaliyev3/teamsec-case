import React from 'react';
import { RefreshCw, CheckCircle, AlertTriangle, Clock } from 'lucide-react';
import { formatDistanceToNow } from 'date-fns';

const SyncStatusCard = ({ lastRun, isSyncing, onSync }) => {
    const statusColor = lastRun?.status === 'SUCCESS' ? 'text-fintech-success' : 'text-fintech-danger';
    const Icon = lastRun?.status === 'SUCCESS' ? CheckCircle : AlertTriangle;

    return (
        <div className="bg-fintech-secondary p-6 rounded-xl border border-slate-700 shadow-lg">
            <div className="flex justify-between items-start mb-4">
                <div>
                    <h3 className="text-fintech-muted text-sm uppercase tracking-wider font-semibold">System Status</h3>
                    <div className="flex items-center gap-2 mt-1">
                        <div className={`w-3 h-3 rounded-full ${isSyncing ? 'animate-pulse bg-fintech-accent' : 'bg-fintech-success'}`} />
                        <span className="text-xl font-bold text-fintech-text">
                            {isSyncing ? 'Syncing...' : 'Operational'}
                        </span>
                    </div>
                </div>
                <button 
                    onClick={onSync}
                    disabled={isSyncing}
                    className={`p-2 rounded-lg transition-all ${isSyncing ? 'bg-slate-700 cursor-not-allowed' : 'bg-fintech-accent hover:bg-blue-600'}`}
                >
                    <RefreshCw className={`w-5 h-5 text-white ${isSyncing ? 'animate-spin' : ''}`} />
                </button>
            </div>

            <div className="space-y-3 pt-4 border-t border-slate-700">
                <div className="flex justify-between items-center text-sm">
                    <span className="text-fintech-muted flex items-center gap-2">
                        <Clock className="w-4 h-4" /> Last Sync
                    </span>
                    <span className="text-fintech-text font-mono">
                        {lastRun ? formatDistanceToNow(new Date(lastRun.sync_date), { addSuffix: true }) : 'Never'}
                    </span>
                </div>
                <div className="flex justify-between items-center text-sm">
                    <span className="text-fintech-muted">Outcome</span>
                    <span className={`flex items-center gap-1 font-bold ${statusColor}`}>
                        {lastRun && <Icon className="w-4 h-4" />}
                        {lastRun?.status || 'N/A'}
                    </span>
                </div>
            </div>
        </div>
    );
};

export default SyncStatusCard;