import React, { useState } from 'react';
import { BarChart2, ChevronDown, ChevronUp } from 'lucide-react';

/**
 * Collapsible panel showing field-level profiling stats from the last sync.
 * Complements column header tooltips with a full overview.
 */
export default function ProfilingStatsPanel({ profilingStats }) {
    const [open, setOpen] = useState(false);
    const credits = profilingStats?.credits ?? {};
    const fields = Object.entries(credits).filter(
        ([k]) => !['_meta', 'tenant_id', 'loan_type', 'inserted_at'].includes(k)
    );

    if (fields.length === 0) return null;

    const formatStat = (stats) => {
        if (!stats || typeof stats !== 'object') return null;
        const parts = [];
        if (stats.min != null && stats.max != null)
            parts.push(`${stats.min} – ${stats.max}`);
        if (stats.avg != null) parts.push(`avg ${stats.avg}`);
        if (stats.unique_count != null) parts.push(`${stats.unique_count} unique`);
        if (stats.null_ratio != null) parts.push(`${(stats.null_ratio * 100).toFixed(1)}% null`);
        if (stats.most_frequent != null) parts.push(`top: ${stats.most_frequent}`);
        return parts.join(' · ') || null;
    };

    return (
        <div className="bg-fintech-secondary rounded-xl border border-slate-700 overflow-hidden">
            <button
                onClick={() => setOpen(!open)}
                className="w-full px-4 py-3 flex items-center justify-between hover:bg-slate-800/50 transition-colors text-left"
            >
                <span className="text-fintech-muted text-sm uppercase tracking-wider font-semibold flex items-center gap-2">
                    <BarChart2 className="w-4 h-4" /> Data Profile (from last sync)
                </span>
                {open ? (
                    <ChevronUp className="w-4 h-4 text-slate-500" />
                ) : (
                    <ChevronDown className="w-4 h-4 text-slate-500" />
                )}
            </button>
            {open && (
                <div className="px-4 pb-4 pt-0 border-t border-slate-700">
                    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 mt-3 text-xs">
                        {fields.map(([field, stats]) => {
                            const s = formatStat(stats);
                            if (!s) return null;
                            return (
                                <div
                                    key={field}
                                    className="p-3 rounded-lg bg-slate-800/50 border border-slate-700"
                                >
                                    <div className="font-medium text-fintech-accent mb-1">
                                        {field.replace(/_/g, ' ')}
                                    </div>
                                    <div className="text-fintech-muted break-words">{s}</div>
                                </div>
                            );
                        })}
                    </div>
                </div>
            )}
        </div>
    );
}
