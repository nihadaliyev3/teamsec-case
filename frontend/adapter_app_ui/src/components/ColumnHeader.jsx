import React, { useState } from 'react';
import { HelpCircle } from 'lucide-react';

const formatStat = (stats) => {
    if (!stats || typeof stats !== 'object') return null;
    const parts = [];
    if (stats.min != null && stats.max != null) {
        parts.push(`Range: ${stats.min} – ${stats.max}`);
    }
    if (stats.avg != null) parts.push(`Avg: ${stats.avg}`);
    if (stats.unique_count != null) parts.push(`Unique: ${stats.unique_count}`);
    const nullPct = stats.null_ratio ?? stats.null_or_empty_ratio;
    if (nullPct != null) parts.push(`Null: ${(nullPct * 100).toFixed(1)}%`);
    if (stats.most_frequent != null) parts.push(`Top: ${stats.most_frequent}`);
    return parts.length ? parts.join(' · ') : null;
};

export default function ColumnHeader({ label, fieldKey, profilingStats }) {
    const [show, setShow] = useState(false);
    const stats = profilingStats?.credits?.[fieldKey];
    const tooltip = formatStat(stats);

    if (!tooltip) {
        return <th className="px-4 py-3 font-medium">{label}</th>;
    }

    return (
        <th
            className="px-4 py-3 font-medium relative group"
            onMouseEnter={() => setShow(true)}
            onMouseLeave={() => setShow(false)}
        >
            <span className="flex items-center gap-1">
                {label}
                <HelpCircle className="w-3.5 h-3.5 text-slate-500 cursor-help" />
            </span>
            {show && (
                <div className="absolute left-0 top-full mt-1 z-50 min-w-[200px] max-w-[320px] p-2 text-xs bg-slate-900 border border-slate-600 rounded-lg shadow-xl text-fintech-muted">
                    <div className="font-medium text-fintech-text mb-1">Profiling stats</div>
                    {tooltip}
                </div>
            )}
        </th>
    );
}
