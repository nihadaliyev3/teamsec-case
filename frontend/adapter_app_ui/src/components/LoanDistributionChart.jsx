import React from 'react';
import {
    BarChart,
    Bar,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    ResponsiveContainer,
} from 'recharts';

/**
 * Portfolio Distribution: Loan volume and count by status.
 * Shows how outstanding balance is distributed across loan statuses
 * (e.g. Active, Closed, Delinquent). Use for risk and exposure analysis.
 */
export default function LoanDistributionChart({ loans }) {
    const dataMap = (loans || []).reduce((acc, loan) => {
        const status = loan.loan_status_code || 'UNKNOWN';
        if (!acc[status]) {
            acc[status] = { status, count: 0, volume: 0 };
        }
        acc[status].count += 1;
        acc[status].volume += Number(loan.outstanding_principal_balance) || 0;
        return acc;
    }, {});

    const data = Object.values(dataMap).sort((a, b) => b.volume - a.volume);

    const formatVolume = (v) =>
        v >= 1e6 ? `${(v / 1e6).toFixed(1)}M` : v >= 1e3 ? `${(v / 1e3).toFixed(1)}K` : String(v);

    return (
        <div className="bg-fintech-secondary p-6 rounded-xl border border-slate-700 shadow-lg">
            <div className="mb-2">
                <h3 className="text-fintech-muted text-sm uppercase tracking-wider font-semibold">
                    Portfolio Distribution
                </h3>
                <p className="text-xs text-slate-500 mt-1 max-w-2xl">
                    Outstanding balance and loan count by status. Reflects exposure and risk concentration across Active, Closed, and other statuses.
                </p>
            </div>
            <div className="h-80 mt-4">
                {data.length === 0 ? (
                    <div className="h-full flex items-center justify-center text-fintech-muted text-sm">
                        No data to display
                    </div>
                ) : (
                    <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={data} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                            <XAxis
                                dataKey="status"
                                stroke="#94a3b8"
                                tick={{ fontSize: 12 }}
                            />
                            <YAxis
                                stroke="#94a3b8"
                                tickFormatter={formatVolume}
                                tick={{ fontSize: 12 }}
                            />
                            <Tooltip
                                contentStyle={{
                                    backgroundColor: '#0f172a',
                                    borderColor: '#334155',
                                    color: '#f8fafc',
                                    borderRadius: '8px',
                                }}
                                content={({ active, payload, label }) =>
                                    active && payload?.[0] && (
                                        <div className="px-3 py-2">
                                            <div className="font-medium text-fintech-text mb-1">
                                                {label}
                                            </div>
                                            <div className="text-xs text-fintech-muted space-y-0.5">
                                                <div>Volume: ${Number(payload[0].value).toLocaleString()}</div>
                                                <div>Count: {payload[0].payload?.count ?? 0} loans</div>
                                            </div>
                                        </div>
                                    )
                                }
                            />
                            <Bar
                                dataKey="volume"
                                fill="#3b82f6"
                                radius={[4, 4, 0, 0]}
                                name="Volume ($)"
                            />
                        </BarChart>
                    </ResponsiveContainer>
                )}
            </div>
        </div>
    );
}
