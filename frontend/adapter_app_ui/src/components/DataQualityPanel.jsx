import React from 'react';
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from 'recharts';

const DataQualityPanel = ({ stats }) => {
    if (!stats) return null;

    // Transform API error object into chart data
    const errors = stats.validation_errors?.general_errors || [];
    const errorCount = errors.length;
    const totalRows = stats.total_rows || 0;
    const successRate = totalRows > 0 ? ((totalRows - errorCount) / totalRows) * 100 : 0;

    const data = [
        { name: 'Valid', value: totalRows - errorCount, color: '#10b981' },
        { name: 'Invalid', value: errorCount, color: '#ef4444' },
    ];

    return (
        <div className="bg-fintech-secondary p-6 rounded-xl border border-slate-700 shadow-lg flex flex-col">
            <h3 className="text-fintech-muted text-sm uppercase tracking-wider font-semibold mb-4">Data Quality Health</h3>
            
            <div className="flex items-center h-48">
                <ResponsiveContainer width="50%" height="100%">
                    <PieChart>
                        <Pie 
                            data={data} 
                            innerRadius={40} 
                            outerRadius={60} 
                            paddingAngle={5} 
                            dataKey="value"
                        >
                            {data.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={entry.color} />
                            ))}
                        </Pie>
                        <Tooltip contentStyle={{ backgroundColor: '#1e293b', borderColor: '#334155' }} />
                    </PieChart>
                </ResponsiveContainer>
                
                <div className="w-1/2 space-y-4">
                    <div className="text-center">
                        <div className="text-3xl font-bold text-fintech-text">{successRate.toFixed(1)}%</div>
                        <div className="text-xs text-fintech-muted">Quality Score</div>
                    </div>
                    <div className="space-y-1 text-sm">
                        {errors.slice(0, 3).map((err, i) => (
                            <div key={i} className="text-fintech-warning bg-yellow-900/20 p-1 rounded px-2 truncate">
                                ⚠️ {err}
                            </div>
                        ))}
                        {errorCount > 3 && (
                            <div className="text-center text-xs text-fintech-muted">
                                +{errorCount - 3} more issues
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
};

export default DataQualityPanel;