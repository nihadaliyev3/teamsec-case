import React, { useState, useEffect } from 'react';
import { dashboardService } from '../services/api';
import { ToastContainer, toast } from 'react-toastify';
import { Bar } from 'react-chartjs-2';
import 'chart.js/auto'; // Auto-register charts
import 'react-toastify/dist/ReactToastify.css';

const MainDashboard = () => {
    const [loanType, setLoanType] = useState('COMMERCIAL');
    const [stats, setStats] = useState([]);
    const [dataRows, setDataRows] = useState([]);

    const handleSync = async () => {
        try {
            await dashboardService.triggerSync(loanType, true);
            toast.success("Sync Job Started! Check logs.");
        } catch (err) {
            toast.error("Sync Failed");
        }
    };

    const loadData = async () => {
        try {
            const [profRes, dataRes] = await Promise.all([
                dashboardService.getProfiling(loanType),
                dashboardService.getData(loanType)
            ]);
            setStats(profRes.data);
            setDataRows(dataRes.data);
        } catch (err) {
            toast.error("Failed to load dashboard data");
        }
    };

    useEffect(() => { loadData(); }, [loanType]);

    // Simple Chart Data Visualization
    const chartData = {
        labels: dataRows.slice(0, 10).map(r => r.loan_account_number),
        datasets: [{
            label: 'Outstanding Balance',
            data: dataRows.slice(0, 10).map(r => r.outstanding_principal_balance),
            backgroundColor: 'rgba(53, 162, 235, 0.5)',
        }]
    };

    return (
        <div className="p-8 bg-gray-50 min-h-screen">
            <ToastContainer />
            <div className="flex justify-between items-center mb-8">
                <h1 className="text-3xl font-bold text-gray-800">Fintech Analytics</h1>
                <div className="space-x-4">
                    <select 
                        value={loanType} 
                        onChange={(e) => setLoanType(e.target.value)}
                        className="p-2 border rounded"
                    >
                        <option value="COMMERCIAL">Commercial</option>
                        <option value="RETAIL">Retail</option>
                    </select>
                    <button 
                        onClick={handleSync}
                        className="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700"
                    >
                        Trigger Sync
                    </button>
                    <button 
                        onClick={loadData}
                        className="bg-green-600 text-white px-4 py-2 rounded hover:bg-green-700"
                    >
                        Refresh Data
                    </button>
                </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
                {/* Chart Section */}
                <div className="bg-white p-6 rounded shadow">
                    <h2 className="text-xl font-semibold mb-4">Top 10 Exposures</h2>
                    <Bar data={chartData} />
                </div>

                {/* Profiling Stats Section */}
                <div className="bg-white p-6 rounded shadow">
                    <h2 className="text-xl font-semibold mb-4">Data Quality Reports</h2>
                    <div className="space-y-4">
                        {stats.map((s, i) => (
                            <div key={i} className="border-b pb-2">
                                <div className="flex justify-between">
                                    <span className="font-medium">{new Date(s.sync_date).toLocaleString()}</span>
                                    <span className={`px-2 rounded text-sm ${s.status === 'SUCCESS' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'}`}>
                                        {s.status}
                                    </span>
                                </div>
                                <div className="text-sm text-gray-500 mt-1">
                                    Rows: {s.total_rows} | Errors: {Object.keys(s.validation_errors).length}
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            </div>
        </div>
    );
};

export default MainDashboard;