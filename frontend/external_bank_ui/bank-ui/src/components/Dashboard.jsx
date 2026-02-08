import React, { useState } from 'react';
import { bankService } from '../services/api';
import { ToastContainer, toast } from 'react-toastify';
import { FaCloudUploadAlt, FaRandom, FaInfoCircle } from 'react-icons/fa';
import 'react-toastify/dist/ReactToastify.css';

const FILE_TYPES = [
    { value: 'commercial_credit', label: 'Commercial Credit' },
    { value: 'commercial_payment', label: 'Commercial Payment' },
    { value: 'retail_credit', label: 'Retail Credit' },
    { value: 'retail_payment', label: 'Retail Payment' },
];

const Dashboard = () => {
    const [uploadType, setUploadType] = useState(FILE_TYPES[0].value);
    const [updateType, setUpdateType] = useState(FILE_TYPES[0].value);
    const [file, setFile] = useState(null);
    const [loading, setLoading] = useState(false);

    // --- Handlers ---

    const handleFileChange = (e) => {
        if (e.target.files) setFile(e.target.files[0]);
    };

    const handleUpload = async (e) => {
        e.preventDefault();
        if (!file) return toast.error("Please select a file first!");

        setLoading(true);
        try {
            await bankService.uploadFile(uploadType, file);
            toast.success("File uploaded successfully!");
            setFile(null);
            // Reset file input manually if needed
        } catch (err) {
            toast.error(err.response?.data?.detail || "Upload failed");
        } finally {
            setLoading(false);
        }
    };

    const handleSimulate = async () => {
        setLoading(true);
        try {
            const res = await bankService.updateData(updateType);
            toast.info(`Update Complete: ${res.data.message || "Rows updated"}`);
        } catch (err) {
            toast.error("Update failed");
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="container">
            <ToastContainer theme="dark" position="top-right" />
            
            <header style={{ marginBottom: '3rem', textAlign: 'center' }}>
                <h1 style={{ fontSize: '2.5rem', fontWeight: 'bold' }}>
                    <span style={{ color: '#3b82f6' }}>External Bank</span> Admin
                </h1>
                <p style={{ color: '#94a3b8' }}>Source System Control Panel</p>
            </header>

            {/* --- CARD 1: UPLOAD --- */}
            <div className="card">
                <div className="card-header">
                    <h2>Data Ingestion</h2>
                    <p>Upload raw CSV files to the bank's file system.</p>
                </div>
                
                <form onSubmit={handleUpload}>
                    <div className="form-group">
                        <label>File Category</label>
                        <select 
                            value={uploadType} 
                            onChange={(e) => setUploadType(e.target.value)}
                        >
                            {FILE_TYPES.map(t => (
                                <option key={t.value} value={t.value}>{t.label}</option>
                            ))}
                        </select>
                    </div>

                    <div className="form-group">
                        <label>CSV File</label>
                        <input 
                            type="file" 
                            accept=".csv" 
                            onChange={handleFileChange}
                        />
                    </div>

                    <button 
                        type="submit" 
                        className="btn btn-primary" 
                        disabled={loading || !file}
                    >
                        {loading ? 'Processing...' : <><FaCloudUploadAlt /> Upload Dataset</>}
                    </button>
                </form>
            </div>

            {/* --- CARD 2: UPDATE (SIMULATION) --- */}
            <div className="card" style={{ borderColor: '#10b981' }}>
                <div className="card-header">
                    <h2>Market Simulation</h2>
                    <p>Trigger simulated events to modify existing records.</p>
                </div>

                <div className="form-group">
                    <label>Target Dataset</label>
                    <select 
                        value={updateType} 
                        onChange={(e) => setUpdateType(e.target.value)}
                    >
                        {FILE_TYPES.map(t => (
                            <option key={t.value} value={t.value}>{t.label}</option>
                        ))}
                    </select>
                </div>

                <div className="tooltip-container">
                    <button 
                        onClick={handleSimulate} 
                        className="btn btn-success" 
                        disabled={loading}
                    >
                        {loading ? 'Simulating...' : <><FaRandom /> Simulate Day Passing</>}
                    </button>
                    <span className="tooltip-text">
                        <FaInfoCircle style={{ marginRight: 5 }} />
                        Simulates a "Day Passing". It randomly modifies 5 rows in the CSV 
                        to prove your Adapter can detect changes.
                    </span>
                </div>
            </div>
        </div>
    );
};

export default Dashboard;