import React, { useState, useMemo } from 'react';
import { Table, ChevronLeft, ChevronRight } from 'lucide-react';
import ColumnHeader from './ColumnHeader';

const PAGE_SIZES = [25, 50, 100, 200, 500];

const formatDate = (s) => (s ? new Date(s).toLocaleDateString() : '–');
const formatCurrency = (n) =>
    n != null ? `$${Number(n).toLocaleString(undefined, { minimumFractionDigits: 2 })}` : '–';
const formatPercent = (n) => (n != null ? `${(Number(n) * 100).toFixed(2)}%` : '–');

const COLUMNS = [
    { key: 'loan_account_number', label: 'Account #', align: 'left', render: (r) => r.loan_account_number },
    { key: 'customer_id', label: 'Customer ID', align: 'left', render: (r) => r.customer_id || '–' },
    { key: 'loan_status_code', label: 'Status', align: 'left', render: (r) => r.loan_status_code || '–' },
    { key: 'original_loan_amount', label: 'Original', align: 'right', render: (r) => formatCurrency(r.original_loan_amount) },
    { key: 'outstanding_principal_balance', label: 'Balance', align: 'right', render: (r) => formatCurrency(r.outstanding_principal_balance) },
    { key: 'nominal_interest_rate', label: 'Rate', align: 'right', render: (r) => formatPercent(r.nominal_interest_rate) },
    { key: 'days_past_due', label: 'DPD', align: 'right', render: (r) => (r.days_past_due != null ? r.days_past_due : '–') },
    { key: 'total_installment_count', label: 'Installments', align: 'right', render: (r) => r.total_installment_count ?? '–' },
    { key: 'outstanding_installment_count', label: 'Outstanding', align: 'right', render: (r) => r.outstanding_installment_count ?? '–' },
    { key: 'loan_start_date', label: 'Start', align: 'left', render: (r) => formatDate(r.loan_start_date) },
    { key: 'final_maturity_date', label: 'Maturity', align: 'left', render: (r) => formatDate(r.final_maturity_date) },
    { key: 'sector_code', label: 'Sector', align: 'left', render: (r) => r.sector_code || '–' },
    { key: 'internal_rating', label: 'Rating', align: 'left', render: (r) => r.internal_rating || '–' },
];

export default function LiveDataTable({
    loans = [],
    totalCount = 0,
    page,
    pageSize,
    onPageChange,
    onPageSizeChange,
    isLoading,
    profilingStats,
}) {
    const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
    const canPrev = page > 0;
    const canNext = page < totalPages - 1;
    const start = page * pageSize + 1;
    const end = Math.min((page + 1) * pageSize, totalCount);

    return (
        <div className="bg-fintech-secondary rounded-xl border border-slate-700 shadow-lg overflow-hidden">
            <div className="px-4 sm:px-6 py-4 border-b border-slate-700 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
                <h3 className="text-fintech-muted text-sm uppercase tracking-wider font-semibold flex items-center gap-2">
                    <Table className="w-4 h-4" /> Live Data Feed
                </h3>
                <div className="flex flex-wrap items-center gap-4">
                    <span className="text-xs text-fintech-muted">
                        {totalCount.toLocaleString()} total · Page {page + 1} of {totalPages}
                    </span>
                    <select
                        value={pageSize}
                        onChange={(e) => onPageSizeChange(Number(e.target.value))}
                        className="bg-slate-800 border border-slate-600 rounded px-2 py-1 text-sm text-fintech-text"
                    >
                        {PAGE_SIZES.map((s) => (
                            <option key={s} value={s}>
                                {s} per page
                            </option>
                        ))}
                    </select>
                    <div className="flex items-center gap-1">
                        <button
                            onClick={() => onPageChange(page - 1)}
                            disabled={!canPrev || isLoading}
                            className="p-2 rounded hover:bg-slate-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                        >
                            <ChevronLeft className="w-4 h-4" />
                        </button>
                        <button
                            onClick={() => onPageChange(page + 1)}
                            disabled={!canNext || isLoading}
                            className="p-2 rounded hover:bg-slate-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                        >
                            <ChevronRight className="w-4 h-4" />
                        </button>
                    </div>
                </div>
            </div>

            <div className="overflow-x-auto">
                <table className="w-full text-left text-sm min-w-[900px]">
                    <thead className="bg-slate-800/50 text-fintech-muted uppercase text-xs">
                        <tr>
                            {COLUMNS.map((col) => (
                                <ColumnHeader
                                    key={col.key}
                                    label={col.label}
                                    fieldKey={col.key}
                                    profilingStats={profilingStats}
                                />
                            ))}
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-700">
                        {isLoading ? (
                            <tr>
                                <td colSpan={COLUMNS.length} className="p-8 text-center text-fintech-muted">
                                    Loading…
                                </td>
                            </tr>
                        ) : loans.length === 0 ? (
                            <tr>
                                <td colSpan={COLUMNS.length} className="p-8 text-center text-fintech-muted">
                                    No records
                                </td>
                            </tr>
                        ) : (
                            loans.map((loan, i) => (
                                <tr key={loan.loan_account_number || i} className="hover:bg-slate-700/30 transition-colors">
                                    {COLUMNS.map((col) => (
                                        <td
                                            key={col.key}
                                            className={`px-4 py-3 ${
                                                col.align === 'right' ? 'text-right font-mono' : ''
                                            } ${col.key === 'loan_status_code' ? 'pr-2' : ''}`}
                                        >
                                            {col.key === 'loan_status_code' ? (
                                                <span
                                                    className={`inline-flex px-2 py-0.5 rounded text-xs font-medium ${
                                                        loan.loan_status_code === 'Closed'
                                                            ? 'bg-slate-700 text-slate-300'
                                                            : loan.days_past_due > 0
                                                            ? 'bg-red-900/30 text-red-400'
                                                            : 'bg-emerald-900/30 text-emerald-400'
                                                    }`}
                                                >
                                                    {loan.loan_status_code || '–'}
                                                </span>
                                            ) : (
                                                col.render(loan)
                                            )}
                                        </td>
                                    ))}
                                </tr>
                            ))
                        )}
                    </tbody>
                </table>
            </div>

            <div className="px-4 sm:px-6 py-3 border-t border-slate-700 text-xs text-fintech-muted">
                Showing {start}–{end} of {totalCount.toLocaleString()} records
            </div>
        </div>
    );
}
