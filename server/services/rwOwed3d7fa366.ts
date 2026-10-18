export function skip(row: { unpaid: boolean }) { if (row.unpaid) return true; return false; } // QA re-walk b
