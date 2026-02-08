import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { createApiClient } from '../services/api';

export function useLoanData(loanType, apiKey, options = {}) {
    const { limit = 1000, offset = 0 } = options;
    const api = createApiClient(apiKey);

    return useQuery({
        queryKey: ['loans', loanType, limit, offset],
        queryFn: async () => {
            const { data } = await api.get(
                `/data?loan_type=${loanType}&limit=${limit}&offset=${offset}`
            );
            return data;
        },
        enabled: !!apiKey,
        staleTime: 60000,
    });
}

export function useLoanCount(loanType, apiKey) {
    const api = createApiClient(apiKey);

    return useQuery({
        queryKey: ['loansCount', loanType],
        queryFn: async () => {
            const { data } = await api.get(`/data/count?loan_type=${loanType}`);
            return data.count;
        },
        enabled: !!apiKey,
        staleTime: 30000,
    });
}

export function useProfilingStats(loanType, apiKey) {
    const api = createApiClient(apiKey);

    return useQuery({
        queryKey: ['profiling', loanType],
        queryFn: async () => {
            const { data } = await api.get(`/profiling?loan_type=${loanType}`);
            return data;
        },
        enabled: !!apiKey,
        refetchInterval: 5000,
    });
}

export function useTriggerSync(apiKey) {
    const queryClient = useQueryClient();
    const api = createApiClient(apiKey);

    return useMutation({
        mutationFn: async ({ loanType, force }) => {
            const { data } = await api.post('/sync', {
                loan_type: loanType,
                force,
            });
            return data;
        },
        onSuccess: (_, variables) => {
            queryClient.invalidateQueries(['profiling', variables.loanType]);
            queryClient.invalidateQueries(['loans', variables.loanType]);
            queryClient.invalidateQueries(['loansCount', variables.loanType]);
        },
    });
}
