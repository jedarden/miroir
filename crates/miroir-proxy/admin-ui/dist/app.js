/**
 * Miroir Admin UI - Plan §13.19
 * Overview, Topology, Indexes, and Aliases sections implementation
 */

(function() {
    'use strict';

    // ============================================================================
    // State Management
    // ============================================================================

    const state = {
        currentSection: 'overview',
        topology: null,
        shards: null,
        rebalanceStatus: null,
        refreshInterval: null,
        isConnected: true,
        indexes: null,
        aliases: null,
        currentSettingsIndex: null,
        currentAliasForHistory: null,
        // Documents section state
        documents: { indexes: [], currentIndex: null, documents: [], limit: 50, offset: 0, filters: [], totalHits: 0 },
        // Query sandbox state
        query: { indexes: [], currentIndex: null, results: null, filters: [], sorts: [], facets: [] },
        // Tasks section state
        tasks: { indexes: [], tasks: [], filter: 'all', indexUid: '', type: '', limit: 50, offset: 0, total: 0 }
    };

    // ============================================================================
    // Navigation
    // ============================================================================

    function initNavigation() {
        const navLinks = document.querySelectorAll('.nav-links a');
        const sections = document.querySelectorAll('.section');
        const sectionTitle = document.getElementById('sectionTitle');

        navLinks.forEach(link => {
            link.addEventListener('click', (e) => {
                e.preventDefault();
                const sectionName = link.dataset.section;

                // Update active link
                navLinks.forEach(l => l.classList.remove('active'));
                link.classList.add('active');

                // Update active section
                sections.forEach(s => s.classList.remove('active'));
                document.getElementById(sectionName).classList.add('active');

                // Update title
                sectionTitle.textContent = link.textContent.trim();

                state.currentSection = sectionName;

                // Close mobile menu
                document.querySelector('.sidebar').classList.remove('open');

                // Refresh data when switching sections
                refreshData();
            });
        });

        // Handle initial hash
        const hash = window.location.hash.slice(1);
        if (hash) {
            const link = document.querySelector(`[data-section="${hash}"]`);
            if (link) link.click();
        }
    }

    // ============================================================================
    // API Client
    // ============================================================================

    const API_BASE = '/_miroir';

    async function fetchAPI(endpoint, options = {}) {
        try {
            const response = await fetch(`${API_BASE}${endpoint}`, {
                ...options,
                headers: {
                    'Accept': 'application/json',
                    ...options.headers
                }
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            return await response.json();
        } catch (error) {
            console.error(`API Error [${endpoint}]:`, error);
            throw error;
        }
    }

    // ============================================================================
    // Data Fetching
    // ============================================================================

    async function fetchTopology() {
        try {
            state.topology = await fetchAPI('/topology');
            updateConnectionStatus(true);
            return state.topology;
        } catch (error) {
            updateConnectionStatus(false);
            throw error;
        }
    }

    async function fetchShards() {
        try {
            state.shards = await fetchAPI('/shards');
            return state.shards;
        } catch (error) {
            console.error('Failed to fetch shards:', error);
            throw error;
        }
    }

    async function fetchRebalanceStatus() {
        try {
            state.rebalanceStatus = await fetchAPI('/rebalance/status');
            return state.rebalanceStatus;
        } catch (error) {
            console.error('Failed to fetch rebalance status:', error);
            return null;
        }
    }

    async function fetchIndexes() {
        try {
            const data = await fetchAPI('/indexes');
            state.indexes = data.results || [];
            return state.indexes;
        } catch (error) {
            console.error('Failed to fetch indexes:', error);
            throw error;
        }
    }

    async function fetchIndexStats(indexUid) {
        try {
            return await fetchAPI(`/indexes/${indexUid}/stats`);
        } catch (error) {
            console.error(`Failed to fetch stats for ${indexUid}:`, error);
            return null;
        }
    }

    async function fetchIndexSettings(indexUid) {
        try {
            return await fetchAPI(`/indexes/${indexUid}/settings`);
        } catch (error) {
            console.error(`Failed to fetch settings for ${indexUid}:`, error);
            return null;
        }
    }

    async function fetchAliases() {
        try {
            const data = await fetchAPI('/aliases');
            state.aliases = data.results || [];
            return state.aliases;
        } catch (error) {
            console.error('Failed to fetch aliases:', error);
            throw error;
        }
    }

    async function fetchAliasDetails(aliasName) {
        try {
            return await fetchAPI(`/aliases/${aliasName}`);
        } catch (error) {
            console.error(`Failed to fetch alias ${aliasName}:`, error);
            return null;
        }
    }

    // ===========================================================================
    // Documents Section
    // ===========================================================================

    async function fetchDocumentsIndexes() {
        try {
            const data = await fetchAPI('/indexes');
            state.documents.indexes = data.results || [];
            return state.documents.indexes;
        } catch (error) {
            console.error('Failed to fetch indexes for documents:', error);
            return [];
        }
    }

    async function fetchDocuments(indexUid, limit, offset, filters) {
        try {
            const filter = filters.length > 0 ? buildFilterString(filters) : undefined;
            const params = new URLSearchParams({
                limit: limit.toString(),
                offset: offset.toString()
            });
            if (filter) params.set('filter', filter);

            const data = await fetchAPI(`/indexes/${encodeURIComponent(indexUid)}/documents?${params}`);
            return {
                results: data.results || [],
                total: data.total || 0,
                limit: data.limit || limit,
                offset: data.offset || offset
            };
        } catch (error) {
            console.error(`Failed to fetch documents for ${indexUid}:`, error);
            return { results: [], total: 0, limit, offset };
        }
    }

    async function fetchDocumentFields(indexUid) {
        try {
            const settings = await fetchAPI(`/indexes/${encodeURIComponent(indexUid)}/settings`);
            return {
                filterable: settings.filterableAttributes || [],
                sortable: settings.sortableAttributes || [],
                displayed: settings.displayedAttributes || ['*']
            };
        } catch (error) {
            console.error(`Failed to fetch settings for ${indexUid}:`, error);
            return { filterable: [], sortable: [], displayed: ['*'] };
        }
    }

    async function importDocuments(indexUid, formData, onProgress) {
        const streaming = document.getElementById('importMethod').value === 'stream';
        const endpoint = streaming
            ? `/_miroir/indexes/${encodeURIComponent(indexUid)}/documents/stream`
            : `/indexes/${encodeURIComponent(indexUid)}/documents`;

        try {
            const response = await fetch(`${API_BASE}${endpoint}`, {
                method: 'POST',
                headers: {
                    'Accept': 'application/json'
                },
                body: formData
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            // For streaming, we need to handle the response differently
            if (streaming) {
                const reader = response.body.getReader();
                const decoder = new TextDecoder();
                let buffer = '';

                while (true) {
                    const { done, value } = await reader.read();
                    if (done) break;

                    buffer += decoder.decode(value, { stream: true });
                    const lines = buffer.split('\n');
                    buffer = lines.pop() || '';

                    for (const line of lines) {
                        if (line.trim()) {
                            try {
                                const event = JSON.parse(line);
                                if (event.type === 'progress' && onProgress) {
                                    onProgress(event);
                                }
                            } catch (e) {
                                console.warn('Failed to parse SSE line:', line);
                            }
                        }
                    }
                }
                return { success: true };
            } else {
                return await response.json();
            }
        } catch (error) {
            console.error('Failed to import documents:', error);
            throw error;
        }
    }

    // ===========================================================================
    // Query Sandbox Section
    // ===========================================================================

    async function fetchQueryIndexes() {
        try {
            const data = await fetchAPI('/indexes');
            state.query.indexes = data.results || [];
            return state.query.indexes;
        } catch (error) {
            console.error('Failed to fetch indexes for query:', error);
            return [];
        }
    }

    async function runQuery(indexUid, query) {
        try {
            const response = await fetch(`${API_BASE}/indexes/${encodeURIComponent(indexUid)}/search`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                body: JSON.stringify(query)
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            return await response.json();
        } catch (error) {
            console.error('Failed to run query:', error);
            throw error;
        }
    }

    async function explainQuery(indexUid, query) {
        try {
            const response = await fetch(`${API_BASE}/indexes/${encodeURIComponent(indexUid)}/explain`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                body: JSON.stringify(query)
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            return await response.json();
        } catch (error) {
            console.error('Failed to explain query:', error);
            throw error;
        }
    }

    async function runShadowDiff(indexUid, query) {
        try {
            const response = await fetch(`${API_BASE}/_miroir/shadow/query`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                body: JSON.stringify({ indexUid, query })
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            return await response.json();
        } catch (error) {
            console.error('Failed to run shadow diff:', error);
            throw error;
        }
    }

    // ===========================================================================
    // Tasks Section
    // ===========================================================================

    async function fetchTasksIndexes() {
        try {
            const data = await fetchAPI('/indexes');
            state.tasks.indexes = data.results || [];
            return state.tasks.indexes;
        } catch (error) {
            console.error('Failed to fetch indexes for tasks:', error);
            return [];
        }
    }

    async function fetchTasks() {
        try {
            const params = new URLSearchParams({
                limit: state.tasks.limit.toString(),
                from: state.tasks.offset.toString()
            });

            // Add filters
            if (state.tasks.filter !== 'all') {
                const statuses = {
                    'active': ['enqueued', 'processing'],
                    'pending': ['enqueued'],
                    'succeeded': ['succeeded'],
                    'failed': ['failed']
                };
                if (statuses[state.tasks.filter]) {
                    params.set('statuses', statuses[state.tasks.filter].join(','));
                }
            }

            if (state.tasks.indexUid) {
                params.set('indexUids', state.tasks.indexUid);
            }

            if (state.tasks.type) {
                const types = {
                    'documentAddition': 'documentAddition',
                    'documentUpdate': 'documentUpdate',
                    'documentDeletion': 'documentDeletion',
                    'settingsUpdate': 'settingsUpdate',
                    'indexCreation': 'indexCreation',
                    'indexDeletion': 'indexDeletion',
                    'indexUpdate': 'indexUpdate',
                    'indexSwap': 'indexSwap'
                };
                if (types[state.tasks.type]) {
                    params.set('types', types[state.tasks.type]);
                }
            }

            const data = await fetchAPI(`/tasks?${params}`);
            state.tasks.tasks = data.results || [];
            state.tasks.total = data.total || 0;
            return state.tasks;
        } catch (error) {
            console.error('Failed to fetch tasks:', error);
            return { tasks: [], total: 0 };
        }
    }

    async function fetchTaskDetails(taskUid) {
        try {
            return await fetchAPI(`/tasks/${taskUid}`);
        } catch (error) {
            console.error(`Failed to fetch task ${taskUid}:`, error);
            return null;
        }
    }

    async function cancelTask(taskUid) {
        try {
            return await fetchAPI(`/tasks/${taskUid}/cancel`, {
                method: 'POST'
            });
        } catch (error) {
            console.error(`Failed to cancel task ${taskUid}:`, error);
            throw error;
        }
    }

    async function deleteTask(taskUid) {
        try {
            return await fetchAPI(`/tasks/${taskUid}`, {
                method: 'DELETE'
            });
        } catch (error) {
            console.error(`Failed to delete task ${taskUid}:`, error);
            throw error;
        }
    }

    // ===========================================================================
    // Utility Functions
    // ===========================================================================

    function buildFilterString(filters) {
        if (!filters || filters.length === 0) return '';

        const parts = filters.map(f => {
            const { field, operator, value } = f;
            switch (operator) {
                case 'equals': return `${field} = "${value}"`;
                case 'notEquals': return `${field} != "${value}"`;
                case 'gt': return `${field} > "${value}"`;
                case 'gte': return `${field} >= "${value}"`;
                case 'lt': return `${field} < "${value}"`;
                case 'lte': return `${field} <= "${value}"`;
                case 'in': return `${field} IN [${value.split(',').map(v => `"${v.trim()}"`).join(', ')}]`;
                case 'exists': return `${field} EXISTS`;
                default: return '';
            }
        }).filter(p => p);

        return parts.join(' AND ');
    }

    function formatDuration(ms) {
        if (!ms) return '-';
        const seconds = Math.floor(ms / 1000);
        if (seconds < 60) return `${seconds}s`;
        const minutes = Math.floor(seconds / 60);
        const remainingSeconds = seconds % 60;
        return `${minutes}m ${remainingSeconds}s`;
    }

    function formatDate(timestamp) {
        if (!timestamp) return '-';
        return new Date(timestamp).toISOString().replace('T', ' ').substring(0, 19);
    }

    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    function extractReplicaGroup(node) {
        // Try to extract from address or use a default
        const match = node.address.match(/(\d+)$/);
        return match ? parseInt(match[1]) : 0;
    }

    function formatLastSeen(ms) {
        if (!ms) return 'Unknown';
        const seconds = Math.floor(ms / 1000);
        if (seconds < 60) return `${seconds}s ago`;
        const minutes = Math.floor(seconds / 60);
        if (minutes < 60) return `${minutes}m ago`;
        const hours = Math.floor(minutes / 60);
        return `${hours}h ago`;
    }

    function showModal(modalId) {
        document.getElementById(modalId).classList.add('active');
    }

    function hideModal(modalId) {
        document.getElementById(modalId).classList.remove('active');
    }

    // ===========================================================================
    // Rendering - Documents Section
    // ===========================================================================

    function renderDocuments() {
        const select = document.getElementById('documentIndexSelect');
        select.innerHTML = '<option value="">Select an index...</option>';
        state.documents.indexes.forEach(idx => {
            const option = document.createElement('option');
            option.value = idx.uid;
            option.textContent = idx.uid;
            if (state.documents.currentIndex === idx.uid) {
                option.selected = true;
            }
            select.appendChild(option);
        });

        if (state.documents.currentIndex && state.documents.documents.length > 0) {
            renderDocumentsTable();
        }
    }

    async function loadDocuments() {
        if (!state.documents.currentIndex) return;

        const data = await fetchDocuments(
            state.documents.currentIndex,
            state.documents.limit,
            state.documents.offset,
            state.documents.filters
        );

        state.documents.documents = data.results;
        state.documents.totalHits = data.total;

        renderDocumentsTable();
        renderDocumentsPagination();
    }

    async function renderDocumentsTable() {
        const thead = document.getElementById('documentsTableHead');
        const tbody = document.getElementById('documentsTableBody');

        if (!state.documents.documents || state.documents.documents.length === 0) {
            thead.innerHTML = '<tr><th>Select an index to browse documents</th></tr>';
            tbody.innerHTML = '<tr><td class="loading">Select an index to browse documents</td></tr>';
            return;
        }

        // Get columns from first document
        const firstDoc = state.documents.documents[0];
        const columns = Object.keys(firstDoc);

        thead.innerHTML = '<tr>' + columns.map(col => `<th>${escapeHtml(col)}</th>`).join('') + '</tr>';

        tbody.innerHTML = state.documents.documents.map(doc => {
            return '<tr>' + columns.map(col => {
                const value = doc[col];
                if (value === null || value === undefined) return '<td class="text-secondary">null</td>';
                if (typeof value === 'object') return `<td><pre>${escapeHtml(JSON.stringify(value, null, 2))}</pre></td>`;
                return `<td>${escapeHtml(String(value))}</td>`;
            }).join('') + '</tr>';
        }).join('');

        document.getElementById('documentCount').textContent =
            `${state.documents.totalHits} total documents`;
    }

    function renderDocumentsPagination() {
        const pagination = document.getElementById('documentPagination');
        const prevBtn = document.getElementById('documentPrevPage');
        const nextBtn = document.getElementById('documentNextPage');
        const pageInfo = document.getElementById('documentPageInfo');

        if (state.documents.totalHits === 0) {
            pagination.style.display = 'none';
            return;
        }

        pagination.style.display = 'flex';

        const from = state.documents.offset + 1;
        const to = Math.min(state.documents.offset + state.documents.limit, state.documents.totalHits);

        pageInfo.textContent = `Showing ${from}-${to} of ${state.documents.totalHits}`;

        prevBtn.disabled = state.documents.offset === 0;
        nextBtn.disabled = to >= state.documents.totalHits;
    }

    // ===========================================================================
    // Rendering - Query Sandbox Section
    // ===========================================================================

    function renderQuerySandbox() {
        const select = document.getElementById('queryIndexSelect');
        select.innerHTML = '<option value="">Select an index...</option>';
        state.query.indexes.forEach(idx => {
            const option = document.createElement('option');
            option.value = idx.uid;
            option.textContent = idx.uid;
            if (state.query.currentIndex === idx.uid) {
                option.selected = true;
            }
            select.appendChild(option);
        });

        if (state.query.currentIndex) {
            loadQueryFields();
        }
    }

    async function loadQueryFields() {
        if (!state.query.currentIndex) return;

        const settings = await fetchDocumentFields(state.query.currentIndex);

        // Update filter field selects
        document.querySelectorAll('#queryFilterBuilder .filter-field').forEach(select => {
            select.innerHTML = '<option value="">Select field...</option>' +
                settings.filterable.map(f => `<option value="${f}">${f}</option>`).join('');
        });

        // Update sort field selects
        document.querySelectorAll('#sortBuilder .sort-field').forEach(select => {
            select.innerHTML = '<option value="">Select field...</option>' +
                settings.sortable.map(f => `<option value="${f}">${f}</option>`).join('');
        });

        // Update facet field selects
        document.querySelectorAll('#facetBuilder .facet-field').forEach(select => {
            select.innerHTML = '<option value="">Select field...</option>' +
                settings.filterable.map(f => `<option value="${f}">${f}</option>`).join('');
        });
    }

    function renderQueryResults(results, duration) {
        const container = document.getElementById('queryResults');
        const stats = document.getElementById('queryStats');

        if (!results || !results.hits || results.hits.length === 0) {
            container.innerHTML = '<p class="placeholder">No results found</p>';
            stats.style.display = 'none';
            return;
        }

        const columns = Object.keys(results.hits[0]);

        container.innerHTML = `
            <div class="results-header">
                <span class="badge info">${results.limit} results</span>
                <span class="text-secondary">of ${results.estimatedTotalHits || results.totalHits} total hits</span>
            </div>
            <table class="data-table">
                <thead>
                    <tr>${columns.map(col => `<th>${escapeHtml(col)}</th>`).join('')}</tr>
                </thead>
                <tbody>
                    ${results.hits.map(hit => `
                        <tr>${columns.map(col => {
                            const value = hit[col];
                            if (value === null || value === undefined) return '<td class="text-secondary">null</td>';
                            if (typeof value === 'object') return `<td><pre>${escapeHtml(JSON.stringify(value, null, 2))}</pre></td>`;
                            return `<td>${escapeHtml(String(value))}</td>`;
                        }).join('')}</tr>
                    `).join('')}
                </tbody>
            </table>
        `;

        stats.style.display = 'block';
        document.getElementById('queryHits').textContent = results.estimatedTotalHits || results.totalHits;
        document.getElementById('queryProcessingTime').textContent = duration ? `${duration}ms` : '-';
        document.getElementById('queryShardsQueried').textContent = results.shardsQueried || '-';

        // Render latency breakdown if available
        if (results.shardBreakdown) {
            renderShardLatencyBreakdown(results.shardBreakdown);
        }
    }

    function renderShardLatencyBreakdown(breakdown) {
        const container = document.getElementById('latencyBreakdown');
        const table = document.getElementById('shardLatencyTable');

        container.style.display = 'block';

        table.innerHTML = `
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Shard ID</th>
                        <th>Node</th>
                        <th>Latency</th>
                        <th>Hits</th>
                    </tr>
                </thead>
                <tbody>
                    ${breakdown.map(b => `
                        <tr>
                            <td>${b.shardId}</td>
                            <td>${escapeHtml(b.node)}</td>
                            <td>${b.latencyMs}ms</td>
                            <td>${b.hits}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;
    }

    function renderExplainResults(explain) {
        const container = document.getElementById('explainResults');
        const pre = document.getElementById('explainJson');

        container.style.display = 'block';
        pre.textContent = JSON.stringify(explain, null, 2);
    }

    function renderShadowDiffResults(diff) {
        const container = document.getElementById('shadowDiffResults');
        const content = document.getElementById('shadowDiffContent');

        container.style.display = 'block';

        if (!diff || !diff.diffs || diff.diffs.length === 0) {
            content.innerHTML = '<p class="placeholder">No differences found between live and shadow results</p>';
            return;
        }

        content.innerHTML = `
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-label">Differences Found</div>
                    <div class="stat-value">${diff.diffs.length}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Hit Differences</div>
                    <div class="stat-value">${diff.hitDiffs || 0}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Ranking Differences</div>
                    <div class="stat-value">${diff.rankingDiffs || 0}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Errors</div>
                    <div class="stat-value">${diff.errors || 0}</div>
                </div>
            </div>
            <table class="data-table" style="margin-top: 1rem;">
                <thead>
                    <tr>
                        <th>Document ID</th>
                        <th>Type</th>
                        <th>Live</th>
                        <th>Shadow</th>
                    </tr>
                </thead>
                <tbody>
                    ${diff.diffs.map(d => `
                        <tr>
                            <td>${escapeHtml(d.docId)}</td>
                            <td><span class="badge ${d.type === 'error' ? 'error' : 'warning'}">${d.type}</span></td>
                            <td>${escapeHtml(JSON.stringify(d.live))}</td>
                            <td>${escapeHtml(JSON.stringify(d.shadow))}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        `;
    }

    // ===========================================================================
    // Rendering - Tasks Section
    // ===========================================================================

    function renderTasks() {
        // Populate index filter
        const indexSelect = document.getElementById('taskIndex');
        indexSelect.innerHTML = '<option value="">All Indexes</option>';
        state.tasks.indexes.forEach(idx => {
            const option = document.createElement('option');
            option.value = idx.uid;
            option.textContent = idx.uid;
            if (state.tasks.indexUid === idx.uid) {
                option.selected = true;
            }
            indexSelect.appendChild(option);
        });

        renderTasksTable();
        renderTasksPagination();
    }

    function renderTasksTable() {
        const tbody = document.getElementById('tasksTableBody');

        if (!state.tasks.tasks || state.tasks.tasks.length === 0) {
            tbody.innerHTML = '<tr><td colspan="9" class="loading">No tasks found</td></tr>';
            return;
        }

        tbody.innerHTML = state.tasks.tasks.map(task => {
            const statusClass = {
                'enqueued': 'info',
                'processing': 'warning',
                'succeeded': 'success',
                'failed': 'error',
                'canceled': 'secondary'
            }[task.status] || 'secondary';

            const progress = task.details && task.details.totalDocuments
                ? `${Math.round((task.details.loadedDocuments || 0) / task.details.totalDocuments * 100)}%`
                : '-';

            const duration = task.startedAt && task.finishedAt
                ? formatDuration(new Date(task.finishedAt) - new Date(task.startedAt))
                : task.startedAt ? formatDuration(Date.now() - new Date(task.startedAt)) : '-';

            const actions = [];
            if (task.status === 'enqueued' || task.status === 'processing') {
                actions.push(`<button class="btn btn-secondary btn-sm" onclick="cancelTaskById(${task.uid})">Cancel</button>`);
            }
            actions.push(`<button class="btn btn-secondary btn-sm" onclick="showTaskDetails(${task.uid})">Details</button>`);

            return `
                <tr>
                    <td>${task.uid}</td>
                    <td>${task.type}</td>
                    <td>${task.indexUid || '-'}</td>
                    <td><span class="badge ${statusClass}">${task.status}</span></td>
                    <td>${progress}</td>
                    <td>${task.enqueuedAt ? formatDate(task.enqueuedAt) : '-'}</td>
                    <td>${task.finishedAt ? formatDate(task.finishedAt) : '-'}</td>
                    <td>${duration}</td>
                    <td>${actions.join(' ')}</td>
                </tr>
            `;
        }).join('');
    }

    function renderTasksPagination() {
        const prevBtn = document.getElementById('taskPrevPage');
        const nextBtn = document.getElementById('taskNextPage');
        const pageInfo = document.getElementById('taskPageInfo');

        const from = state.tasks.offset + 1;
        const to = Math.min(state.tasks.offset + state.tasks.limit, state.tasks.total);

        pageInfo.textContent = `Showing ${from}-${to} of ${state.tasks.total} tasks`;

        prevBtn.disabled = state.tasks.offset === 0;
        nextBtn.disabled = to >= state.tasks.total;
    }

    async function showTaskDetails(taskUid) {
        const details = await fetchTaskDetails(taskUid);
        if (!details) {
            alert('Failed to load task details');
            return;
        }

        const content = document.getElementById('taskDetailsContent');
        content.innerHTML = `
            <div class="card" style="margin: 0;">
                <h4>Task ${taskUid}</h4>
                <table class="data-table">
                    <tr><th>UID</th><td>${details.uid}</td></tr>
                    <tr><th>Type</th><td>${details.type}</td></tr>
                    <tr><th>Index</th><td>${details.indexUid || '-'}</td></tr>
                    <tr><th>Status</th><td><span class="badge ${details.status}">${details.status}</span></td></tr>
                    <tr><th>Enqueued</th><td>${details.enqueuedAt ? formatDate(details.enqueuedAt) : '-'}</td></tr>
                    <tr><th>Started</th><td>${details.startedAt ? formatDate(details.startedAt) : '-'}</td></tr>
                    <tr><th>Finished</th><td>${details.finishedAt ? formatDate(details.finishedAt) : '-'}</td></tr>
                    <tr><th>Duration</th><td>${details.startedAt && details.finishedAt ? formatDuration(new Date(details.finishedAt) - new Date(details.startedAt)) : '-'}</td></tr>
                    <tr><th>Error</th><td>${details.error ? `<pre class="error">${escapeHtml(details.error)}</pre>` : '-'}</td></tr>
                </table>
                ${details.details ? `
                    <h4>Details</h4>
                    <pre class="settings-json">${escapeHtml(JSON.stringify(details.details, null, 2))}</pre>
                ` : ''}
            </div>
        `;

        showModal('taskDetailsModal');
    }

    // Global function for onclick handler
    window.cancelTaskById = async function(taskUid) {
        if (!confirm(`Cancel task ${taskUid}?`)) return;

        try {
            await cancelTask(taskUid);
            await fetchTasks();
            renderTasks();
        } catch (error) {
            alert(`Failed to cancel task: ${error.message}`);
        }
    };

    // Global functions for filter/sort/facet builders
    window.addFilterRow = function(containerId) {
        const container = document.getElementById(containerId);
        const row = document.createElement('div');
        row.className = 'filter-row';
        row.innerHTML = `
            <select class="form-input filter-field">
                <option value="">Select field...</option>
            </select>
            <select class="form-input filter-operator">
                <option value="equals">Equals</option>
                <option value="notEquals">Not Equals</option>
                <option value="gt">Greater Than</option>
                <option value="gte">Greater Than or Equal</option>
                <option value="lt">Less Than</option>
                <option value="lte">Less Than or Equal</option>
                <option value="in">In</option>
                <option value="exists">Exists</option>
            </select>
            <input type="text" class="form-input filter-value" placeholder="Value">
            <button class="btn btn-secondary btn-sm" onclick="addFilterRow('${containerId}')">+</button>
            <button class="btn btn-secondary btn-sm" onclick="removeFilterRow(this)">−</button>
        `;
        container.appendChild(row);
    };

    window.removeFilterRow = function(button) {
        const container = button.parentElement.parentElement;
        if (container.children.length > 1) {
            button.parentElement.remove();
        }
    };

    async function refreshData() {
        // Always fetch topology (used by overview and topology sections)
        await fetchTopology();

        // Fetch additional data based on current section
        if (state.currentSection === 'topology') {
            await Promise.all([
                fetchShards(),
                fetchRebalanceStatus()
            ]);
            renderTopology();
        } else if (state.currentSection === 'overview') {
            await fetchRebalanceStatus();
            renderOverview();
        } else if (state.currentSection === 'indexes') {
            await fetchIndexes();
            renderIndexes();
        } else if (state.currentSection === 'aliases') {
            await fetchAliases();
            renderAliases();
        } else if (state.currentSection === 'documents') {
            await fetchDocumentsIndexes();
            renderDocuments();
        } else if (state.currentSection === 'query') {
            await fetchQueryIndexes();
            renderQuerySandbox();
        } else if (state.currentSection === 'tasks') {
            await fetchTasksIndexes();
            await fetchTasks();
            renderTasks();
        }
    }

    // ============================================================================
    // Rendering - Overview Section
    // ============================================================================

    function renderOverview() {
        if (!state.topology) return;

        const t = state.topology;

        // Cluster status
        const clusterStatusEl = document.getElementById('clusterStatus');
        const clusterStatusSubEl = document.getElementById('clusterStatusSub');
        if (t.fully_covered) {
            clusterStatusEl.textContent = 'Healthy';
            clusterStatusEl.className = 'stat-value';
            clusterStatusEl.style.color = 'var(--success-color)';
            clusterStatusSubEl.textContent = 'All nodes operational';
        } else {
            clusterStatusEl.textContent = 'Degraded';
            clusterStatusEl.style.color = 'var(--warning-color)';
            clusterStatusSubEl.textContent = `${t.degraded_node_count} node(s) unhealthy`;
        }

        // Total shards
        document.getElementById('totalShards').textContent = t.shards;
        document.getElementById('replicationFactor').textContent = t.replication_factor;

        // Nodes
        document.getElementById('totalNodes').textContent = t.nodes.length;
        document.getElementById('degradedNodes').textContent = t.degraded_node_count;

        // Replica groups (calculate from unique replica_group values in nodes)
        const groups = new Set(t.nodes.map(n => {
            // Extract replica group from topology - need to get from nodes
            // For now, estimate based on replication factor
            return Math.floor(n.shard_count / (t.shards / t.replication_factor));
        }));
        document.getElementById('totalGroups').textContent = Math.ceil(t.nodes.length / t.replication_factor);

        // Active operations (rebalance, reshard)
        const activeOpsEl = document.getElementById('activeOperations');
        if (state.rebalanceStatus && state.rebalanceStatus.in_progress) {
            const pct = state.rebalanceStatus.overall_pct_complete || 0;
            activeOpsEl.innerHTML = `
                <div class="operation-item">
                    <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                        <strong>Rebalance in Progress</strong>
                        <span class="badge info">${pct}%</span>
                    </div>
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: ${pct}%"></div>
                    </div>
                    ${state.rebalanceStatus.metrics ? `
                        <div style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 0.5rem;">
                            Documents migrated: ${state.rebalanceStatus.metrics.documents_migrated_total || 0}
                        </div>
                    ` : ''}
                </div>
            `;
        } else {
            activeOpsEl.innerHTML = '<p class="placeholder">No active operations</p>';
        }

        // Recent activity (placeholder for now)
        const recentActivityEl = document.getElementById('recentActivity');
        recentActivityEl.innerHTML = '<p class="placeholder">No recent activity</p>';
    }

    // ============================================================================
    // Rendering - Topology Section
    // ============================================================================

    function renderTopology() {
        if (!state.topology) return;

        renderNodeTable();
        renderShardCoverageMap();
        renderRebalanceProgress();
    }

    function renderNodeTable() {
        const tbody = document.getElementById('nodeTableBody');
        if (!state.topology || !state.topology.nodes) {
            tbody.innerHTML = '<tr><td colspan="6" class="loading">Loading...</td></tr>';
            return;
        }

        const nodes = state.topology.nodes;
        if (nodes.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" class="loading">No nodes found</td></tr>';
            return;
        }

        tbody.innerHTML = nodes.map(node => {
            const statusClass = node.status === 'active' ? 'success' :
                               node.status === 'draining' ? 'warning' : 'error';
            const statusLabel = node.status.charAt(0).toUpperCase() + node.status.slice(1);

            // Calculate replica group from node ID or address (heuristic)
            const replicaGroup = extractReplicaGroup(node);

            // Last seen formatting
            const lastSeen = node.last_seen_ms > 0
                ? formatLastSeen(node.last_seen_ms)
                : 'Unknown';

            return `
                <tr>
                    <td data-label="Node ID">${escapeHtml(node.id)}</td>
                    <td data-label="Address">${escapeHtml(node.address)}</td>
                    <td data-label="Status"><span class="badge ${statusClass}">${statusLabel}</span></td>
                    <td data-label="Replica Group">${replicaGroup}</td>
                    <td data-label="Shards">${node.shard_count || 0}</td>
                    <td data-label="Last Seen">${lastSeen}</td>
                </tr>
            `;
        }).join('');
    }

    function renderShardCoverageMap() {
        const container = document.getElementById('shardCoverageMap');
        if (!state.topology || !state.shards) {
            container.innerHTML = '<p class="placeholder">Loading...</p>';
            return;
        }

        const shardCount = state.topology.shards;
        const shards = state.shards.shards;

        // Determine health status for each shard
        const cells = [];
        for (let i = 0; i < shardCount; i++) {
            const shardId = i.toString();
            const nodeIds = shards[shardId] || [];

            let status = 'healthy';
            if (nodeIds.length === 0) {
                status = 'missing';
            } else if (nodeIds.length < state.topology.replication_factor) {
                status = 'degraded';
            }

            cells.push(`
                <div class="shard-cell ${status}" title="Shard ${i}">
                    ${i}
                    <div class="shard-tooltip">
                        <strong>Shard ${i}</strong><br>
                        Replicas: ${nodeIds.length}<br>
                        Nodes: ${nodeIds.map(id => escapeHtml(id)).join(', ') || 'None'}
                    </div>
                </div>
            `);
        }

        container.innerHTML = `
            <div style="margin-bottom: 1rem;">
                <span class="badge success">Healthy</span>
                <span class="badge warning" style="margin-left: 0.5rem;">Degraded</span>
                <span class="badge error" style="margin-left: 0.5rem;">Missing</span>
            </div>
            <div class="shard-coverage-map">
                ${cells.join('')}
            </div>
        `;
    }

    function renderRebalanceProgress() {
        const container = document.getElementById('rebalanceProgress');
        if (!state.rebalanceStatus || !state.rebalanceStatus.in_progress) {
            container.innerHTML = '<p class="placeholder">No rebalance in progress</p>';
            return;
        }

        const rs = state.rebalanceStatus;
        const phases = rs.phases || [];

        let phasesHtml = '';
        if (phases.length > 0) {
            phasesHtml = `
                <div style="margin-top: 1rem;">
                    <h4 style="font-size: 0.875rem; margin-bottom: 0.5rem;">Migration Progress</h4>
                    ${phases.map(p => `
                        <div style="margin-bottom: 0.75rem;">
                            <div style="display: flex; justify-content: space-between; font-size: 0.875rem;">
                                <span>Shard ${p.shard}: ${escapeHtml(p.source || 'Unknown')} → ${escapeHtml(p.destination || 'Unknown')}</span>
                                <span>${p.pct_complete || 0}%</span>
                            </div>
                            <div class="progress-bar">
                                <div class="progress-fill" style="width: ${p.pct_complete || 0}%"></div>
                            </div>
                        </div>
                    `).join('')}
                </div>
            `;
        }

        container.innerHTML = `
            <div style="margin-bottom: 1rem;">
                <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                    <strong>Overall Progress</strong>
                    <span class="badge info">${rs.overall_pct_complete || 0}%</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill" style="width: ${rs.overall_pct_complete || 0}%"></div>
                </div>
                ${rs.started_at ? `<p style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 0.5rem;">Started: ${escapeHtml(rs.started_at)}</p>` : ''}
            </div>
            ${phasesHtml}
        `;
    }

    // ============================================================================
    // Rendering - Indexes Section
    // ============================================================================

    async function renderIndexes() {
        const tbody = document.getElementById('indexesTableBody');
        if (!state.indexes) {
            tbody.innerHTML = '<tr><td colspan="6" class="loading">Loading...</td></tr>';
            return;
        }

        if (state.indexes.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" class="loading">No indexes found. Create your first index to get started.</td></tr>';
            return;
        }

        // Fetch stats for all indexes in parallel
        const statsPromises = state.indexes.map(idx => fetchIndexStats(idx.uid));
        const statsResults = await Promise.allSettled(statsPromises);

        // Fetch settings version for all indexes
        const settingsPromises = state.indexes.map(idx => fetchIndexSettings(idx.uid));
        const settingsResults = await Promise.allSettled(settingsPromises);

        tbody.innerHTML = state.indexes.map((idx, i) => {
            const stats = statsResults[i].status === 'fulfilled' ? statsResults[i].value : null;
            const settings = settingsResults[i].status === 'fulfilled' ? settingsResults[i].value : null;

            const docCount = stats?.numberOfDocuments || 0;
            const primaryKey = idx.primaryKey || '-';
            const settingsVersion = settings?._miroirSettingsVersion || '-';
            const fingerprint = settings?._miroirFingerprint || '-';

            return `
                <tr>
                    <td data-label="UID">${escapeHtml(idx.uid)}</td>
                    <td data-label="Primary Key">${escapeHtml(primaryKey)}</td>
                    <td data-label="Documents">${docCount.toLocaleString()}</td>
                    <td data-label="Settings Version">${settingsVersion}</td>
                    <td data-label="Fingerprint"><code class="code">${escapeHtml(fingerprint.substring(0, 12))}${fingerprint.length > 12 ? '...' : ''}</code></td>
                    <td data-label="Actions">
                        <div class="action-buttons">
                            <button class="btn btn-sm btn-secondary" onclick="openSettingsModal('${escapeHtml(idx.uid)}')">Settings</button>
                            <button class="btn btn-sm btn-danger" onclick="confirmDeleteIndex('${escapeHtml(idx.uid)}')">Delete</button>
                        </div>
                    </td>
                </tr>
            `;
        }).join('');
    }

    // ============================================================================
    // Rendering - Aliases Section
    // ============================================================================

    async function renderAliases() {
        const tbody = document.getElementById('aliasesTableBody');
        if (!state.aliases) {
            tbody.innerHTML = '<tr><td colspan="6" class="loading">Loading...</td></tr>';
            return;
        }

        if (state.aliases.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" class="loading">No aliases found. Create an alias to get started.</td></tr>';
            return;
        }

        tbody.innerHTML = state.aliases.map(alias => {
            const kindLabel = alias.kind === 'single' ? 'Single Target' : 'Multi Target';
            const target = alias.kind === 'single'
                ? (alias.currentUid || '-')
                : (alias.targetUids?.join(', ') || '-');

            return `
                <tr>
                    <td data-label="Name">${escapeHtml(alias.name)}</td>
                    <td data-label="Type"><span class="badge info">${escapeHtml(kindLabel)}</span></td>
                    <td data-label="Current Target">${escapeHtml(String(target))}</td>
                    <td data-label="Version">${alias.version || 0}</td>
                    <td data-label="Created">${alias.createdAt ? new Date(alias.createdAt * 1000).toLocaleString() : '-'}</td>
                    <td data-label="Actions">
                        <div class="action-buttons">
                            ${alias.kind === 'single' ? `<button class="btn btn-sm btn-primary" onclick="openFlipAliasModal('${escapeHtml(alias.name)}', '${escapeHtml(String(target))}')">Flip</button>` : ''}
                            <button class="btn btn-sm btn-secondary" onclick="showAliasHistory('${escapeHtml(alias.name)}')">History</button>
                            <button class="btn btn-sm btn-danger" onclick="confirmDeleteAlias('${escapeHtml(alias.name)}')">Delete</button>
                        </div>
                    </td>
                </tr>
            `;
        }).join('');
    }

    async function showAliasHistory(aliasName) {
        state.currentAliasForHistory = aliasName;
        const details = await fetchAliasDetails(aliasName);
        if (!details) return;

        document.getElementById('aliasHistoryName').textContent = aliasName;
        document.getElementById('aliasHistoryCard').style.display = 'block';

        const timeline = document.getElementById('aliasHistoryTimeline');
        const history = details.history || [];

        if (history.length === 0) {
            timeline.innerHTML = '<p class="placeholder">No history available</p>';
            return;
        }

        timeline.innerHTML = history.map((entry, i) => `
            <div class="timeline-entry ${i === 0 ? 'current' : ''}">
                <div class="timeline-time">${new Date(entry.flippedAt * 1000).toLocaleString()}</div>
                <div class="timeline-content">
                    <strong>${escapeHtml(entry.uid)}</strong>
                    <span>${i === 0 ? 'Current target' : 'Previous target'}</span>
                </div>
            </div>
        `).join('');
    }

    // ============================================================================
    // Utilities
    // ============================================================================

    function extractReplicaGroup(node) {
        // Try to extract replica group from node ID or address
        // This is a heuristic - in production, the API should return this directly
        const match = node.id.match(/(\d+)$/) || node.address.match(/(\d+)/);
        return match ? parseInt(match[1]) : '-';
    }

    function formatLastSeen(ms) {
        if (ms < 1000) return `${ms}ms ago`;
        if (ms < 60000) return `${Math.floor(ms / 1000)}s ago`;
        if (ms < 3600000) return `${Math.floor(ms / 60000)}m ago`;
        return `${Math.floor(ms / 3600000)}h ago`;
    }

    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    function updateConnectionStatus(connected) {
        state.isConnected = connected;
        const statusEl = document.getElementById('connectionStatus');
        const dot = statusEl.querySelector('.status-dot');

        if (connected) {
            dot.classList.remove('disconnected');
            statusEl.childNodes[1].textContent = ' Connected';
        } else {
            dot.classList.add('disconnected');
            statusEl.childNodes[1].textContent = ' Disconnected';
        }
    }

    // ============================================================================
    // Mobile Menu
    // ============================================================================

    function initMobileMenu() {
        const toggle = document.getElementById('mobileMenuToggle');
        const sidebar = document.querySelector('.sidebar');

        toggle.addEventListener('click', () => {
            sidebar.classList.toggle('open');
        });

        // Close when clicking outside
        document.addEventListener('click', (e) => {
            if (window.innerWidth <= 768) {
                if (!sidebar.contains(e.target) && !toggle.contains(e.target)) {
                    sidebar.classList.remove('open');
                }
            }
        });
    }

    // ============================================================================
    // Refresh Button
    // ============================================================================

    function initRefreshButton() {
        const btn = document.getElementById('refreshBtn');
        btn.addEventListener('click', () => {
            refreshData();
        });
    }

    // ============================================================================
    // Auto-refresh (30 seconds)
    // ============================================================================

    function initAutoRefresh() {
        // Refresh every 30 seconds
        state.refreshInterval = setInterval(() => {
            if (document.visibilityState === 'visible') {
                refreshData();
            }
        }, 30000);
    }

    // ============================================================================
    // Initialization
    // ============================================================================

    // ============================================================================
    // Modal Handlers - Indexes
    // ============================================================================

    function openSettingsModal(indexUid) {
        state.currentSettingsIndex = indexUid;

        // Fetch current settings
        fetchIndexSettings(indexUid).then(settings => {
            const currentJson = JSON.stringify(settings || {}, null, 2);
            document.getElementById('currentSettingsJson').textContent = currentJson;
            document.getElementById('settingsEditor').value = currentJson;
            document.getElementById('settingsModalTitle').textContent = `Settings: ${indexUid}`;
            document.getElementById('settingsDiff').style.display = 'none';
            document.getElementById('settingsApplyBtn').style.display = 'none';
            document.getElementById('settingsPreviewBtn').style.display = 'inline-flex';
            showModal('settingsModal');
        }).catch(err => {
            console.error('Failed to fetch settings:', err);
            alert('Failed to load settings. Please try again.');
        });
    }

    function previewSettingsChanges() {
        const editor = document.getElementById('settingsEditor');
        const newSettings = JSON.parse(editor.value);
        const currentJson = document.getElementById('currentSettingsJson').textContent;
        const currentSettings = JSON.parse(currentJson || '{}');

        // Compute fingerprint of new settings
        const newFingerprint = computeFingerprint(newSettings);
        document.getElementById('newFingerprint').textContent = newFingerprint;

        // Compute diff
        const diffSummary = document.getElementById('diffSummary');
        const diff = computeDiff(currentSettings, newSettings);

        if (diff.length === 0) {
            diffSummary.innerHTML = '<p class="info">No changes detected.</p>';
        } else {
            diffSummary.innerHTML = diff.map(line =>
                `<div class="diff-line ${line.type}">${escapeHtml(line.text)}</div>`
            ).join('');
        }

        document.getElementById('settingsDiff').style.display = 'block';
        document.getElementById('settingsPreviewBtn').style.display = 'none';
        document.getElementById('settingsApplyBtn').style.display = 'inline-flex';
    }

    async function applySettingsChanges() {
        const indexUid = state.currentSettingsIndex;
        const editor = document.getElementById('settingsEditor');
        const newSettings = JSON.parse(editor.value);

        try {
            await fetchAPI(`/indexes/${indexUid}/settings`, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(newSettings)
            });

            hideModal('settingsModal');
            refreshData();
        } catch (error) {
            console.error('Failed to apply settings:', error);
            alert('Failed to apply settings. Please try again.');
        }
    }

    function confirmDeleteIndex(indexUid) {
        document.getElementById('deleteIndexName').textContent = indexUid;
        document.getElementById('deleteIndexConfirm').value = '';
        document.getElementById('deleteIndexConfirmBtn').disabled = true;
        showModal('deleteIndexModal');
    }

    async function deleteIndex() {
        const indexUid = document.getElementById('deleteIndexName').textContent;
        const confirmInput = document.getElementById('deleteIndexConfirm').value;

        if (confirmInput !== indexUid) {
            alert('Index name does not match.');
            return;
        }

        try {
            await fetchAPI(`/indexes/${indexUid}`, { method: 'DELETE' });
            hideModal('deleteIndexModal');
            refreshData();
        } catch (error) {
            console.error('Failed to delete index:', error);
            alert('Failed to delete index. Please try again.');
        }
    }

    // ============================================================================
    // Modal Handlers - Aliases
    // ============================================================================

    function openFlipAliasModal(aliasName, currentTarget) {
        document.getElementById('flipAliasName').textContent = aliasName;
        document.getElementById('flipAliasCurrent').value = currentTarget;
        document.getElementById('flipAliasNew').value = '';
        showModal('flipAliasModal');
    }

    async function flipAlias() {
        const aliasName = document.getElementById('flipAliasName').textContent;
        const newTarget = document.getElementById('flipAliasNew').value.trim();

        if (!newTarget) {
            alert('Please enter a new target.');
            return;
        }

        try {
            await fetchAPI(`/_miroir/aliases/${aliasName}`, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ target: newTarget })
            });

            hideModal('flipAliasModal');
            refreshData();
        } catch (error) {
            console.error('Failed to flip alias:', error);
            alert('Failed to flip alias. Please try again.');
        }
    }

    function confirmDeleteAlias(aliasName) {
        document.getElementById('deleteAliasName').textContent = aliasName;
        document.getElementById('deleteAliasConfirm').value = '';
        document.getElementById('deleteAliasConfirmBtn').disabled = true;
        showModal('deleteAliasModal');
    }

    async function deleteAlias() {
        const aliasName = document.getElementById('deleteAliasName').textContent;
        const confirmInput = document.getElementById('deleteAliasConfirm').value;

        if (confirmInput !== aliasName) {
            alert('Alias name does not match.');
            return;
        }

        try {
            await fetchAPI(`/_miroir/aliases/${aliasName}`, { method: 'DELETE' });
            hideModal('deleteAliasModal');
            refreshData();
        } catch (error) {
            console.error('Failed to delete alias:', error);
            alert('Failed to delete alias. Please try again.');
        }
    }

    async function createAlias() {
        const name = document.getElementById('aliasName').value.trim();
        const type = document.getElementById('aliasType').value;
        const target = document.getElementById('aliasTarget').value.trim();
        const targets = document.getElementById('aliasTargets').value.trim().split(',').map(s => s.trim()).filter(s => s);

        if (!name) {
            alert('Please enter an alias name.');
            return;
        }

        const body = type === 'single'
            ? { target }
            : { targets };

        try {
            await fetchAPI(`/_miroir/aliases/${name}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            });

            hideModal('createAliasModal');
            refreshData();
        } catch (error) {
            console.error('Failed to create alias:', error);
            alert('Failed to create alias. Please try again.');
        }
    }

    // ============================================================================
    // Modal Helpers
    // ============================================================================

    function showModal(modalId) {
        document.getElementById(modalId).classList.add('active');
    }

    function hideModal(modalId) {
        document.getElementById(modalId).classList.remove('active');
    }

    function computeFingerprint(settings) {
        const canonical = JSON.stringify(settings, Object.keys(settings).sort());
        let hash = 0;
        for (let i = 0; i < canonical.length; i++) {
            const char = canonical.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash = hash & hash;
        }
        return Math.abs(hash).toString(16).padStart(8, '0');
    }

    function computeDiff(current, newSettings) {
        const diff = [];

        // Find removed keys
        for (const key in current) {
            if (!(key in newSettings)) {
                diff.push({ type: 'removed', text: `- ${key}: ${JSON.stringify(current[key])}` });
            }
        }

        // Find added and changed keys
        for (const key in newSettings) {
            const currentVal = current[key];
            const newVal = newSettings[key];

            if (!(key in current)) {
                diff.push({ type: 'added', text: `+ ${key}: ${JSON.stringify(newVal)}` });
            } else if (JSON.stringify(currentVal) !== JSON.stringify(newVal)) {
                diff.push({ type: 'removed', text: `- ${key}: ${JSON.stringify(currentVal)}` });
                diff.push({ type: 'added', text: `+ ${key}: ${JSON.stringify(newVal)}` });
            }
        }

        return diff;
    }

    // ============================================================================
    // Initialization
    // ============================================================================

    function initModals() {
        // Settings modal
        document.getElementById('settingsModalClose').addEventListener('click', () => hideModal('settingsModal'));
        document.getElementById('settingsCancelBtn').addEventListener('click', () => hideModal('settingsModal'));
        document.getElementById('settingsPreviewBtn').addEventListener('click', previewSettingsChanges);
        document.getElementById('settingsApplyBtn').addEventListener('click', applySettingsChanges);

        // Create index modal
        document.getElementById('createIndexBtn').addEventListener('click', () => showModal('createIndexModal'));
        document.getElementById('createIndexModalClose').addEventListener('click', () => hideModal('createIndexModal'));
        document.getElementById('createIndexCancelBtn').addEventListener('click', () => hideModal('createIndexModal'));
        document.getElementById('createIndexConfirmBtn').addEventListener('click', async () => {
            const uid = document.getElementById('indexUid').value.trim();
            const primaryKey = document.getElementById('indexPrimaryKey').value.trim() || null;

            if (!uid) {
                alert('Please enter an index UID.');
                return;
            }

            try {
                const body = primaryKey ? { uid, primaryKey } : { uid };
                await fetchAPI('/indexes', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body)
                });
                hideModal('createIndexModal');
                refreshData();
            } catch (error) {
                console.error('Failed to create index:', error);
                alert('Failed to create index. Please try again.');
            }
        });

        // Delete index modal
        document.getElementById('deleteIndexModalClose').addEventListener('click', () => hideModal('deleteIndexModal'));
        document.getElementById('deleteIndexCancelBtn').addEventListener('click', () => hideModal('deleteIndexModal'));
        document.getElementById('deleteIndexConfirmBtn').addEventListener('click', deleteIndex);
        document.getElementById('deleteIndexConfirm').addEventListener('input', (e) => {
            document.getElementById('deleteIndexConfirmBtn').disabled = e.target.value !== document.getElementById('deleteIndexName').textContent;
        });

        // Create alias modal
        document.getElementById('createAliasBtn').addEventListener('click', () => showModal('createAliasModal'));
        document.getElementById('createAliasModalClose').addEventListener('click', () => hideModal('createAliasModal'));
        document.getElementById('createAliasCancelBtn').addEventListener('click', () => hideModal('createAliasModal'));
        document.getElementById('createAliasConfirmBtn').addEventListener('click', createAlias);
        document.getElementById('aliasType').addEventListener('change', (e) => {
            const isSingle = e.target.value === 'single';
            document.getElementById('singleTargetGroup').style.display = isSingle ? 'block' : 'none';
            document.getElementById('multiTargetGroup').style.display = isSingle ? 'none' : 'block';
        });

        // Flip alias modal
        document.getElementById('flipAliasModalClose').addEventListener('click', () => hideModal('flipAliasModal'));
        document.getElementById('flipAliasCancelBtn').addEventListener('click', () => hideModal('flipAliasModal'));
        document.getElementById('flipAliasConfirmBtn').addEventListener('click', flipAlias);

        // Delete alias modal
        document.getElementById('deleteAliasModalClose').addEventListener('click', () => hideModal('deleteAliasModal'));
        document.getElementById('deleteAliasCancelBtn').addEventListener('click', () => hideModal('deleteAliasModal'));
        document.getElementById('deleteAliasConfirmBtn').addEventListener('click', deleteAlias);
        document.getElementById('deleteAliasConfirm').addEventListener('input', (e) => {
            document.getElementById('deleteAliasConfirmBtn').disabled = e.target.value !== document.getElementById('deleteAliasName').textContent;
        });

        // Close modals on backdrop click
        document.querySelectorAll('.modal').forEach(modal => {
            modal.addEventListener('click', (e) => {
                if (e.target === modal) {
                    modal.classList.remove('active');
                }
            });
        });

        // Initialize Documents section
        initDocumentsSection();

        // Initialize Query Sandbox section
        initQuerySandboxSection();

        // Initialize Tasks section
        initTasksSection();
    }

    function initDocumentsSection() {
        // Index selection
        document.getElementById('documentIndexSelect').addEventListener('change', async (e) => {
            state.documents.currentIndex = e.target.value;
            if (state.documents.currentIndex) {
                await loadDocuments();
            }
        });

        // Pagination
        document.getElementById('documentLimit').addEventListener('change', (e) => {
            state.documents.limit = parseInt(e.target.value);
            if (state.documents.currentIndex) loadDocuments();
        });

        document.getElementById('documentOffset').addEventListener('change', (e) => {
            state.documents.offset = parseInt(e.target.value);
            if (state.documents.currentIndex) loadDocuments();
        });

        document.getElementById('documentPrevPage').addEventListener('click', () => {
            const newOffset = Math.max(0, state.documents.offset - state.documents.limit);
            state.documents.offset = newOffset;
            document.getElementById('documentOffset').value = newOffset;
            if (state.documents.currentIndex) loadDocuments();
        });

        document.getElementById('documentNextPage').addEventListener('click', () => {
            const newOffset = state.documents.offset + state.documents.limit;
            state.documents.offset = newOffset;
            document.getElementById('documentOffset').value = newOffset;
            if (state.documents.currentIndex) loadDocuments();
        });

        // Filter builder
        document.getElementById('applyDocumentFilterBtn').addEventListener('click', async () => {
            const filters = [];
            document.querySelectorAll('#documentFilterBuilder .filter-row').forEach(row => {
                const field = row.querySelector('.filter-field').value;
                const operator = row.querySelector('.filter-operator').value;
                const value = row.querySelector('.filter-value').value;
                if (field && value) {
                    filters.push({ field, operator, value });
                }
            });
            state.documents.filters = filters;
            state.documents.offset = 0;
            document.getElementById('documentOffset').value = 0;
            if (state.documents.currentIndex) loadDocuments();
        });

        document.getElementById('clearDocumentFilterBtn').addEventListener('click', async () => {
            state.documents.filters = [];
            state.documents.offset = 0;
            document.getElementById('documentOffset').value = 0;
            // Clear filter inputs
            document.querySelectorAll('#documentFilterBuilder .filter-row').forEach(row => {
                row.querySelector('.filter-field').value = '';
                row.querySelector('.filter-value').value = '';
            });
            if (state.documents.currentIndex) loadDocuments();
        });

        // Import documents
        document.getElementById('importDocumentsBtn').addEventListener('click', () => showModal('importDocumentsModal'));
        document.getElementById('importDocumentsModalClose').addEventListener('click', () => hideModal('importDocumentsModal'));
        document.getElementById('importCancelBtn').addEventListener('click', () => hideModal('importDocumentsModal'));

        // File dropzone
        const dropzone = document.getElementById('importDropzone');
        const fileInput = document.getElementById('importFileInput');

        dropzone.addEventListener('click', () => fileInput.click());
        dropzone.addEventListener('dragover', (e) => {
            e.preventDefault();
            dropzone.classList.add('dragover');
        });
        dropzone.addEventListener('dragleave', () => {
            dropzone.classList.remove('dragover');
        });
        dropzone.addEventListener('drop', (e) => {
            e.preventDefault();
            dropzone.classList.remove('dragover');
            handleFiles(e.dataTransfer.files);
        });

        fileInput.addEventListener('change', (e) => {
            handleFiles(e.target.files);
        });

        let selectedFiles = [];

        function handleFiles(files) {
            selectedFiles = Array.from(files);
            const preview = document.getElementById('importPreview');
            const previewContent = document.getElementById('importPreviewContent');
            const fileCount = document.getElementById('importFileCount');
            const confirmBtn = document.getElementById('importConfirmBtn');

            if (selectedFiles.length === 0) {
                preview.style.display = 'none';
                confirmBtn.disabled = true;
                return;
            }

            // Show preview of first file
            const firstFile = selectedFiles[0];
            const reader = new FileReader();
            reader.onload = (e) => {
                previewContent.textContent = e.target.result.substring(0, 1000);
                if (firstFile.size > 1000) {
                    previewContent.textContent += '\n... (truncated)';
                }
            };
            reader.readAsText(firstFile);

            fileCount.textContent = `${selectedFiles.length} file(s) selected: ${selectedFiles.map(f => f.name).join(', ')}`;
            preview.style.display = 'block';
            confirmBtn.disabled = !state.documents.currentIndex;
        }

        document.getElementById('importConfirmBtn').addEventListener('click', async () => {
            if (!state.documents.currentIndex || selectedFiles.length === 0) return;

            const format = document.getElementById('importFormat').value;
            const formData = new FormData();

            selectedFiles.forEach(file => {
                formData.append('files', file);
            });
            formData.append('format', format);

            const progress = document.getElementById('importProgress');
            const progressBar = document.getElementById('importProgressBar');
            const status = document.getElementById('importStatus');

            progress.style.display = 'block';

            try {
                await importDocuments(state.documents.currentIndex, formData, (event) => {
                    if (event.progress !== undefined) {
                        const pct = Math.round(event.progress * 100);
                        progressBar.style.width = `${pct}%`;
                        status.textContent = `Processing: ${pct}%`;
                    }
                    if (event.status) {
                        status.textContent = event.status;
                    }
                });

                status.textContent = 'Import completed successfully!';
                setTimeout(() => {
                    hideModal('importDocumentsModal');
                    progress.style.display = 'none';
                    loadDocuments();
                }, 1500);
            } catch (error) {
                status.textContent = `Import failed: ${error.message}`;
                progressBar.style.width = '0%';
            }
        });

        // Export documents
        document.getElementById('exportDocumentsBtn').addEventListener('click', async () => {
            if (!state.documents.currentIndex) {
                alert('Please select an index first');
                return;
            }

            try {
                const data = await fetchDocuments(state.documents.currentIndex, 1000, 0, []);
                const blob = new Blob([JSON.stringify(data.results, null, 2)], { type: 'application/json' });
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `${state.documents.currentIndex}-export-${Date.now()}.json`;
                a.click();
                URL.revokeObjectURL(url);
            } catch (error) {
                alert(`Export failed: ${error.message}`);
            }
        });
    }

    function initQuerySandboxSection() {
        // Index selection
        document.getElementById('queryIndexSelect').addEventListener('change', async (e) => {
            state.query.currentIndex = e.target.value;
            if (state.query.currentIndex) {
                await loadQueryFields();
            }
        });

        // Run query
        document.getElementById('runQueryBtn').addEventListener('click', async () => {
            if (!state.query.currentIndex) {
                alert('Please select an index first');
                return;
            }

            const query = buildQuery();
            const startTime = Date.now();

            try {
                const results = await runQuery(state.query.currentIndex, query);
                const duration = Date.now() - startTime;
                renderQueryResults(results, duration);
            } catch (error) {
                alert(`Query failed: ${error.message}`);
            }
        });

        // Explain query
        document.getElementById('explainQueryBtn').addEventListener('click', async () => {
            if (!state.query.currentIndex) {
                alert('Please select an index first');
                return;
            }

            const query = buildQuery();

            try {
                const explain = await explainQuery(state.query.currentIndex, query);
                renderExplainResults(explain);
            } catch (error) {
                alert(`Explain failed: ${error.message}`);
            }
        });

        // Shadow diff
        document.getElementById('shadowDiffBtn').addEventListener('click', async () => {
            if (!state.query.currentIndex) {
                alert('Please select an index first');
                return;
            }

            const query = buildQuery();

            try {
                const diff = await runShadowDiff(state.query.currentIndex, query);
                renderShadowDiffResults(diff);
            } catch (error) {
                alert(`Shadow diff failed: ${error.message}`);
            }
        });

        // Add sort button
        document.getElementById('addSortBtn').addEventListener('click', () => {
            const container = document.getElementById('sortBuilder');
            const row = document.createElement('div');
            row.className = 'sort-row';
            row.innerHTML = `
                <select class="form-input sort-field">
                    <option value="">Select field...</option>
                </select>
                <select class="form-input sort-direction">
                    <option value="asc">Ascending</option>
                    <option value="desc">Descending</option>
                </select>
                <button class="btn btn-secondary btn-sm" onclick="removeSortRow(this)">−</button>
            `;
            container.appendChild(row);
            // Load fields if index is selected
            if (state.query.currentIndex) {
                loadQueryFields();
            }
        });

        // Add facet button
        document.getElementById('addFacetBtn').addEventListener('click', () => {
            const container = document.getElementById('facetBuilder');
            const row = document.createElement('div');
            row.className = 'facet-row';
            row.innerHTML = `
                <select class="form-input facet-field">
                    <option value="">Select field...</option>
                </select>
                <button class="btn btn-secondary btn-sm" onclick="removeFacetRow(this)">−</button>
            `;
            container.appendChild(row);
            // Load fields if index is selected
            if (state.query.currentIndex) {
                loadQueryFields();
            }
        });

        // Add filter button
        document.getElementById('addQueryFilterBtn').addEventListener('click', () => {
            addFilterRow('queryFilterBuilder');
            // Load fields if index is selected
            if (state.query.currentIndex) {
                loadQueryFields();
            }
        });
    }

    function buildQuery() {
        const query = {
            q: document.getElementById('queryQ').value || '',
            limit: parseInt(document.getElementById('queryLimit').value) || 20,
            offset: parseInt(document.getElementById('queryOffset').value) || 0
        };

        // Build filter
        const filters = [];
        document.querySelectorAll('#queryFilterBuilder .filter-row').forEach(row => {
            const field = row.querySelector('.filter-field').value;
            const operator = row.querySelector('.filter-operator').value;
            const value = row.querySelector('.filter-value').value;
            if (field && value) {
                filters.push({ field, operator, value });
            }
        });

        if (filters.length > 0) {
            query.filter = buildFilterString(filters);
        }

        // Build sort
        const sorts = [];
        document.querySelectorAll('#sortBuilder .sort-row').forEach(row => {
            const field = row.querySelector('.sort-field').value;
            const direction = row.querySelector('.sort-direction').value;
            if (field) {
                sorts.push(`${field}:${direction}`);
            }
        });

        if (sorts.length > 0) {
            query.sort = sorts;
        }

        // Build facets
        const facets = [];
        document.querySelectorAll('#facetBuilder .facet-row').forEach(row => {
            const field = row.querySelector('.facet-field').value;
            if (field) {
                facets.push(field);
            }
        });

        if (facets.length > 0) {
            query.facets = facets;
        }

        return query;
    }

    // Global functions for removing rows
    window.removeSortRow = function(button) {
        const container = button.parentElement.parentElement;
        if (container.children.length > 1) {
            button.parentElement.remove();
        }
    };

    window.removeFacetRow = function(button) {
        const container = button.parentElement.parentElement;
        if (container.children.length > 1) {
            button.parentElement.remove();
        }
    };

    function initTasksSection() {
        // Filter controls
        document.getElementById('taskFilter').addEventListener('change', (e) => {
            state.tasks.filter = e.target.value;
            state.tasks.offset = 0;
            fetchTasks().then(renderTasks);
        });

        document.getElementById('taskIndex').addEventListener('change', (e) => {
            state.tasks.indexUid = e.target.value;
            state.tasks.offset = 0;
            fetchTasks().then(renderTasks);
        });

        document.getElementById('taskType').addEventListener('change', (e) => {
            state.tasks.type = e.target.value;
            state.tasks.offset = 0;
            fetchTasks().then(renderTasks);
        });

        // Pagination
        document.getElementById('taskPrevPage').addEventListener('click', () => {
            const newOffset = Math.max(0, state.tasks.offset - state.tasks.limit);
            state.tasks.offset = newOffset;
            fetchTasks().then(renderTasks);
        });

        document.getElementById('taskNextPage').addEventListener('click', () => {
            const newOffset = state.tasks.offset + state.tasks.limit;
            state.tasks.offset = newOffset;
            fetchTasks().then(renderTasks);
        });

        // Task details modal
        document.getElementById('taskDetailsModalClose').addEventListener('click', () => hideModal('taskDetailsModal'));
        document.getElementById('taskDetailsCloseBtn').addEventListener('click', () => hideModal('taskDetailsModal'));
    }

    function init() {
        initNavigation();
        initMobileMenu();
        initRefreshButton();
        initModals();

        // Initial data fetch
        refreshData().then(() => {
            // Render overview after initial fetch
            renderOverview();
        }).catch(err => {
            console.error('Initial data fetch failed:', err);
            // Show error in overview
            document.getElementById('clusterStatus').textContent = 'Error';
            document.getElementById('clusterStatus').style.color = 'var(--error-color)';
        });

        initAutoRefresh();

        // Refresh on visibility change (user returns to tab)
        document.addEventListener('visibilitychange', () => {
            if (document.visibilityState === 'visible') {
                refreshData();
            }
        });
    }

    // Start the app when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

    // Export functions to global scope for onclick handlers
    window.openSettingsModal = openSettingsModal;
    window.confirmDeleteIndex = confirmDeleteIndex;
    window.openFlipAliasModal = openFlipAliasModal;
    window.showAliasHistory = showAliasHistory;
    window.confirmDeleteAlias = confirmDeleteAlias;

})();
