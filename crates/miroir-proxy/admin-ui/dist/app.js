/**
 * Miroir Admin UI - Plan §13.19
 * Overview and Topology sections implementation
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
        isConnected: true
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

    function init() {
        initNavigation();
        initMobileMenu();
        initRefreshButton();

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

})();
