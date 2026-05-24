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
        currentAliasForHistory: null
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
