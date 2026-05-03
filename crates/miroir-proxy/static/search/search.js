// Miroir Search UI - SPA
(function() {
    'use strict';

    // Configuration
    const DEBOUNCE_MS = 150;
    const RESULTS_PER_PAGE = 20;

    // State
    let currentIndex = null;
    let sessionToken = null;
    let currentQuery = '';
    let currentFilters = {};
    let currentPage = 0;
    let debounceTimer = null;
    let config = null;

    // Initialize
    function init() {
        const indexMatch = window.location.pathname.match(/\/ui\/search\/([^/]+)/);
        if (!indexMatch) {
            showError('No index specified');
            return;
        }

        currentIndex = indexMatch[1];
        setupEventListeners();
        loadSession();
    }

    // Session management
    async function loadSession() {
        try {
            const response = await fetch(`/_miroir/ui/search/${currentIndex}/session`);
            if (!response.ok) {
                throw new Error('Failed to get session');
            }

            const data = await response.json();
            sessionToken = data.token;

            // Load config after session
            await loadConfig();

            // Update UI
            document.getElementById('logo').textContent = config?.title || 'Search';

        } catch (error) {
            showError('Failed to initialize search: ' + error.message);
        }
    }

    async function loadConfig() {
        try {
            const response = await fetch(`/_miroir/ui/search/${currentIndex}/config`);
            if (response.ok) {
                config = await response.json();
            }
        } catch (error) {
            console.warn('Failed to load config:', error);
        }
    }

    // API helper
    async function search(query, filters = {}, page = 0) {
        const requestBody = {
            q: query,
            limit: RESULTS_PER_PAGE,
            offset: page * RESULTS_PER_PAGE
        };

        // Add filters
        const filterParts = [];
        for (const [key, values] of Object.entries(filters)) {
            if (Array.isArray(values) && values.length > 0) {
                filterParts.push(`${key} IN ${JSON.stringify(values)}`);
            }
        }
        if (filterParts.length > 0) {
            requestBody.filter = filterParts.join(' AND ');
        }

        const response = await fetch(`/indexes/${currentIndex}/search`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${sessionToken}`
            },
            body: JSON.stringify(requestBody)
        });

        if (!response.ok) {
            throw new Error(`Search failed: ${response.status}`);
        }

        return response.json();
    }

    // Event listeners
    function setupEventListeners() {
        const searchInput = document.getElementById('searchInput');
        const searchBtn = document.getElementById('searchBtn');

        // Debounced search on input
        searchInput.addEventListener('input', (e) => {
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(() => {
                performSearch(e.target.value, 0);
            }, DEBOUNCE_MS);
        });

        // Search button click
        searchBtn.addEventListener('click', () => {
            performSearch(searchInput.value, 0);
        });

        // Enter key
        searchInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                performSearch(searchInput.value, 0);
            }
        });

        // Focus search on slash key
        document.addEventListener('keydown', (e) => {
            if (e.key === '/' && document.activeElement !== searchInput) {
                e.preventDefault();
                searchInput.focus();
            }
        });
    }

    // Perform search
    async function performSearch(query, page) {
        currentQuery = query;
        currentPage = page;

        const resultsDiv = document.getElementById('results');
        resultsDiv.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

        try {
            const data = await search(query, currentFilters, page);
            renderResults(data);
            renderFacets(data);
            renderPagination(data);
            updateResultCount(data);
        } catch (error) {
            showError(error.message);
        }
    }

    // Render results
    function renderResults(data) {
        const resultsDiv = document.getElementById('results');

        if (!data.hits || data.hits.length === 0) {
            resultsDiv.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">🔍</div>
                    <div class="empty-state-title">No results found</div>
                    <p>Try adjusting your search or filters</p>
                </div>
            `;
            return;
        }

        const resultsHtml = data.hits.map(hit => {
            const title = hit[config?.display_attributes?.[0] || 'title'] || hit.id || 'Untitled';
            const snippet = hit[config?.display_attributes?.[1] || 'description'] || '';
            const url = config?.hit_url_template?.replace(`{${config?.primary_key_field || 'id'}}`, hit.id || '') || '#';

            return `
                <div class="result-card">
                    <div class="result-title">
                        <a href="${url}" target="_blank" rel="noopener">${escapeHtml(title)}</a>
                    </div>
                    ${snippet ? `<div class="result-snippet">${escapeHtml(snippet)}</div>` : ''}
                    <div class="result-meta">
                        <span>ID: ${escapeHtml(String(hit.id || ''))}</span>
                        ${hit._rankingScore ? `<span>Score: ${hit._rankingScore.toFixed(2)}</span>` : ''}
                    </div>
                </div>
            `;
        }).join('');

        resultsDiv.innerHTML = resultsHtml;
    }

    // Render facets
    function renderFacets(data) {
        const facetsDiv = document.getElementById('facets');

        if (!data.facetDistribution || Object.keys(data.facetDistribution).length === 0) {
            facetsDiv.innerHTML = '';
            return;
        }

        const facetsHtml = Object.entries(data.facetDistribution).map(([facetName, values]) => `
            <div class="facet-group">
                <div class="facet-title">${escapeHtml(facetName)}</div>
                ${Object.entries(values).slice(0, 10).map(([value, count]) => `
                    <label class="facet-option">
                        <input
                            type="checkbox"
                            data-facet="${escapeHtml(facetName)}"
                            data-value="${escapeHtml(value)}"
                            ${currentFilters[facetName]?.includes(value) ? 'checked' : ''}
                        >
                        <span>${escapeHtml(value)}</span>
                        <span class="facet-count">${count}</span>
                    </label>
                `).join('')}
            </div>
        `).join('');

        facetsDiv.innerHTML = facetsHtml;

        // Add event listeners to checkboxes
        facetsDiv.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
            checkbox.addEventListener('change', () => {
                const facet = checkbox.dataset.facet;
                const value = checkbox.dataset.value;

                if (!currentFilters[facet]) {
                    currentFilters[facet] = [];
                }

                if (checkbox.checked) {
                    currentFilters[facet].push(value);
                } else {
                    currentFilters[facet] = currentFilters[facet].filter(v => v !== value);
                }

                performSearch(currentQuery, 0);
            });
        });
    }

    // Render pagination
    function renderPagination(data) {
        const paginationDiv = document.getElementById('pagination');
        const totalPages = Math.ceil((data.estimatedTotalHits || 0) / RESULTS_PER_PAGE);

        if (totalPages <= 1) {
            paginationDiv.innerHTML = '';
            return;
        }

        const currentPageNum = currentPage + 1;
        let pages = [];

        // Always show first page
        pages.push(1);

        // Show pages around current page
        for (let i = Math.max(2, currentPageNum - 2); i <= Math.min(totalPages - 1, currentPageNum + 2); i++) {
            pages.push(i);
        }

        // Always show last page
        if (totalPages > 1) {
            pages.push(totalPages);
        }

        // Remove duplicates and sort
        pages = [...new Set(pages)].sort((a, b) => a - b);

        const paginationHtml = pages.map((page, index) => {
            const prevPage = pages[index - 1];
            const showEllipsisBefore = prevPage && prevPage < page - 1;

            let html = '';

            if (showEllipsisBefore) {
                html += '<button disabled>...</button>';
            }

            const isActive = page === currentPageNum;
            html += `<button class="${isActive ? 'active' : ''}" data-page="${page - 1}">${page}</button>`;

            return html;
        }).join('');

        paginationDiv.innerHTML = paginationHtml;

        // Add event listeners
        paginationDiv.querySelectorAll('button:not(:disabled)').forEach(button => {
            button.addEventListener('click', () => {
                const page = parseInt(button.dataset.page);
                performSearch(currentQuery, page);
                window.scrollTo({ top: 0, behavior: 'smooth' });
            });
        });
    }

    // Update result count
    function updateResultCount(data) {
        const count = data.estimatedTotalHits || 0;
        const time = data.processingTimeMs || 0;
        document.getElementById('resultCount').textContent =
            `${count.toLocaleString()} results (${time}ms)`;
    }

    // Show error
    function showError(message) {
        const resultsDiv = document.getElementById('results');
        resultsDiv.innerHTML = `<div class="error">${escapeHtml(message)}</div>`;
    }

    // Escape HTML
    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // Start the app
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
