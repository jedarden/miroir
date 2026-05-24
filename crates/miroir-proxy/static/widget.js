// Miroir Search Web Component (plan §13.21)
// <script src=".../ui/widget.js"></script>
// <miroir-search index="products" accent="#2563eb"></miroir-search>

(function() {
    'use strict';

    const TEMPLATE = document.createElement('template');
    TEMPLATE.innerHTML = `
        <style>
            :host {
                display: block;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
                --miroir-accent: #2563eb;
                --miroir-bg: #ffffff;
                --miroir-text: #111827;
                --miroir-border: #e5e7eb;
            }

            .miroir-widget-container {
                border: 1px solid var(--miroir-border);
                border-radius: 0.5rem;
                overflow: hidden;
                background-color: var(--miroir-bg);
                color: var(--miroir-text);
            }

            .miroir-widget-header {
                padding: 0.75rem 1rem;
                border-bottom: 1px solid var(--miroir-border);
                display: flex;
                gap: 0.5rem;
                align-items: center;
            }

            .miroir-widget-input {
                flex: 1;
                padding: 0.5rem 0.75rem;
                border: 1px solid var(--miroir-border);
                border-radius: 0.375rem;
                font-size: 0.875rem;
                outline: none;
            }

            .miroir-widget-input:focus {
                border-color: var(--miroir-accent);
                box-shadow: 0 0 0 2px rgba(37, 99, 235, 0.1);
            }

            .miroir-widget-button {
                padding: 0.5rem 1rem;
                background-color: var(--miroir-accent);
                color: white;
                border: none;
                border-radius: 0.375rem;
                cursor: pointer;
                font-size: 0.875rem;
                transition: background-color 0.2s;
            }

            .miroir-widget-button:hover {
                opacity: 0.9;
            }

            .miroir-widget-results {
                max-height: 400px;
                overflow-y: auto;
            }

            .miroir-widget-result {
                padding: 0.75rem 1rem;
                border-bottom: 1px solid var(--miroir-border);
                cursor: pointer;
                transition: background-color 0.15s;
            }

            .miroir-widget-result:hover {
                background-color: #f9fafb;
            }

            .miroir-widget-result:last-child {
                border-bottom: none;
            }

            .miroir-widget-result-title {
                font-weight: 600;
                color: var(--miroir-accent);
                margin-bottom: 0.25rem;
            }

            .miroir-widget-result-snippet {
                font-size: 0.875rem;
                color: #6b7280;
            }

            .miroir-widget-loading {
                padding: 1rem;
                text-align: center;
                color: #6b7280;
            }

            .miroir-widget-empty {
                padding: 1rem;
                text-align: center;
                color: #6b7280;
            }

            .miroir-widget-error {
                padding: 1rem;
                background-color: #fee2e2;
                color: #991b1b;
            }

            /* Dark mode support via attribute */
            :host([dark-mode]) {
                --miroir-bg: #1f2937;
                --miroir-text: #f9fafb;
                --miroir-border: #374151;
            }

            :host([dark-mode]) .miroir-widget-result:hover {
                background-color: #374151;
            }
        </style>
        <div class="miroir-widget-container">
            <div class="miroir-widget-header">
                <input type="text" class="miroir-widget-input" placeholder="Search..." />
                <button class="miroir-widget-button">Search</button>
            </div>
            <div class="miroir-widget-results"></div>
        </div>
    `;

    class MiroirSearch extends HTMLElement {
        constructor() {
            super();
            this.attachShadow({ mode: 'open' });
            this.shadowRoot.appendChild(TEMPLATE.content.cloneNode(true));

            this._index = null;
            this._accent = null;
            this._origin = null;
            this._sessionToken = null;
            this._debounceTimer = null;
            this._sessionId = crypto.randomUUID();
        }

        static get observedAttributes() {
            return ['index', 'accent', 'origin', 'dark-mode'];
        }

        attributeChangedCallback(name, oldValue, newValue) {
            if (oldValue === newValue) return;

            switch (name) {
                case 'index':
                    this._index = newValue;
                    this._loadSession();
                    break;
                case 'accent':
                    this._accent = newValue;
                    this.style.setProperty('--miroir-accent', newValue);
                    break;
                case 'origin':
                    this._origin = newValue;
                    break;
                case 'dark-mode':
                    // Handled by CSS :host([dark-mode]) selector
                    break;
            }
        }

        connectedCallback() {
            // Get attributes
            this._index = this.getAttribute('index') || 'default';
            this._accent = this.getAttribute('accent');
            this._origin = this.getAttribute('origin') || window.location.origin;

            // Set accent color if provided
            if (this._accent) {
                this.style.setProperty('--miroir-accent', this._accent);
            }

            // Set up event listeners
            this._input = this.shadowRoot.querySelector('.miroir-widget-input');
            this._button = this.shadowRoot.querySelector('.miroir-widget-button');
            this._resultsContainer = this.shadowRoot.querySelector('.miroir-widget-results');

            this._button.addEventListener('click', () => this._performSearch());
            this._input.addEventListener('input', () => {
                clearTimeout(this._debounceTimer);
                this._debounceTimer = setTimeout(() => this._performSearch(), 150);
            });
            this._input.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') {
                    this._performSearch();
                }
            });

            // Load session
            this._loadSession();

            // Dispatch custom event when ready
            this.dispatchEvent(new CustomEvent('miroir-ready', {
                bubbles: true,
                composed: true,
                detail: { index: this._index }
            }));
        }

        async _loadSession() {
            if (!this._index) return;

            try {
                const response = await fetch(`${this._origin}/_miroir/ui/search/${this._index}/session`);
                if (!response.ok) {
                    throw new Error('Failed to get session');
                }

                const data = await response.json();
                this._sessionToken = data.token;
            } catch (error) {
                this._showError('Failed to initialize: ' + error.message);
            }
        }

        async _performSearch() {
            if (!this._sessionToken) {
                this._showError('Session not loaded');
                return;
            }

            const query = this._input.value.trim();
            if (!query) {
                this._resultsContainer.innerHTML = '';
                return;
            }

            this._showLoading();

            try {
                const response = await fetch(`${this._origin}/indexes/${this._index}/search`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Authorization': `Bearer ${this._sessionToken}`
                    },
                    body: JSON.stringify({
                        q: query,
                        limit: 10,
                        attributesToRetrieve: ['*'],
                        attributesToHighlight: ['*']
                    })
                });

                if (!response.ok) {
                    throw new Error(`Search failed: ${response.status}`);
                }

                const data = await response.json();
                this._renderResults(data);

                // Dispatch custom event with results
                this.dispatchEvent(new CustomEvent('miroir-results', {
                    bubbles: true,
                    composed: true,
                    detail: {
                        query,
                        count: data.estimatedTotalHits || 0,
                        hits: data.hits || []
                    }
                }));

            } catch (error) {
                this._showError(error.message);
            }
        }

        _renderResults(data) {
            if (!data.hits || data.hits.length === 0) {
                this._resultsContainer.innerHTML = '<div class="miroir-widget-empty">No results found</div>';
                return;
            }

            const html = data.hits.map((hit, index) => {
                const formatted = hit._formatted || {};
                const titleAttr = this._getAttributeValue(formatted, hit, 'title') ||
                                this._getAttributeValue(formatted, hit, 'name') ||
                                hit.id || 'Untitled';
                const snippet = this._getAttributeValue(formatted, hit, 'description') || '';

                return `
                    <div class="miroir-widget-result" data-index="${index}">
                        <div class="miroir-widget-result-title">${this._escapeHtml(titleAttr)}</div>
                        ${snippet ? `<div class="miroir-widget-result-snippet">${this._escapeHtml(snippet)}</div>` : ''}
                    </div>
                `;
            }).join('');

            this._resultsContainer.innerHTML = html;

            // Add click listeners
            this._resultsContainer.querySelectorAll('.miroir-widget-result').forEach(result => {
                result.addEventListener('click', () => {
                    const index = parseInt(result.dataset.index, 10);
                    const hit = data.hits[index];
                    this.dispatchEvent(new CustomEvent('miroir-result-click', {
                        bubbles: true,
                        composed: true,
                        detail: {
                            hit,
                            index,
                            query: this._input.value
                        }
                    }));
                });
            });
        }

        _getAttributeValue(formatted, hit, attr) {
            return formatted[attr] || hit[attr];
        }

        _escapeHtml(text) {
            if (typeof text !== 'string') return '';
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        _showLoading() {
            this._resultsContainer.innerHTML = '<div class="miroir-widget-loading">Searching...</div>';
        }

        _showError(message) {
            this._resultsContainer.innerHTML = `<div class="miroir-widget-error">${this._escapeHtml(message)}</div>`;
        }

        // Public API methods

        /** Perform a search with the given query */
        search(query) {
            this._input.value = query;
            this._performSearch();
        }

        /** Clear the search input and results */
        clear() {
            this._input.value = '';
            this._resultsContainer.innerHTML = '';
        }
    }

    // Register the custom element
    if (!customElements.get('miroir-search')) {
        customElements.define('miroir-search', MiroirSearch);
    }
})();
