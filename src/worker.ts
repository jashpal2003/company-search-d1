// Cloudflare Worker for Company Search API
// Uses D1 Database (10 GB FREE)

export interface Env {
  DB: D1Database;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    // CORS headers for all responses
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Content-Type': 'application/json'
    };

    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    try {
      // Route: Health Check
      if (path === '/api/health') {
        return new Response(
          JSON.stringify({
            status: 'healthy',
            database: 'D1',
            storage: '10 GB FREE',
            timestamp: new Date().toISOString()
          }),
          { headers: corsHeaders }
        );
      }

      // Route: Search Companies
      if (path === '/api/search') {
        const query = url.searchParams.get('q');
        const page = parseInt(url.searchParams.get('page') || '1');
        const perPage = Math.min(parseInt(url.searchParams.get('per_page') || '20'), 50);

        if (!query || query.length < 2) {
          return new Response(
            JSON.stringify({ error: 'Query must be at least 2 characters' }),
            { status: 400, headers: corsHeaders }
          );
        }

        const offset = (page - 1) * perPage;

        // Use FTS5 full-text search for better performance
        const { results: companies } = await env.DB.prepare(
          `SELECT c.id, c.company_name, c.cin, c.status, 
                  c.registration_date, c.company_class, c.roc
           FROM companies c
           WHERE c.company_name LIKE ?
           ORDER BY c.company_name
           LIMIT ? OFFSET ?`
        ).bind(`%${query}%`, perPage, offset).all();

        // Get total count
        const { total } = await env.DB.prepare(
          'SELECT COUNT(*) as total FROM companies WHERE company_name LIKE ?'
        ).bind(`%${query}%`).first() || { total: 0 };

        return new Response(
          JSON.stringify({
            success: true,
            query,
            total: total || 0,
            page,
            per_page: perPage,
            total_pages: Math.ceil((total || 0) / perPage),
            companies: companies || []
          }),
          { headers: corsHeaders }
        );
      }

      // Route: Get Company by CIN
      if (path.startsWith('/api/company/')) {
        const cin = path.split('/').pop();

        if (!cin) {
          return new Response(
            JSON.stringify({ error: 'CIN is required' }),
            { status: 400, headers: corsHeaders }
          );
        }

        const company = await env.DB.prepare(
          'SELECT * FROM companies WHERE cin = ?'
        ).bind(cin).first();

        if (!company) {
          return new Response(
            JSON.stringify({ error: 'Company not found' }),
            { status: 404, headers: corsHeaders }
          );
        }

        return new Response(
          JSON.stringify({
            success: true,
            company
          }),
          { headers: corsHeaders }
        );
      }

      // Route: Database Statistics
      if (path === '/api/stats') {
        const stats = await env.DB.prepare(`
          SELECT 
            COUNT(*) as total_companies,
            COUNT(CASE WHEN status = 'Active' THEN 1 END) as active_companies,
            MAX(updated_at) as last_update
          FROM companies
        `).first();

        return new Response(
          JSON.stringify({
            total_companies: stats?.total_companies || 0,
            active_companies: stats?.active_companies || 0,
            inactive_companies: (stats?.total_companies || 0) - (stats?.active_companies || 0),
            last_update: stats?.last_update || null,
            tier: 'Cloudflare D1 FREE (10 GB)',
            limits: {
              storage: '10 GB',
              reads: '1 million/day',
              writes: '100k/day'
            }
          }),
          { headers: corsHeaders }
        );
      }

      // Route: Serve Frontend (index.html)
      if (path === '/' || path === '/index.html') {
        return new Response(HTML_CONTENT, {
          headers: {
            'Content-Type': 'text/html',
          }
        });
      }

      // 404 Not Found
      return new Response(
        JSON.stringify({ error: 'Not found' }),
        { status: 404, headers: corsHeaders }
      );

    } catch (error) {
      console.error('Error:', error);
      return new Response(
        JSON.stringify({ 
          error: 'Internal server error',
          message: error instanceof Error ? error.message : 'Unknown error'
        }),
        { status: 500, headers: corsHeaders }
      );
    }
  },
};

// Embedded HTML Frontend (can also be served from Pages)
const HTML_CONTENT = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Company Search - India (2M+ Companies)</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        .header {
            text-align: center;
            color: white;
            margin-bottom: 40px;
        }
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        .header p { font-size: 1.2em; opacity: 0.9; }
        .search-card {
            background: white;
            border-radius: 15px;
            padding: 40px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            margin-bottom: 30px;
        }
        .search-box {
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
        }
        .search-input {
            flex: 1;
            padding: 15px;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            font-size: 16px;
        }
        .search-btn {
            padding: 15px 30px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            border-radius: 8px;
            font-size: 1.1em;
            cursor: pointer;
        }
        .results { background: white; border-radius: 15px; padding: 30px; box-shadow: 0 20px 60px rgba(0,0,0,0.3); display: none; }
        .results.show { display: block; }
        .result-card {
            background: #f8f9fa;
            border-radius: 10px;
            padding: 20px;
            margin-bottom: 20px;
            border-left: 5px solid #667eea;
        }
        .loading { text-align: center; padding: 20px; display: none; }
        .loading.show { display: block; }
        .badge { 
            display: inline-block;
            padding: 5px 10px;
            border-radius: 5px;
            font-size: 0.9em;
            margin-top: 10px;
        }
        .badge-success { background: #d4edda; color: #155724; }
        .badge-danger { background: #f8d7da; color: #721c24; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🏢 Company Search India</h1>
            <p>Search 2M+ Indian Companies - Powered by Cloudflare D1 (100% FREE)</p>
        </div>

        <div class="search-card">
            <div class="search-box">
                <input type="text" class="search-input" id="searchInput" placeholder="Enter company name (e.g., Tata, Reliance, Infosys)">
                <button class="search-btn" onclick="searchCompanies()">🔍 Search</button>
            </div>
            <div class="loading" id="loading">Searching...</div>
            <div id="error"></div>
        </div>

        <div class="results" id="results"></div>
    </div>

    <script>
        async function searchCompanies() {
            const query = document.getElementById('searchInput').value.trim();
            const loading = document.getElementById('loading');
            const results = document.getElementById('results');
            const error = document.getElementById('error');

            if (!query || query.length < 2) {
                error.innerHTML = '<p style="color: red;">Please enter at least 2 characters</p>';
                return;
            }

            error.innerHTML = '';
            results.classList.remove('show');
            loading.classList.add('show');

            try {
                const response = await fetch(\`/api/search?q=\${encodeURIComponent(query)}&page=1&per_page=20\`);
                const data = await response.json();

                if (data.success && data.companies.length > 0) {
                    displayResults(data);
                } else {
                    error.innerHTML = '<p style="color: orange;">No companies found</p>';
                }
            } catch (err) {
                error.innerHTML = '<p style="color: red;">Search failed. Please try again.</p>';
            } finally {
                loading.classList.remove('show');
            }
        }

        function displayResults(data) {
            const results = document.getElementById('results');
            results.innerHTML = \`
                <h2>Found \${data.total} Companies</h2>
                <div>
                    \${data.companies.map((company, index) => \`
                        <div class="result-card">
                            <h3>\${company.company_name}</h3>
                            <p><strong>CIN:</strong> \${company.cin}</p>
                            <p><strong>Status:</strong> 
                                <span class="badge \${company.status === 'Active' ? 'badge-success' : 'badge-danger'}">
                                    \${company.status}
                                </span>
                            </p>
                            <p><strong>Registration:</strong> \${company.registration_date || 'N/A'}</p>
                            <p><strong>Class:</strong> \${company.company_class || 'N/A'}</p>
                            <p><strong>ROC:</strong> \${company.roc || 'N/A'}</p>
                        </div>
                    \`).join('')}
                </div>
            \`;
            results.classList.add('show');
        }

        // Allow Enter key to search
        document.getElementById('searchInput').addEventListener('keypress', function(e) {
            if (e.key === 'Enter') searchCompanies();
        });
    </script>
</body>
</html>`;
