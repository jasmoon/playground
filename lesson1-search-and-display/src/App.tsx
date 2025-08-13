// src/App.tsx
import React, { useState, useEffect } from 'react';

const DEBOUNCE_DELAY = 300; // milliseconds

interface Repo {
  id: number;
  name: string;
  html_url: string;
  description: string | null;
  owner: {
    login: string;
  };
}

const App: React.FC = () => {
  const [query, setQuery] = useState<string>('');
  const [repos, setRepos] = useState<Repo[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [debouncedQuery, setDebouncedQuery] = useState(query);

  const onChangeQuery = React.useCallback(
  (e: React.ChangeEvent<HTMLInputElement>) => {
    setQuery(e.target.value);
  },
  [] // no dependencies, stable function reference
);

  // Update debouncedQuery only after user stops typing for 300ms
  useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedQuery(query);
    }, DEBOUNCE_DELAY);

    // Cleanup if query changes before 300ms (resets the timer)
    return () => {
      clearTimeout(handler);
    };
  }, [query]);

  // Fetch repos when query changes
  useEffect(() => {
    if (!debouncedQuery) {
      setRepos([]);
      return;
    }

    
    setLoading(true);
    setError(null);

    fetch(`https://api.github.com/search/repositories?q=${encodeURIComponent(query)}&per_page=10`)
      .then(async (res) => {
        if (!res.ok) {
          throw new Error(`Error: ${res.status} ${res.statusText}`);
        }
        const data = await res.json();
        setRepos(data.items);
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [debouncedQuery]);

  const filteredRepos = React.useMemo(() => {
    return repos.filter((repo) => (repo.description?.length ?? 0) > 20);
  }, [repos]);

  return (
    <div style={{ padding: '1rem', fontFamily: 'Arial, sans-serif' }}>
      <h1>GitHub Repo Search</h1>
      <input
        type="text"
        placeholder="Type repo name..."
        value={query}
        onChange={onChangeQuery}
        style={{ padding: '0.5rem', width: '300px' }}
      />
      {loading && <p>Loading...</p>}
      {error && <p style={{ color: 'red' }}>Error: {error}</p>}
      {!loading && !error && filteredRepos.length > 0 && (
        <table border={1} cellPadding={8} style={{ marginTop: '1rem', borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th>Name</th>
              <th>Owner</th>
              <th>Description</th>
            </tr>
          </thead>
          <tbody>
            {filteredRepos.map((repo) => (
              <tr key={repo.id}>
                <td>
                  <a href={repo.html_url} target="_blank" rel="noopener noreferrer">
                    {repo.name}
                  </a>
                </td>
                <td>{repo.owner.login}</td>
                <td>{repo.description || '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {!loading && !error && query && repos.length === 0 && <p>No repos found.</p>}
    </div>
  );
};

export default App;