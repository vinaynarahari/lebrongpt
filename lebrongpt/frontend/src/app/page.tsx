'use client';

import { useState } from 'react';

interface PlayerStats {
  player_name: string;
  stats: Record<string, string | number>;
}

export default function Home() {
  const [player2, setPlayer2] = useState('');
  const [player1Stats, setPlayer1Stats] = useState<PlayerStats | null>(null);
  const [player2Stats, setPlayer2Stats] = useState<PlayerStats | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const fetchPlayerStats = async (playerName: string) => {
    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/player/${encodeURIComponent(playerName)}`);
      if (!response.ok) {
        throw new Error('Player not found');
      }
      return await response.json();
    } catch (error) {
      console.error('Error fetching player stats:', error);
      throw error;
    }
  };

  const handleCompare = async () => {
    setLoading(true);
    setError('');
    try {
      // Fetch stats for both players
      const [lebronStats, player2Data] = await Promise.all([
        fetchPlayerStats('LeBron James'),
        fetchPlayerStats(player2)
      ]);

      setPlayer1Stats(lebronStats);
      setPlayer2Stats(player2Data);

      // Get comparison
      const comparisonResponse = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/compare`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          player1: 'LeBron James',
          player2,
        }),
      });

      if (!comparisonResponse.ok) {
        throw new Error('Failed to compare players');
      }

      const comparisonData = await comparisonResponse.json();
      // Update stats with comparison data
      setPlayer1Stats(prev => ({ ...prev!, stats: comparisonData.player1_stats }));
      setPlayer2Stats(prev => ({ ...prev!, stats: comparisonData.player2_stats }));
    } catch (error) {
      console.error('Error:', error);
      setError(error instanceof Error ? error.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  const renderStatsTable = (stats: Record<string, string | number>) => {
    return (
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Stat</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Value</th>
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-200">
          {Object.entries(stats).map(([key, value]) => (
            <tr key={key}>
              <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{key}</td>
              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{value}</td>
            </tr>
          ))}
        </tbody>
      </table>
    );
  };

  return (
    <main className="min-h-screen p-8">
      <h1 className="text-4xl font-bold mb-8 text-center">LeBronGPT 🏀</h1>
      
      <div className="max-w-4xl mx-auto">
        <div className="space-y-4 mb-8">
          <div>
            <label className="block text-sm font-medium mb-2">Player 1 (LeBron)</label>
            <input
              type="text"
              value="LeBron James"
              disabled
              className="w-full p-2 border rounded bg-gray-100"
            />
          </div>
          
          <div>
            <label className="block text-sm font-medium mb-2">Player 2</label>
            <input
              type="text"
              value={player2}
              onChange={(e) => setPlayer2(e.target.value)}
              className="w-full p-2 border rounded"
              placeholder="Enter player name..."
            />
          </div>
          
          <button
            onClick={handleCompare}
            disabled={loading || !player2}
            className="w-full bg-blue-500 text-white p-2 rounded hover:bg-blue-600 disabled:bg-gray-400"
          >
            {loading ? 'Comparing...' : 'Compare Players'}
          </button>
        </div>
        
        {error && (
          <div className="mb-8 p-4 bg-red-50 text-red-700 rounded">
            {error}
          </div>
        )}

        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
          {player1Stats && (
            <div className="p-4 bg-gray-50 rounded">
              <h2 className="text-xl font-semibold mb-4">LeBron James Stats</h2>
              {renderStatsTable(player1Stats.stats)}
            </div>
          )}
          
          {player2Stats && (
            <div className="p-4 bg-gray-50 rounded">
              <h2 className="text-xl font-semibold mb-4">{player2} Stats</h2>
              {renderStatsTable(player2Stats.stats)}
            </div>
          )}
        </div>
      </div>
    </main>
  );
}
