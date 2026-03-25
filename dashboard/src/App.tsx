import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Database, Activity, HardDrive, Cpu, AlertCircle, Clock, Zap, ChevronRight } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, AreaChart, Area } from 'recharts';

interface LsmState {
  memtable_size?: number;
  l0_sstables?: number;
  l1_sstables?: number;
  p99_latency?: number;
  [key: string]: any;
}

interface HistoryPoint {
  time: string;
  memtable_size: number;
  p99_latency: number;
}

const StatCard = ({ title, value, icon: Icon, unit = '', color = 'text-cyan-400', bgGlow = 'bg-cyan-500/10' }: any) => {
  return (
    <motion.div
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      className="bg-[#111]/80 backdrop-blur-xl border border-white/10 rounded-2xl p-6 relative overflow-hidden group transition-all hover:border-white/20"
    >
      <div className={`absolute -inset-px opacity-0 group-hover:opacity-100 transition-opacity duration-500 rounded-2xl ${bgGlow} blur-2xl`} />
      <div className="relative z-10">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-gray-400 font-medium text-sm tracking-wide">{title}</h3>
          <Icon size={18} className="text-gray-500 group-hover:text-gray-300 transition-colors" />
        </div>
        <div className="flex items-baseline gap-2">
          <motion.span 
            key={value}
            initial={{ opacity: 0, y: -10 }}
            animate={{ opacity: 1, y: 0 }}
            className={`text-3xl font-semibold font-mono text-gray-100 tracking-tight`}
          >
            {value !== undefined ? value.toLocaleString() : '--'}
          </motion.span>
          {unit && <span className="text-gray-500 text-xs font-mono">{unit}</span>}
        </div>
      </div>
    </motion.div>
  );
};

export default function App() {
  const [state, setState] = useState<LsmState | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [history, setHistory] = useState<HistoryPoint[]>([]);
  const [isSpiking, setIsSpiking] = useState(false);
  const [showToast, setShowToast] = useState(false);

  useEffect(() => {
    const fetchData = async () => {
      try {
        let currentData: LsmState = {};
        try {
          const response = await fetch('https://accretiondb.onrender.com/state');
          if (response.ok) {
            currentData = await response.json();
            setError(null);
          } else {
            throw new Error('Backend returned non-200');
          }
        } catch (e: any) {
          setError('Backend offline - Showing simulated data');
          // Fallback minimal simulated data so dashboard stays alive
          currentData = {
            memtable_size: 4096,
            l0_sstables: 12,
            l1_sstables: 34,
            p99_latency: 5
          };
        }

        if (isSpiking) {
          // Inject chaotic workload data
          currentData = {
            ...currentData,
            memtable_size: (currentData.memtable_size || 0) + 15000000 + Math.random() * 5000000,
            l0_sstables: (currentData.l0_sstables || 0) + 80 + Math.floor(Math.random() * 20),
            l1_sstables: (currentData.l1_sstables || 0) + 150 + Math.floor(Math.random() * 30),
            p99_latency: (currentData.p99_latency || 0) + 300 + Math.random() * 200,
          };
        }

        setState(currentData);
        
        setHistory(prev => {
          const newPoint = {
            time: new Date().toLocaleTimeString([], { hour12: false, second: '2-digit', minute: '2-digit' }),
            memtable_size: currentData.memtable_size || 0,
            p99_latency: currentData.p99_latency || 0,
          };
          const next = [...prev, newPoint];
          if (next.length > 30) next.shift(); // Keep last 30 points
          return next;
        });
        
      } catch (err: any) {
        console.error(err);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 1000);
    return () => clearInterval(interval);
  }, [isSpiking]);

  const triggerBenchmark = () => {
    setShowToast(true);
    setIsSpiking(true);
    setTimeout(() => {
      setIsSpiking(false);
      setShowToast(false);
    }, 5000);
  };

  return (
    <div className="min-h-screen bg-[#050505] text-gray-200 font-sans p-4 sm:p-8 relative selection:bg-cyan-500/30">
      {/* Refined subtle gradient background */}
      <div className="absolute top-0 inset-x-0 h-96 bg-gradient-to-b from-cyan-900/10 to-transparent pointer-events-none" />
      
      <style>{`
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #333; border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: #555; }
      `}</style>

      <div className="max-w-7xl mx-auto relative z-10">
        <header className="mb-10 flex flex-col md:flex-row md:items-center justify-between border-b border-white/5 pb-6">
          <div className="flex items-center gap-4 mb-6 md:mb-0">
            <div className="p-3 bg-[#111] rounded-2xl border border-white/10 shadow-lg shadow-black">
              <Database size={28} className="text-gray-100" />
            </div>
            <div>
              <h1 className="text-2xl font-semibold bg-clip-text text-transparent bg-gradient-to-r from-white to-gray-500 tracking-tight">AccretionDB</h1>
              <div className="flex items-center gap-2 text-gray-500 text-sm mt-1">
                <span>Production Environment</span>
                <ChevronRight size={14} />
                <span className="font-mono text-xs bg-white/5 px-2 py-0.5 rounded-full border border-white/10">us-east-1</span>
              </div>
            </div>
          </div>
          
          <div className="flex items-center gap-4 text-sm font-medium">
            <button 
              onClick={triggerBenchmark}
              disabled={isSpiking}
              className="flex items-center gap-2 bg-white text-black px-5 py-2.5 rounded-full font-semibold hover:bg-gray-200 hover:scale-[1.02] active:scale-[0.98] transition-all disabled:opacity-50 disabled:hover:scale-100 shadow-[0_0_20px_rgba(255,255,255,0.1)]"
            >
              <Zap size={16} className={isSpiking ? "animate-bounce" : ""} />
              {isSpiking ? "Benchmarking..." : "Simulate Workload"}
            </button>

            <AnimatePresence mode="wait">
              {error ? (
                <motion.div 
                  initial={{ opacity: 0, scale: 0.9 }}
                  animate={{ opacity: 1, scale: 1 }}
                  exit={{ opacity: 0, scale: 0.9 }}
                  className="flex items-center gap-2 text-amber-400 bg-amber-950/30 px-4 py-2 rounded-full border border-amber-500/20 text-xs backdrop-blur-sm"
                >
                  <AlertCircle size={14} />
                  <span>{error}</span>
                </motion.div>
              ) : (
                <motion.div
                  initial={{ opacity: 0, scale: 0.9 }}
                  animate={{ opacity: 1, scale: 1 }}
                  exit={{ opacity: 0, scale: 0.9 }}
                  className="flex items-center gap-2 text-emerald-400 bg-emerald-950/30 px-4 py-2 rounded-full border border-emerald-500/20 text-xs backdrop-blur-sm"
                >
                  <div className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
                  <span>System Healthy</span>
                </motion.div>
              )}
            </AnimatePresence>
          </div>
        </header>

        {/* Top Stats */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-5 mb-10">
          <StatCard 
            title="Memtable Size" 
            value={state?.memtable_size}
            icon={Cpu}
            unit="Bytes"
            bgGlow="bg-cyan-500/10"
          />
          <StatCard 
            title="L0 SSTables" 
            value={state?.l0_sstables}
            icon={HardDrive}
            unit="tables"
            bgGlow="bg-purple-500/10"
          />
          <StatCard 
            title="L1 SSTables" 
            value={state?.l1_sstables}
            icon={Database}
            unit="tables"
            bgGlow="bg-orange-500/10"
          />
          <StatCard 
            title="P99 Latency" 
            value={state?.p99_latency ? Math.round(state.p99_latency) : undefined}
            icon={Clock}
            unit="ms"
            bgGlow="bg-emerald-500/10"
          />
        </div>

        {/* Charts Section */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-5 mb-10">
          {/* Memtable Size Chart */}
          <div className="bg-[#111]/80 backdrop-blur-xl border border-white/10 rounded-2xl p-6 relative group hover:border-white/20 transition-colors">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-gray-100 font-medium text-sm flex items-center gap-2">
                Memtable Allocation History
              </h3>
              <div className="text-xs font-mono text-cyan-400 bg-cyan-950/30 px-2 py-1 rounded border border-cyan-500/20">
                LIVE
              </div>
            </div>
            <div className="h-[250px] w-full">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={history}>
                  <defs>
                    <linearGradient id="colorMemtable" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#22d3ee" stopOpacity={0.3}/>
                      <stop offset="95%" stopColor="#22d3ee" stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#222" vertical={false} />
                  <XAxis dataKey="time" stroke="#555" fontSize={11} tickMargin={10} axisLine={false} tickLine={false} />
                  <YAxis stroke="#555" fontSize={11} axisLine={false} tickLine={false} tickFormatter={(val) => (val / 1000000).toFixed(1) + 'M'} />
                  <Tooltip 
                    contentStyle={{ backgroundColor: '#111', border: '1px solid #333', borderRadius: '8px', color: '#fff' }}
                    itemStyle={{ color: '#22d3ee' }}
                  />
                  <Area type="monotone" dataKey="memtable_size" stroke="#22d3ee" strokeWidth={2} fillOpacity={1} fill="url(#colorMemtable)" animationDuration={300} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Latency Chart */}
          <div className="bg-[#111]/80 backdrop-blur-xl border border-white/10 rounded-2xl p-6 relative group hover:border-white/20 transition-colors">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-gray-100 font-medium text-sm flex items-center gap-2">
                P99 Latency Feed
              </h3>
              <div className="text-xs font-mono text-emerald-400 bg-emerald-950/30 px-2 py-1 rounded border border-emerald-500/20">
                LIVE
              </div>
            </div>
            <div className="h-[250px] w-full">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={history}>
                  <defs>
                    <linearGradient id="colorLatency" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#10b981" stopOpacity={0.3}/>
                      <stop offset="95%" stopColor="#10b981" stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#222" vertical={false} />
                  <XAxis dataKey="time" stroke="#555" fontSize={11} tickMargin={10} axisLine={false} tickLine={false} />
                  <YAxis stroke="#555" fontSize={11} axisLine={false} tickLine={false} />
                  <Tooltip 
                    contentStyle={{ backgroundColor: '#111', border: '1px solid #333', borderRadius: '8px', color: '#fff' }}
                    itemStyle={{ color: '#10b981' }}
                  />
                  <Area type="stepAfter" dataKey="p99_latency" stroke="#10b981" strokeWidth={2} fillOpacity={1} fill="url(#colorLatency)" animationDuration={300} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>

        {/* LSM Tree Visualizer */}
        <div className="bg-[#111]/80 backdrop-blur-xl border border-white/10 rounded-2xl p-6 relative mb-10 hover:border-white/20 transition-colors">
          <div className="flex flex-col md:flex-row md:items-center justify-between mb-8 gap-4">
            <h3 className="text-gray-100 font-medium text-sm flex items-center gap-2">
              <Database size={16} className="text-gray-400" /> LSM Tree Topology
            </h3>
            <div className="text-xs text-gray-500">Visualizing scale and compaction levels</div>
          </div>

          <div className="space-y-8">
            {/* Level 0 */}
            <div className="bg-[#0a0a0a] border border-white/5 rounded-xl p-4">
              <div className="flex items-center gap-4 mb-4">
                <span className="text-gray-100 font-semibold text-sm">Level 0</span>
                <div className="h-[1px] flex-1 bg-gradient-to-r from-purple-500/20 to-transparent"></div>
                <span className="text-purple-400 font-mono text-xs bg-purple-950/30 px-2 py-1 rounded">{state?.l0_sstables || 0} Tables</span>
              </div>
              <div className="flex flex-wrap gap-1.5 max-h-48 overflow-y-auto pr-2 pb-2">
                <AnimatePresence>
                  {Array.from({ length: state?.l0_sstables || 0 }).map((_, i) => (
                    <motion.div
                      key={`l0-${i}`}
                      initial={{ scale: 0, opacity: 0 }}
                      animate={{ scale: 1, opacity: 1 }}
                      exit={{ scale: 0, opacity: 0 }}
                      transition={{ duration: 0.2 }}
                      className="w-4 h-6 rounded-[3px] border border-purple-500/30 bg-purple-500/10 hover:bg-purple-500/30 transition-colors cursor-crosshair"
                      title={`L0 SSTable ${i}`}
                    />
                  ))}
                </AnimatePresence>
                {(state?.l0_sstables || 0) === 0 && (
                  <div className="text-gray-600 text-sm italic py-2">No SSTables in Level 0</div>
                )}
              </div>
            </div>

            {/* Level 1 */}
            <div className="bg-[#0a0a0a] border border-white/5 rounded-xl p-4">
              <div className="flex items-center gap-4 mb-4">
                <span className="text-gray-100 font-semibold text-sm">Level 1</span>
                <div className="h-[1px] flex-1 bg-gradient-to-r from-orange-500/20 to-transparent"></div>
                <span className="text-orange-400 font-mono text-xs bg-orange-950/30 px-2 py-1 rounded">{state?.l1_sstables || 0} Tables</span>
              </div>
              <div className="flex flex-wrap gap-1.5 max-h-48 overflow-y-auto pr-2 pb-2">
                <AnimatePresence>
                  {Array.from({ length: state?.l1_sstables || 0 }).map((_, i) => (
                    <motion.div
                      key={`l1-${i}`}
                      initial={{ scale: 0, opacity: 0 }}
                      animate={{ scale: 1, opacity: 1 }}
                      exit={{ scale: 0, opacity: 0 }}
                      transition={{ duration: 0.2 }}
                      className="w-6 h-6 rounded-[3px] border border-orange-500/30 bg-orange-500/10 hover:bg-orange-500/30 transition-colors cursor-crosshair"
                      title={`L1 SSTable ${i}`}
                    />
                  ))}
                </AnimatePresence>
                {(state?.l1_sstables || 0) === 0 && (
                  <div className="text-gray-600 text-sm italic py-2">No SSTables in Level 1</div>
                )}
              </div>
            </div>
          </div>
        </div>

      </div>
    </div>
  );
}
