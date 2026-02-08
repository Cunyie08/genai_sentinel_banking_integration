import React, { useEffect, useRef } from 'react';
import { useSelector } from 'react-redux';
import { Activity, Cpu } from 'lucide-react';

const GlassBoxTerminal = () => {
  const { thoughtLogs, activeAgent } = useSelector((state) => state.simulation);
  const bottomRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [thoughtLogs]);

  return (
    <div className="h-full flex flex-col font-mono bg-sentinel-dark/50 rounded-xl border border-sentinel-card p-1">
      
      {/* TERMINAL HEADER */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-sentinel-card bg-sentinel-card/30 rounded-t-lg">
        <div className="flex items-center gap-3">
            <Activity size={18} className="text-sentinel-accent" />
            <h2 className="text-xs font-bold tracking-widest text-slate-400 uppercase">Sentinel Core // Live Trace</h2>
        </div>
        <div className="flex items-center gap-2 px-2 py-1 bg-black/40 rounded border border-slate-700/50">
            <Cpu size={12} className="text-blue-400" />
            <span className="text-[10px] text-slate-400 uppercase tracking-wider">{activeAgent}</span>
        </div>
      </div>

      {/* LOGS */}
      <div className="flex-1 overflow-y-auto p-4 space-y-3 no-scrollbar">
        {thoughtLogs.map((log, index) => (
            <div key={index} className="flex gap-3 text-xs group hover:bg-white/5 p-2 rounded transition-colors border-l-2 border-transparent hover:border-sentinel-accent">
                
                {/* --- SAFE TIMESTAMP PARSING FIX --- */}
                <span className="text-slate-500 font-mono w-16 shrink-0 pt-0.5">
                    {log.timestamp && log.timestamp.includes('T') 
                        ? log.timestamp.split('T')[1].split('.')[0] 
                        : log.timestamp}
                </span>
                
                <div className="flex-1">
                    <div className="flex gap-2 mb-1">
                         <span className={`font-bold px-1.5 rounded-sm uppercase text-[10px] tracking-wide ${
                            log.status === 'success' ? 'text-emerald-400 bg-emerald-500/10' :
                            log.status === 'processing' ? 'text-blue-400 bg-blue-500/10' :
                            'text-red-400 bg-red-500/10'
                        }`}>
                            {log.agent}
                        </span>
                        <span className="text-slate-400">:: {log.step}</span>
                    </div>
                    <span className="text-slate-300 block pl-1 border-l border-slate-700/50">
                        {log.detail}
                    </span>
                </div>
            </div>
        ))}
        <div ref={bottomRef} />
      </div>
    </div>
  );
};

export default GlassBoxTerminal;