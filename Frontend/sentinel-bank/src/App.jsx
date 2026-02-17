import React from 'react';
import MobileSimulator from './components/MobileSimulator';
import GlassBoxTerminal from './components/GlassBoxTerminal';

function App() {
  return (
    <div className="flex h-screen bg-sentinel-dark text-sentinel-text w-full">
      
      {/* LEFT PANEL: User Experience (40%) */}
      <div className="w-2/5 min-w-[400px] flex items-center justify-center bg-[#020617] border-r border-slate-800 relative z-10 shadow-2xl">
        {/* Abstract Background Glow */}
        <div className="absolute inset-0 bg-gradient-to-b from-blue-900/10 via-transparent to-transparent pointer-events-none"></div>
        
        {/* The Phone */}
        <div className="transform scale-[0.85] 2xl:scale-100 transition-transform duration-500 ease-out hover:scale-[0.87]">
            <MobileSimulator />
        </div>
      </div>

      {/* RIGHT PANEL: The "Glass Box" (60%) */}
      <div className="w-3/5 p-6 flex flex-col bg-sentinel-dark relative">
        {/* Grid Background Pattern */}
        <div className="absolute inset-0 opacity-[0.03]" 
             style={{ backgroundImage: 'radial-gradient(#fff 1px, transparent 1px)', backgroundSize: '20px 20px' }}>
        </div>

        {/* Terminal Container */}
        <div className="relative z-10 h-full shadow-2xl">
            <GlassBoxTerminal />
        </div>
      </div>

    </div>
  );
}

export default App;