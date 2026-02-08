import React, { useState, useEffect, useRef } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import { Send, Menu, Battery, Wifi, Signal } from 'lucide-react';
import { addMessage, setThinking, addThought } from '../store/simulationSlice';

const MobileSimulator = () => {
  const dispatch = useDispatch();
  const { messages, isThinking } = useSelector((state) => state.simulation);
  const [input, setInput] = useState('');
  const scrollRef = useRef(null);

  useEffect(() => {
    scrollRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, isThinking]);

  // WEEK 1 MOCK LOGIC (Replace with API in Week 2)
  const handleSend = () => {
    if (!input.trim()) return;

    // 1. User Message
    const userText = input;
    dispatch(addMessage({ id: Date.now(), sender: 'user', text: userText, timestamp: new Date().toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'}) }));
    setInput('');
    dispatch(setThinking(true));

    // 2. Mock AI Logic
    setTimeout(() => {
      dispatch(addThought({ id: Date.now(), agent: 'Dispatcher', step: 'Intent Analysis', status: 'processing', detail: `Analyzing input: "${userText}"`, timestamp: new Date().toISOString() }));
    }, 600);

    setTimeout(() => {
      dispatch(addThought({ id: Date.now() + 1, agent: 'Sentinel', step: 'Risk Scan', status: 'success', detail: 'Transaction Pattern: NORMAL', timestamp: new Date().toISOString() }));
    }, 1500);

    setTimeout(() => {
        dispatch(setThinking(false));
        dispatch(addMessage({ id: Date.now() + 2, sender: 'agent', text: 'I can help with that. Please verify your details.', timestamp: new Date().toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'}) }));
    }, 2500);
  };

  return (
    // PHONE FRAME
    <div className="relative w-[360px] h-[780px] bg-black rounded-[50px] border-[12px] border-gray-900 shadow-2xl overflow-hidden ring-4 ring-gray-800/50">
      
      {/* NOTCH */}
      <div className="absolute top-0 left-1/2 -translate-x-1/2 w-32 h-7 bg-black rounded-b-2xl z-20"></div>

      {/* SCREEN */}
      <div className="w-full h-full bg-mobile-bg flex flex-col pt-10 font-sans">
        
        {/* STATUS BAR */}
        <div className="px-6 flex justify-between items-center text-xs font-medium text-slate-800 mb-2">
            <span>9:41</span>
            <div className="flex gap-1">
                <Signal size={14} />
                <Wifi size={14} />
                <Battery size={14} />
            </div>
        </div>

        {/* HEADER */}
        <div className="bg-white px-4 py-3 shadow-sm flex items-center gap-3">
            <div className="p-2 bg-blue-600 rounded-full text-white">
                <Menu size={18} />
            </div>
            <div>
                <h1 className="font-bold text-slate-800 text-sm">Sentinel Bank</h1>
                <p className="text-[10px] text-emerald-600 font-medium">● Online</p>
            </div>
        </div>

        {/* CHAT AREA */}
        <div className="flex-1 overflow-y-auto p-4 space-y-4 no-scrollbar">
            {messages.map((msg) => (
                <div key={msg.id} className={`flex ${msg.sender === 'user' ? 'justify-end' : 'justify-start'}`}>
                    <div className={`max-w-[85%] p-3 text-xs leading-relaxed rounded-2xl shadow-sm ${
                        msg.sender === 'user' 
                        ? 'bg-blue-600 text-white rounded-br-none' 
                        : 'bg-white text-slate-700 border border-slate-200 rounded-bl-none'
                    }`}>
                        {msg.text}
                    </div>
                </div>
            ))}
            {isThinking && (
                 <div className="flex gap-1 ml-2 p-2">
                    <div className="w-1.5 h-1.5 bg-slate-400 rounded-full animate-bounce"></div>
                    <div className="w-1.5 h-1.5 bg-slate-400 rounded-full animate-bounce delay-75"></div>
                    <div className="w-1.5 h-1.5 bg-slate-400 rounded-full animate-bounce delay-150"></div>
                </div>
            )}
            <div ref={scrollRef}></div>
        </div>

        {/* INPUT */}
        <div className="bg-white p-4 pb-8 border-t border-slate-100">
            <div className="flex gap-2 items-center bg-slate-50 px-4 py-2 rounded-full border border-slate-200 focus-within:ring-2 focus-within:ring-blue-500 transition-all">
                <input 
                    className="flex-1 bg-transparent text-sm text-slate-800 focus:outline-none placeholder:text-slate-400"
                    placeholder="Type a message..."
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyPress={(e) => e.key === 'Enter' && handleSend()}
                />
                <button onClick={handleSend} className="text-blue-600 hover:text-blue-700">
                    <Send size={18} />
                </button>
            </div>
        </div>
      </div>
    </div>
  );
};

export default MobileSimulator;