import { createSlice } from '@reduxjs/toolkit';

const initialState = {
  // MOBILE APP STATE
  messages: [
    { id: 1, sender: 'agent', text: 'Good morning, Ayo. How can I help you today?', timestamp: '10:00 AM' }
  ],
  isThinking: false,

  // GLASS BOX STATE
  activeAgent: 'Orchestrator',
  thoughtLogs: [
    { 
      id: 'init-1', 
      agent: 'System', 
      step: 'Boot', 
      status: 'success', 
      detail: 'Sentinel Core Online', 
      // FIXED: Use a fake ISO string so .split('T') works
      timestamp: new Date().toISOString() 
    }
  ]
};

const simulationSlice = createSlice({
  name: 'simulation',
  initialState,
  reducers: {
    addMessage: (state, action) => {
      state.messages.push(action.payload);
    },
    setThinking: (state, action) => {
      state.isThinking = action.payload;
    },
    addThought: (state, action) => {
      state.thoughtLogs.push(action.payload);
      state.activeAgent = action.payload.agent;
    },
  }
});

export const { addMessage, setThinking, addThought } = simulationSlice.actions;
export default simulationSlice.reducer;