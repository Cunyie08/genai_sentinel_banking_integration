import { configureStore } from '@reduxjs/toolkit';
import simulationReducer from './simulationSlice';

export const store = configureStore({
  reducer: {
    simulation: simulationReducer,
  },
});