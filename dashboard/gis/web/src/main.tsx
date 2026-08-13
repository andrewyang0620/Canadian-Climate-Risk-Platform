import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { MotionConfig } from "motion/react";
import './index.css'
import App from './App.tsx'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <MotionConfig
      reducedMotion="user"
      transition={{
        duration: 0.22,
        ease: [0.22, 1, 0.36, 1],
      }}
    >
      <App />
    </MotionConfig>
  </StrictMode>,
)
