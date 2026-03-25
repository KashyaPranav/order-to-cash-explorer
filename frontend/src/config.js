// API configuration for production deployment
// Set VITE_API_URL in Vercel environment variables to point to your Render backend
const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000'

export default API_BASE
